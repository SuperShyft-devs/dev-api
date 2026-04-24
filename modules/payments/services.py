"""Payments business logic: bookings, Razorpay orders, verify, failures, status."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, or_, select, union_all, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from core.config import settings
from modules.diagnostics.models import DiagnosticPackage
from modules.payments.models import Booking, Order, OrderBooking, Payment
from modules.payments.razorpay_client import get_razorpay_client
from modules.users.models import User

logger = logging.getLogger(__name__)


def rupees_from_paise(amount_paise: int) -> float:
    """UI display only — never persist derived rupees."""
    return round(amount_paise / 100.0, 2)


def _price_to_paise(price: Decimal | float | int | None) -> int:
    if price is None:
        raise ValueError("Price is missing")
    d = price if isinstance(price, Decimal) else Decimal(str(price))
    return int((d * 100).to_integral_value())


def _verify_razorpay_signature(*, order_id: str, payment_id: str, signature: str) -> bool:
    message = f"{order_id}|{payment_id}"
    generated = hmac.new(
        key=settings.RAZORPAY_KEY_SECRET.encode(),
        msg=message.encode(),
        digestmod=hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(generated, signature)


def _create_razorpay_order_sync(*, amount_paise: int, receipt: str) -> dict[str, Any]:
    client = get_razorpay_client()
    return client.order.create(
        {
            "amount": amount_paise,
            "currency": "INR",
            "receipt": receipt,
        }
    )


async def _booking_ids_for_order(db: AsyncSession, order_row: Order) -> list[int]:
    ob_result = await db.execute(
        select(OrderBooking.booking_id).where(OrderBooking.order_id == order_row.order_id)
    )
    ids = {row[0] for row in ob_result.all()}
    ids.add(order_row.booking_id)
    return sorted(ids)


async def _resolve_line_pricing(
    db: AsyncSession, *, entity_type: str, entity_id: int
) -> tuple[int, str] | dict[str, Any]:
    if entity_type == "diagnostic_package":
        pkg_result = await db.execute(
            select(DiagnosticPackage).where(
                DiagnosticPackage.diagnostic_package_id == entity_id,
                DiagnosticPackage.status == "active",
            )
        )
        package = pkg_result.scalar_one_or_none()
        if package is None:
            return {"_error": (400, "Diagnostic package not found or not active")}
        rupee_price = package.price if package.price is not None else package.original_price
        if rupee_price is None:
            return {
                "_error": (
                    400,
                    "Diagnostic package has no price (set price or original_price in the database)",
                ),
            }
        return (_price_to_paise(rupee_price), package.package_name or "")
    return {"_error": (400, f"Unsupported entity_type: {entity_type}")}


# (order_id, booking_id) pairs: Razorpay order row + junction (multi-line) + legacy single booking
_booking_order_link = (
    union_all(
        select(Order.order_id.label("oid"), Order.booking_id.label("bid")),
        select(OrderBooking.order_id.label("oid"), OrderBooking.booking_id.label("bid")),
    )
).subquery("bol")


class PaymentsService:
    async def create_order(
        self,
        db: AsyncSession,
        *,
        payer_user_id: int,
        items: list[tuple[int, str, int]],
        authenticated_user_id: int,
    ) -> dict[str, Any]:
        try:
            payer_result = await db.execute(select(User).where(User.user_id == payer_user_id))
            payer_user = payer_result.scalar_one_or_none()
            if payer_user is None:
                return {"_error": (400, "User not found")}

            is_self = payer_user_id == authenticated_user_id
            is_family = (
                payer_user.parent_id == authenticated_user_id
                or (payer_user.parent_id is None and False)
            )
            if not is_self:
                auth_result = await db.execute(select(User).where(User.user_id == authenticated_user_id))
                auth_user = auth_result.scalar_one_or_none()
                is_family = (
                    (payer_user.parent_id == authenticated_user_id)
                    or (auth_user is not None and auth_user.parent_id == payer_user_id)
                )
            if not is_self and not is_family:
                return {"_error": (403, "Not authorized to create orders for this user")}

            bookings_created: list[Booking] = []
            total_paise = 0

            for member_user_id, entity_type, entity_id in items:
                member_result = await db.execute(select(User).where(User.user_id == member_user_id))
                if member_result.scalar_one_or_none() is None:
                    return {"_error": (400, f"User not found for line (user_id={member_user_id})")}

                resolved = await _resolve_line_pricing(db, entity_type=entity_type, entity_id=entity_id)
                if isinstance(resolved, dict) and resolved.get("_error"):
                    return resolved
                assert isinstance(resolved, tuple)
                line_paise, entity_name = resolved
                total_paise += line_paise

                booking = Booking(
                    user_id=member_user_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    entity_name=entity_name,
                    amount_paise=line_paise,
                    currency="INR",
                    status="pending",
                )
                db.add(booking)
                await db.flush()
                bookings_created.append(booking)

            if not bookings_created:
                return {"_error": (400, "No booking lines")}

            anchor_booking_id = bookings_created[0].booking_id
            receipt = f"checkout_{anchor_booking_id}"

            try:
                rz_order = await asyncio.to_thread(
                    _create_razorpay_order_sync,
                    amount_paise=total_paise,
                    receipt=receipt,
                )
            except Exception as exc:
                logger.exception("Razorpay order creation failed: %s", exc)
                for b in bookings_created:
                    await db.delete(b)
                await db.commit()
                return {"_error": (502, "Payment service unavailable")}

            razorpay_order_id = rz_order.get("id")
            if not razorpay_order_id:
                for b in bookings_created:
                    await db.delete(b)
                await db.commit()
                return {"_error": (502, "Payment service unavailable")}

            order_row = Order(
                booking_id=anchor_booking_id,
                user_id=payer_user_id,
                razorpay_order_id=razorpay_order_id,
                amount_paise=total_paise,
                currency="INR",
                status="created",
            )
            db.add(order_row)
            await db.flush()

            for b in bookings_created:
                db.add(OrderBooking(order_id=order_row.order_id, booking_id=b.booking_id))

            await db.commit()

            booking_ids = [b.booking_id for b in bookings_created]
            return {
                "success": True,
                "booking_ids": booking_ids,
                "booking_id": booking_ids[0],
                "razorpay_order_id": razorpay_order_id,
                "amount_paise": total_paise,
                "amount_rupees": rupees_from_paise(total_paise),
                "currency": "INR",
                "key_id": settings.RAZORPAY_KEY_ID,
            }
        except Exception as exc:
            logger.exception("create_order failed: %s", exc)
            await db.rollback()
            return {"_error": (500, "Internal server error")}

    async def verify_payment(
        self,
        db: AsyncSession,
        *,
        razorpay_payment_id: str,
        razorpay_order_id: str,
        razorpay_signature: str,
    ) -> dict[str, Any]:
        try:
            if not _verify_razorpay_signature(
                order_id=razorpay_order_id,
                payment_id=razorpay_payment_id,
                signature=razorpay_signature,
            ):
                return {"_signature_invalid": True}

            order_result = await db.execute(
                select(Order).where(Order.razorpay_order_id == razorpay_order_id)
            )
            order_row = order_result.scalar_one_or_none()
            if order_row is None:
                return {"_error": (404, "Order not found")}

            if order_row.status == "paid":
                pay_result = await db.execute(
                    select(Payment)
                    .where(
                        Payment.order_id == order_row.order_id,
                        Payment.status == "success",
                    )
                    .order_by(Payment.payment_id.desc())
                    .limit(1)
                )
                existing = pay_result.scalar_one_or_none()
                if existing is not None:
                    booking_ids = await _booking_ids_for_order(db, order_row)
                    return {
                        "success": True,
                        "message": "Payment verified. Booking confirmed.",
                        "booking_ids": booking_ids,
                        "booking_id": booking_ids[0] if booking_ids else order_row.booking_id,
                        "payment_id": razorpay_payment_id,
                    }
                return {"_error": (422, "Order marked paid but no payment record")}

            booking_ids = await _booking_ids_for_order(db, order_row)
            bookings_result = await db.execute(
                select(Booking).where(Booking.booking_id.in_(booking_ids))
            )
            bookings = list(bookings_result.scalars().all())
            if len(bookings) != len(booking_ids):
                return {"_error": (404, "Booking not found")}

            for b in bookings:
                if b.status != "pending":
                    return {
                        "_error": (
                            422,
                            "One or more bookings are not pending; cannot complete payment",
                        ),
                    }

            now = datetime.now(timezone.utc)
            payment_row = Payment(
                order_id=order_row.order_id,
                booking_id=order_row.booking_id,
                user_id=order_row.user_id,
                razorpay_payment_id=razorpay_payment_id,
                razorpay_order_id=razorpay_order_id,
                razorpay_signature=razorpay_signature,
                amount_paise=order_row.amount_paise,
                currency=order_row.currency or "INR",
                status="success",
                payment_method=None,
                signature_verified=True,
                failure_reason=None,
                paid_at=now,
            )
            db.add(payment_row)

            order_row.status = "paid"
            order_row.updated_at = now

            for booking in bookings:
                booking.status = "confirmed"
                booking.updated_at = now
                if booking.entity_type == "diagnostic_package":
                    await db.execute(
                        update(DiagnosticPackage)
                        .where(DiagnosticPackage.diagnostic_package_id == booking.entity_id)
                        .values(bookings_count=DiagnosticPackage.bookings_count + 1)
                    )

            await db.commit()

            sorted_ids = sorted(booking_ids)
            return {
                "success": True,
                "message": "Payment verified. Booking confirmed.",
                "booking_ids": sorted_ids,
                "booking_id": sorted_ids[0],
                "payment_id": razorpay_payment_id,
            }
        except Exception as exc:
            logger.exception("verify_payment failed: %s", exc)
            await db.rollback()
            return {"_error": (500, "Internal server error")}

    async def record_failure(
        self,
        db: AsyncSession,
        *,
        razorpay_order_id: str,
        failure_reason: str | None,
    ) -> dict[str, Any]:
        try:
            order_result = await db.execute(
                select(Order).where(Order.razorpay_order_id == razorpay_order_id)
            )
            order_row = order_result.scalar_one_or_none()
            if order_row is None:
                return {"_error": (404, "Order not found")}

            reason = failure_reason if failure_reason and failure_reason.strip() else "Unknown"

            payment_row = Payment(
                order_id=order_row.order_id,
                booking_id=order_row.booking_id,
                user_id=order_row.user_id,
                razorpay_payment_id=None,
                razorpay_order_id=razorpay_order_id,
                razorpay_signature=None,
                amount_paise=order_row.amount_paise,
                currency=order_row.currency or "INR",
                status="failed",
                payment_method=None,
                signature_verified=False,
                failure_reason=reason,
                paid_at=None,
            )
            db.add(payment_row)

            order_row.status = "attempted"
            order_row.updated_at = datetime.now(timezone.utc)

            await db.commit()

            return {
                "success": False,
                "message": "Payment failure recorded. You can retry.",
            }
        except Exception as exc:
            logger.exception("record_failure failed: %s", exc)
            await db.rollback()
            return {"_error": (500, "Internal server error")}

    async def get_booking_status(
        self,
        db: AsyncSession,
        *,
        booking_id: int,
        include_user_detail: bool = False,
        requesting_user_id: int | None = None,
        is_employee: bool = False,
    ) -> dict[str, Any] | None:
        booking_result = await db.execute(select(Booking).where(Booking.booking_id == booking_id))
        booking = booking_result.scalar_one_or_none()
        if booking is None:
            return None

        if not is_employee and requesting_user_id is not None:
            if booking.user_id != requesting_user_id:
                order_link_res = await db.execute(
                    select(_booking_order_link.c.oid).where(_booking_order_link.c.bid == booking_id).limit(1)
                )
                oid = order_link_res.scalar_one_or_none()
                is_payer = False
                if oid is not None:
                    order_res = await db.execute(select(Order).where(Order.order_id == oid))
                    order = order_res.scalar_one_or_none()
                    is_payer = order is not None and order.user_id == requesting_user_id
                if not is_payer:
                    return None

        order_ids_sq = select(_booking_order_link.c.oid).where(_booking_order_link.c.bid == booking_id)
        pay_result = await db.execute(
            select(Payment)
            .where(
                or_(
                    Payment.booking_id == booking_id,
                    Payment.order_id.in_(order_ids_sq),
                )
            )
            .order_by(Payment.payment_id.desc())
            .limit(1)
        )
        payment_row = pay_result.scalar_one_or_none()

        paid_at_str: str | None = None
        if payment_row and payment_row.paid_at is not None:
            ts = payment_row.paid_at
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            paid_at_str = ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        base: dict[str, Any] = {
            "booking_id": booking.booking_id,
            "entity_type": booking.entity_type,
            "entity_name": booking.entity_name,
            "amount_paise": booking.amount_paise,
            "amount_rupees": rupees_from_paise(booking.amount_paise),
            "currency": booking.currency or "INR",
            "booking_status": booking.status,
            "payment_status": payment_row.status if payment_row else None,
            "payment_method": payment_row.payment_method if payment_row else None,
            "razorpay_payment_id": payment_row.razorpay_payment_id if payment_row else None,
            "signature_verified": payment_row.signature_verified if payment_row else False,
            "paid_at": paid_at_str,
        }

        if include_user_detail:
            user_result = await db.execute(select(User).where(User.user_id == booking.user_id))
            user_row = user_result.scalar_one_or_none()
            user_name = (
                " ".join(x for x in [user_row.first_name, user_row.last_name] if x)
                if user_row
                else ""
            )
            booked_ts = booking.booked_at
            if booked_ts is not None and booked_ts.tzinfo is None:
                booked_ts = booked_ts.replace(tzinfo=timezone.utc)
            booked_at_str = booked_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if booked_ts else ""
            base.update(
                {
                    "user_id": booking.user_id,
                    "user_name": user_name or "—",
                    "booked_at": booked_at_str,
                    "failure_reason": payment_row.failure_reason if payment_row else None,
                    "signature_verified": payment_row.signature_verified if payment_row else None,
                }
            )

            oid_res = await db.execute(
                select(_booking_order_link.c.oid)
                .where(_booking_order_link.c.bid == booking_id)
                .limit(1)
            )
            first_oid = oid_res.scalar_one_or_none()
            if first_oid is not None:
                o_res = await db.execute(select(Order).where(Order.order_id == first_oid))
                order_row = o_res.scalar_one_or_none()
                if order_row is not None:
                    sib_ids = await _booking_ids_for_order(db, order_row)
                    sib_bookings = await db.execute(
                        select(Booking).where(Booking.booking_id.in_(sib_ids))
                    )
                    sib_list = list(sib_bookings.scalars().all())
                    lines_out: list[dict[str, Any]] = []
                    for sb in sorted(sib_list, key=lambda x: x.booking_id):
                        u_r = await db.execute(select(User).where(User.user_id == sb.user_id))
                        u = u_r.scalar_one_or_none()
                        nm = (
                            " ".join(x for x in [u.first_name, u.last_name] if x)
                            if u
                            else ""
                        )
                        lines_out.append(
                            {
                                "booking_id": sb.booking_id,
                                "user_id": sb.user_id,
                                "user_name": nm or "—",
                                "entity_name": sb.entity_name,
                                "amount_paise": sb.amount_paise,
                                "amount_rupees": rupees_from_paise(sb.amount_paise),
                                "booking_status": sb.status,
                            }
                        )
                    ob_cnt = await db.execute(
                        select(func.count())
                        .select_from(OrderBooking)
                        .where(OrderBooking.order_id == order_row.order_id)
                    )
                    junction_count = int(ob_cnt.scalar_one() or 0)
                    line_count = junction_count if junction_count > 0 else 1
                    payer_res = await db.execute(
                        select(User).where(User.user_id == order_row.user_id)
                    )
                    payer_u = payer_res.scalar_one_or_none()
                    payer_nm = (
                        " ".join(x for x in [payer_u.first_name, payer_u.last_name] if x)
                        if payer_u
                        else ""
                    )
                    base["checkout"] = {
                        "order_id": order_row.order_id,
                        "razorpay_order_id": order_row.razorpay_order_id,
                        "order_amount_paise": order_row.amount_paise,
                        "order_amount_rupees": rupees_from_paise(order_row.amount_paise),
                        "checkout_line_count": line_count,
                        "payer_user_id": order_row.user_id,
                        "payer_user_name": payer_nm or "—",
                        "lines": lines_out,
                    }

        return base

    async def list_bookings_admin(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        search: str | None,
        status: str | None,
        sort_key: str,
        sort_dir: str,
    ) -> dict[str, Any]:
        member_user = aliased(User)
        payer_user = aliased(User)

        # Explicit FROM + correlate(Booking): otherwise SQLAlchemy strips bol FROM
        # inside JOIN ON scalar subqueries ("no FROM clauses due to auto-correlation").
        order_ids_for_booking_sq = (
            select(_booking_order_link.c.oid)
            .select_from(_booking_order_link)
            .where(_booking_order_link.c.bid == Booking.booking_id)
            .correlate(Booking)
            .scalar_subquery()
        )

        latest_payment_id_sq = (
            select(func.max(Payment.payment_id))
            .where(
                or_(
                    Payment.booking_id == Booking.booking_id,
                    Payment.order_id.in_(order_ids_for_booking_sq),
                )
            )
            .correlate(Booking)
            .scalar_subquery()
        )

        booking_primary_order_sq = (
            select(_booking_order_link.c.oid)
            .select_from(_booking_order_link)
            .where(_booking_order_link.c.bid == Booking.booking_id)
            .limit(1)
            .correlate(Booking)
            .scalar_subquery()
        )

        filters = []
        if status:
            filters.append(Booking.status == status)
        if search and search.strip():
            q = f"%{search.strip()}%"
            user_full = func.trim(
                func.concat(
                    func.coalesce(member_user.first_name, ""),
                    " ",
                    func.coalesce(member_user.last_name, ""),
                )
            )
            filters.append(or_(Booking.entity_name.ilike(q), user_full.ilike(q)))

        count_stmt = (
            select(func.count()).select_from(Booking).join(member_user, member_user.user_id == Booking.user_id)
        )
        if filters:
            count_stmt = count_stmt.where(*filters)
        total_result = await db.execute(count_stmt)
        total = int(total_result.scalar_one() or 0)

        order_fn = Booking.booking_id.desc() if sort_dir.lower() == "desc" else Booking.booking_id.asc()

        list_stmt = (
            select(Booking, member_user, Payment, Order, payer_user)
            .select_from(Booking)
            .join(member_user, member_user.user_id == Booking.user_id)
            .outerjoin(Order, Order.order_id == booking_primary_order_sq)
            .outerjoin(payer_user, payer_user.user_id == Order.user_id)
            .outerjoin(Payment, Payment.payment_id == latest_payment_id_sq)
        )
        if filters:
            list_stmt = list_stmt.where(*filters)
        list_stmt = list_stmt.order_by(order_fn).offset((page - 1) * limit).limit(limit)
        rows = await db.execute(list_stmt)
        row_tuples = rows.all()
        order_ids_needed = {
            order_row.order_id
            for (_, _, _, order_row, _) in row_tuples
            if order_row is not None
        }
        counts_map: dict[int, int] = {}
        if order_ids_needed:
            cnt_res = await db.execute(
                select(OrderBooking.order_id, func.count(OrderBooking.booking_id))
                .where(OrderBooking.order_id.in_(order_ids_needed))
                .group_by(OrderBooking.order_id)
            )
            for oid, c in cnt_res.all():
                counts_map[int(oid)] = int(c)

        items: list[dict[str, Any]] = []
        for booking, user_row, pay, order_row, payer_row in row_tuples:
            user_name = " ".join(x for x in [user_row.first_name, user_row.last_name] if x) or "—"
            payer_user_id: int | None = None
            payer_user_name: str | None = None
            if order_row is not None:
                payer_user_id = order_row.user_id
                if payer_row is not None:
                    payer_user_name = (
                        " ".join(x for x in [payer_row.first_name, payer_row.last_name] if x) or "—"
                    )
                else:
                    payer_user_name = "—"
            booked_ts = booking.booked_at
            if booked_ts is not None and booked_ts.tzinfo is None:
                booked_ts = booked_ts.replace(tzinfo=timezone.utc)
            booked_at_str = (
                booked_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if booked_ts else ""
            )
            checkout_line_count: int | None = None
            order_id: int | None = None
            razorpay_order_id: str | None = None
            order_amount_paise: int | None = None
            if order_row is not None:
                order_id = order_row.order_id
                razorpay_order_id = order_row.razorpay_order_id
                order_amount_paise = order_row.amount_paise
                raw_c = counts_map.get(order_row.order_id, 0)
                checkout_line_count = raw_c if raw_c > 0 else 1

            items.append(
                {
                    "booking_id": booking.booking_id,
                    "user_id": booking.user_id,
                    "user_name": user_name,
                    "entity_type": booking.entity_type,
                    "entity_name": booking.entity_name,
                    "amount_paise": booking.amount_paise,
                    "currency": booking.currency or "INR",
                    "status": booking.status,
                    "payment_status": pay.status if pay else None,
                    "payment_method": pay.payment_method if pay else None,
                    "booked_at": booked_at_str,
                    "order_id": order_id,
                    "razorpay_order_id": razorpay_order_id,
                    "order_amount_paise": order_amount_paise,
                    "checkout_line_count": checkout_line_count,
                    "payer_user_id": payer_user_id,
                    "payer_user_name": payer_user_name,
                }
            )

        return {"items": items, "total": total}
