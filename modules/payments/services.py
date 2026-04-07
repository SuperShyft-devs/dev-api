"""Payments business logic: bookings, Razorpay orders, verify, failures, status."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.diagnostics.models import DiagnosticPackage
from modules.payments.models import Booking, Order, Payment
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


class PaymentsService:
    async def create_order(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        entity_type: str,
        entity_id: int,
    ) -> dict[str, Any]:
        try:
            user_result = await db.execute(select(User).where(User.user_id == user_id))
            user = user_result.scalar_one_or_none()
            if user is None:
                return {"_error": (400, "User not found")}

            amount_paise: int
            entity_name: str

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
                amount_paise = _price_to_paise(rupee_price)
                entity_name = package.package_name or ""
            else:
                return {"_error": (400, f"Unsupported entity_type: {entity_type}")}

            booking = Booking(
                user_id=user_id,
                entity_type=entity_type,
                entity_id=entity_id,
                entity_name=entity_name,
                amount_paise=amount_paise,
                currency="INR",
                status="pending",
            )
            db.add(booking)
            await db.flush()

            try:
                rz_order = await asyncio.to_thread(
                    _create_razorpay_order_sync,
                    amount_paise=amount_paise,
                    receipt=f"booking_{booking.booking_id}",
                )
            except Exception as exc:
                logger.exception("Razorpay order creation failed: %s", exc)
                await db.delete(booking)
                await db.commit()
                return {"_error": (502, "Payment service unavailable")}

            razorpay_order_id = rz_order.get("id")
            if not razorpay_order_id:
                await db.delete(booking)
                await db.commit()
                return {"_error": (502, "Payment service unavailable")}

            order_row = Order(
                booking_id=booking.booking_id,
                user_id=user_id,
                razorpay_order_id=razorpay_order_id,
                amount_paise=amount_paise,
                currency="INR",
                status="created",
            )
            db.add(order_row)
            await db.commit()

            return {
                "success": True,
                "booking_id": booking.booking_id,
                "razorpay_order_id": razorpay_order_id,
                "amount_paise": amount_paise,
                "amount_rupees": rupees_from_paise(amount_paise),
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
                    b_result = await db.execute(
                        select(Booking).where(Booking.booking_id == order_row.booking_id)
                    )
                    booking = b_result.scalar_one()
                    return {
                        "success": True,
                        "message": "Payment verified. Booking confirmed.",
                        "booking_id": booking.booking_id,
                        "payment_id": razorpay_payment_id,
                    }
                return {"_error": (422, "Order marked paid but no payment record")}

            booking_result = await db.execute(
                select(Booking).where(Booking.booking_id == order_row.booking_id)
            )
            booking = booking_result.scalar_one_or_none()
            if booking is None:
                return {"_error": (404, "Booking not found")}

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

            booking.status = "confirmed"
            booking.updated_at = now

            if booking.entity_type == "diagnostic_package":
                await db.execute(
                    update(DiagnosticPackage)
                    .where(DiagnosticPackage.diagnostic_package_id == booking.entity_id)
                    .values(bookings_count=DiagnosticPackage.bookings_count + 1)
                )

            await db.commit()

            return {
                "success": True,
                "message": "Payment verified. Booking confirmed.",
                "booking_id": booking.booking_id,
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
    ) -> dict[str, Any] | None:
        booking_result = await db.execute(select(Booking).where(Booking.booking_id == booking_id))
        booking = booking_result.scalar_one_or_none()
        if booking is None:
            return None

        pay_result = await db.execute(
            select(Payment)
            .where(Payment.booking_id == booking_id)
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
        latest_sub = (
            select(
                Payment.booking_id.label("bid"),
                func.max(Payment.payment_id).label("max_pid"),
            ).group_by(Payment.booking_id)
        ).subquery()

        filters = []
        if status:
            filters.append(Booking.status == status)
        if search and search.strip():
            q = f"%{search.strip()}%"
            user_full = func.trim(
                func.concat(
                    func.coalesce(User.first_name, ""),
                    " ",
                    func.coalesce(User.last_name, ""),
                )
            )
            filters.append(or_(Booking.entity_name.ilike(q), user_full.ilike(q)))

        count_stmt = select(func.count()).select_from(Booking).join(User, User.user_id == Booking.user_id)
        if filters:
            count_stmt = count_stmt.where(*filters)
        total_result = await db.execute(count_stmt)
        total = int(total_result.scalar_one() or 0)

        order_fn = Booking.booking_id.desc() if sort_dir.lower() == "desc" else Booking.booking_id.asc()

        list_stmt = (
            select(Booking, User, Payment)
            .select_from(Booking)
            .join(User, User.user_id == Booking.user_id)
            .outerjoin(latest_sub, latest_sub.c.bid == Booking.booking_id)
            .outerjoin(Payment, Payment.payment_id == latest_sub.c.max_pid)
        )
        if filters:
            list_stmt = list_stmt.where(*filters)
        list_stmt = list_stmt.order_by(order_fn).offset((page - 1) * limit).limit(limit)
        rows = await db.execute(list_stmt)
        items: list[dict[str, Any]] = []
        for booking, user_row, pay in rows.all():
            user_name = " ".join(x for x in [user_row.first_name, user_row.last_name] if x) or "—"
            booked_ts = booking.booked_at
            if booked_ts is not None and booked_ts.tzinfo is None:
                booked_ts = booked_ts.replace(tzinfo=timezone.utc)
            booked_at_str = (
                booked_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if booked_ts else ""
            )
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
                }
            )

        return {"items": items, "total": total}
