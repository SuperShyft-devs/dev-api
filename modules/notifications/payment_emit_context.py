"""Build standardized emit context for payments.completed."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.discounts.models import DiscountCode, DiscountUsage
from modules.payments.models import Booking, Order, Payment
from modules.users.models import User

logger = logging.getLogger(__name__)


def _user_contact_row(user: User | None) -> dict[str, Any] | None:
    if user is None:
        return None
    phone = (user.phone or "").strip() or None
    email = (user.email or "").strip().lower() or None
    name = f"{user.first_name or ''} {user.last_name or ''}".strip() or None
    return {
        "user_id": int(user.user_id),
        "phone": phone,
        "email": email,
        "name": name,
        "role": "participant",
    }


async def build_payment_completed_emit_context(
    db: AsyncSession,
    *,
    order: Order,
    bookings: list[Booking],
    payer_user_id: int,
    razorpay_payment_id: str | None = None,
) -> dict[str, Any]:
    payer = (
        await db.execute(select(User).where(User.user_id == int(payer_user_id)))
    ).scalar_one_or_none()

    members: list[dict[str, Any]] = []
    seen_uids: set[int] = set()
    for booking in bookings:
        uid = int(booking.user_id)
        if uid in seen_uids:
            continue
        seen_uids.add(uid)
        member_user = (
            await db.execute(select(User).where(User.user_id == uid))
        ).scalar_one_or_none()
        row = _user_contact_row(member_user)
        if row:
            members.append(row)

    booker_row = _user_contact_row(payer)
    if booker_row:
        booker_row = {**booker_row, "role": "booker"}

    payment_id = razorpay_payment_id
    if not payment_id:
        pay_result = await db.execute(
            select(Payment)
            .where(Payment.order_id == order.order_id, Payment.status == "success")
            .order_by(Payment.payment_id.desc())
            .limit(1)
        )
        pay_row = pay_result.scalar_one_or_none()
        if pay_row and pay_row.razorpay_payment_id:
            payment_id = pay_row.razorpay_payment_id

    discount_code = ""
    discount_paise: int | None = None
    usage_rows = await db.execute(
        select(DiscountUsage, DiscountCode.code)
        .join(DiscountCode, DiscountCode.discount_code_id == DiscountUsage.discount_code_id)
        .where(
            DiscountUsage.order_id == int(order.order_id),
            DiscountUsage.status.in_(("committed", "reserved")),
        )
        .order_by(DiscountUsage.usage_id.desc())
        .limit(1)
    )
    usage_pair = usage_rows.first()
    if usage_pair is not None:
        usage, code_str = usage_pair
        discount_code = (code_str or "").strip()
        discount_paise = int(usage.discount_paise or 0)

    payment_block: dict[str, Any] = {
        "order_id": order.razorpay_order_id,
        "payment_id": payment_id or "",
        "amount_paise": int(order.amount_paise or 0),
        "discount_code": discount_code,
    }
    if discount_paise is not None:
        payment_block["discount_paise"] = discount_paise

    ctx: dict[str, Any] = {
        "payer_user_id": int(payer_user_id),
        "user_id": int(payer_user_id),
        "booker": booker_row or {},
        "members": members,
        "payment": payment_block,
    }
    return ctx


async def emit_payments_completed(
    db: AsyncSession,
    *,
    order: Order,
    bookings: list[Booking],
    payer_user_id: int,
    razorpay_payment_id: str | None = None,
    engagement_id: int | None = None,
) -> list[int]:
    from modules.notifications.engine import get_notification_engine

    ctx = await build_payment_completed_emit_context(
        db,
        order=order,
        bookings=bookings,
        payer_user_id=payer_user_id,
        razorpay_payment_id=razorpay_payment_id,
    )
    eid = engagement_id
    if eid is None and bookings:
        meta = bookings[0].metadata_ or {}
        if meta.get("engagement_id") is not None:
            eid = int(meta["engagement_id"])
    return await get_notification_engine().emit(
        db,
        event_code="payments.completed",
        engagement_id=eid,
        context=ctx,
    )


async def try_emit_payments_completed_after_verify(
    db: AsyncSession,
    *,
    razorpay_order_id: str,
    payer_user_id: int,
    bookings: list[Booking],
    razorpay_payment_id: str | None = None,
    engagement_id: int | None = None,
) -> None:
    """Best-effort payments.completed emit (bindings may be absent)."""
    try:
        order_result = await db.execute(
            select(Order).where(Order.razorpay_order_id == razorpay_order_id.strip())
        )
        order = order_result.scalar_one_or_none()
        if order is None or not bookings:
            return
        await emit_payments_completed(
            db,
            order=order,
            bookings=bookings,
            payer_user_id=payer_user_id,
            razorpay_payment_id=razorpay_payment_id,
            engagement_id=engagement_id,
        )
    except Exception as exc:
        logger.warning("payments.completed emit failed: %s", exc)
