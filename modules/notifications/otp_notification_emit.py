"""Emit auth.otp_sent via the notification engine (all OTP entrypoints)."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.engine import get_notification_engine


async def emit_auth_otp_sent(
    db: AsyncSession,
    *,
    otp: str,
    service_key: str,
    phone: str | None = None,
    email: str | None = None,
    user_id: int | None = None,
    staff: dict | None = None,
    notify_all_channels: bool = False,
) -> list[int]:
    """Dispatch OTP using catalog bindings for auth.otp_sent."""
    context: dict[str, object] = {
        "otp": otp,
        "service_key": service_key,
        "fallback_service_keys": [service_key],
        "otp_notify_all_channels": bool(notify_all_channels),
    }
    if user_id is not None:
        context["user_id"] = int(user_id)
    if phone and str(phone).strip():
        context["phone"] = str(phone).strip()
    if email and str(email).strip():
        context["email"] = str(email).strip().lower()
    if staff:
        context["participant_details"] = {"staff": staff}
    return await get_notification_engine().emit(
        db,
        event_code="auth.otp_sent",
        context=context,
    )
