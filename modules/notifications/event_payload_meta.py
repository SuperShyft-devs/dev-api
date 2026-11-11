"""Per-event payload recipient metadata (trigger APIs, admin copy)."""

from __future__ import annotations

EVENT_PAYLOAD_TRIGGER_APIS: dict[str, tuple[str, ...]] = {
    "payments.completed": (
        "POST /payments/verify",
        "POST /book/blood-test",
        "POST /book/bio-ai",
        "POST /book/consultation",
        "POST /users/code/{engagement_code}/onboard/confirm-payment",
        "POST /users/public/family-onboard/confirm-payment",
    ),
    "payments.failed": ("POST /payments/failed",),
    "auth.otp_sent": (
        "POST /auth/send-otp",
        "POST /auth/resend-otp",
        "POST /users/code/{code}/onboard (paid OTP)",
    ),
}


EVENT_PAYLOAD_ADMIN_HINT: dict[str, str] = {
    "payments.completed": (
        "All listed APIs send the same payment context: who paid (payer/booker) "
        "and each line-item participant (members). Pick who should receive this message."
    ),
    "auth.otp_sent": "Use the phone or email from the OTP request when no user account exists yet.",
}


def payload_trigger_apis_for_event(event_code: str) -> list[str]:
    return list(EVENT_PAYLOAD_TRIGGER_APIS.get(event_code, ()))
