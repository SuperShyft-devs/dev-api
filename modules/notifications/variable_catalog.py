"""Map event variable groups to insertable field paths for admin wizards."""

from __future__ import annotations

VARIABLE_GROUP_FIELDS: dict[str, tuple[str, ...]] = {
    "user": (
        "user.name",
        "user.first_name",
        "user.last_name",
        "user.phone",
        "user.email",
        "user.age",
        "user.gender",
        "user.address",
        "user.pincode",
    ),
    "staff": (
        "staff.name",
        "staff.first_name",
        "staff.last_name",
        "staff.phone",
        "staff.email",
    ),
    "engagement": (
        "engagement.engagement_id",
        "engagement.engagement_code",
        "engagement.engagement_name",
        "engagement.engagement_type",
        "engagement.cabin_name",
    ),
    "session": ("session.date", "session.time", "session.cabin"),
    "booking": (
        "booking.booking_id",
        "booking.vendor_booking_id",
        "booking.collection_date",
        "booking.collection_time",
    ),
    "consultation": (
        "consultation.expert_type",
        "consultation.expert_name",
        "consultation.meet_link",
        "consultation.date",
        "consultation.time",
        "consultation.cabin",
    ),
    "payment": ("payment.order_id", "payment.payment_id", "payment.amount", "payment.discount_code"),
    "report": ("report.blood_report_url", "report.bio_ai_report_url", "report.face_scan_report_url"),
    "otp": ("otp.otp",),
    "gift": (
        "gift.booker_name",
        "gift.booker_phone",
        "gift.recipient_name",
        "gift.relationship",
        "gift.gift_url",
        "gift.gift_card_url",
        "gift.gift_message",
    ),
    "support": ("support.ticket_id", "support.query_text"),
    "phlebo": ("phlebo.phlebo_name", "phlebo.phlebo_phone"),
    "error": ("error.error_message", "error.api_path"),
    "server": (
        "server.hostname",
        "server.cpu_usage",
        "server.memory_usage",
        "server.storage_usage",
        "server.load_1m",
        "server.cores",
    ),
}


def fields_for_groups(groups: tuple[str, ...] | list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for field in VARIABLE_GROUP_FIELDS.get(group, ()):
            if field not in seen:
                seen.add(field)
                out.append(field)
    return out


KNOWN_RECIPIENT_TYPES: frozenset[str] = frozenset(
    {
        "user",
        "members",
        "booker",
        "assistants",
        "assistants.phlebo",
        "assistants.expert",
        "assistants.staff",
        "departments",
        "departments.ops",
        "departments.tech",
        "departments.design",
        "custom",
    }
)

RECIPIENT_PAYLOAD_HINTS: dict[str, str] = {
    "user": "user_id or participant_user_id (or user_ids)",
    "members": "members[] or member_contacts[]",
    "booker": "booker_user_id or booker_contact",
    "assistants": "engagement_id → all onboarding assistants",
    "assistants.phlebo": "engagement_id → phlebo assistants",
    "assistants.expert": "engagement_id → expert assistants",
    "assistants.staff": "engagement_id → staff assistants",
    "departments": "active employees with any department assignment",
    "departments.ops": "active employees in ops department",
    "departments.tech": "active employees in tech department",
    "departments.design": "active employees in design department",
    "custom": "phones and emails configured on this binding",
}
