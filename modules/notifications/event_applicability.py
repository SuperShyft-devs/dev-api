"""Engagement notification applicability and admin catalog grouping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

BloodCollectionRule = Literal["any", "none", "camp_collection", "home_collection"]
TriStateRule = Literal["any", "true", "false"]
ConsultationModeRule = Literal["any", "online", "offline"]

CATALOG_GROUP_ORDER: tuple[str, ...] = (
    "Camp & enrollment",
    "Home blood booking",
    "Consultations",
    "Reports & reminders",
    "Payments & checkout",
    "Platform",
)

_KINDS_WITH_BLOOD = frozenset(
    {
        "bio_ai",
        "blood_test",
        "blood_test_with_consultation",
        "bio_ai_with_consultation",
    }
)
_KINDS_WITH_CONSULTATION = frozenset(
    {
        "consultation",
        "blood_test_with_consultation",
        "bio_ai_with_consultation",
    }
)


@dataclass(frozen=True, slots=True)
class EngagementApplicability:
    """When an event may appear on engagement notification setup (step 4)."""

    engagement_kinds: tuple[str, ...] = ()
    blood_collection: BloodCollectionRule = "any"
    auto_booking: TriStateRule = "any"
    consultation_mode: ConsultationModeRule = "any"
    requires_consultation_type: bool = False
    platform_only: bool = False


@dataclass(frozen=True, slots=True)
class EngagementNotificationProfile:
    engagement_kind: str | None
    blood_collection_type: str | None
    auto_booking: bool | None
    consultation_mode: str | None
    payment_type: str | None = None

    @property
    def has_blood_collection(self) -> bool:
        return bool(self.blood_collection_type)

    @property
    def has_consultation_type(self) -> bool:
        k = (self.engagement_kind or "").strip()
        return k in _KINDS_WITH_CONSULTATION


def profile_from_mapping(data: dict[str, Any]) -> EngagementNotificationProfile:
    kind = data.get("engagement_kind") or data.get("engagement_type")
    if kind is not None:
        kind = str(kind).strip() or None
    bct = data.get("blood_collection_type")
    if bct is not None:
        bct = str(bct).strip() or None
    cm = data.get("consultation_mode")
    if cm is not None:
        cm = str(cm).strip() or None
    ab = data.get("auto_booking")
    auto: bool | None
    if ab is None:
        auto = None
    else:
        auto = bool(ab)
    pt = data.get("payment_type")
    if pt is not None:
        pt = str(pt).strip() or None
    return EngagementNotificationProfile(
        engagement_kind=kind,
        blood_collection_type=bct,
        auto_booking=auto,
        consultation_mode=cm,
        payment_type=pt,
    )


def _profile_from_row_fields(
    *,
    engagement_kind: str | None,
    blood_collection_type: str | None,
    auto_booking: bool | None,
    consultation_mode: str | None,
    payment_type: str | None = None,
) -> EngagementNotificationProfile:
    return EngagementNotificationProfile(
        engagement_kind=engagement_kind,
        blood_collection_type=blood_collection_type,
        auto_booking=auto_booking,
        consultation_mode=consultation_mode,
        payment_type=payment_type,
    )


def _payment_type_str(row: Any) -> str | None:
    pt = getattr(row, "payment_type", None)
    if pt is None:
        return None
    if hasattr(pt, "value"):
        return str(pt.value)
    return str(pt).strip() or None


def profile_from_engagement_row(row: Any) -> EngagementNotificationProfile:
    kind = getattr(row, "engagement_type", None)
    kind_str: str | None = None
    if kind is not None:
        if hasattr(kind, "value") and not isinstance(kind, int):
            kind_str = str(kind.value)
        elif isinstance(kind, int):
            kind_str = None
        else:
            kind_str = str(kind)
    bct = getattr(row, "blood_collection_type", None)
    bct_str = bct.value if hasattr(bct, "value") else str(bct) if bct else None
    cm = getattr(row, "consultation_mode", None)
    cm_str = cm.value if hasattr(cm, "value") else str(cm) if cm else None
    auto = getattr(row, "auto_booking", None)
    return _profile_from_row_fields(
        engagement_kind=kind_str,
        blood_collection_type=bct_str,
        auto_booking=bool(auto) if auto is not None else None,
        consultation_mode=cm_str,
        payment_type=_payment_type_str(row),
    )


async def profile_from_engagement(db: Any, row: Any) -> EngagementNotificationProfile:
    """Resolve engagement type code (FK id) and collection mode from a persisted engagement."""
    from modules.engagements.models import EngagementType

    type_ref = getattr(row, "engagement_type", None)
    kind_str: str | None = None
    if type_ref is not None:
        if isinstance(type_ref, int):
            et = await db.get(EngagementType, type_ref)
            kind_str = et.code if et else None
        elif isinstance(type_ref, str) and type_ref.isdigit():
            et = await db.get(EngagementType, int(type_ref))
            kind_str = et.code if et else None
        elif hasattr(type_ref, "value"):
            kind_str = str(type_ref.value)
        else:
            kind_str = str(type_ref)
    bct = getattr(row, "blood_collection_type", None)
    bct_str = bct.value if hasattr(bct, "value") else str(bct) if bct else None
    cm = getattr(row, "consultation_mode", None)
    cm_str = cm.value if hasattr(cm, "value") else str(cm) if cm else None
    auto = getattr(row, "auto_booking", None)
    return _profile_from_row_fields(
        engagement_kind=kind_str,
        blood_collection_type=bct_str,
        auto_booking=bool(auto) if auto is not None else None,
        consultation_mode=cm_str,
        payment_type=_payment_type_str(row),
    )


CANONICAL_ENGAGEMENT_PROFILE_KEYS: tuple[str, ...] = (
    "engagement_type_bio_aiandpayment_type_paid_by_groupandblood_collection_campandauto_booking_on",
    "engagement_type_bio_aiandpayment_type_paid_by_groupandblood_collection_campandauto_booking_off",
    "engagement_type_bio_aiandpayment_type_paid_by_groupandblood_collection_homeandauto_booking_on",
    "engagement_type_bio_aiandpayment_type_paid_by_groupandblood_collection_homeandauto_booking_off",
    "engagement_type_bio_aiandpayment_type_paid_by_patientandblood_collection_campandauto_booking_on",
    "engagement_type_bio_aiandpayment_type_paid_by_patientandblood_collection_campandauto_booking_off",
    "engagement_type_bio_aiandpayment_type_paid_by_patientandblood_collection_homeandauto_booking_on",
    "engagement_type_bio_aiandpayment_type_paid_by_patientandblood_collection_homeandauto_booking_off",
    "engagement_type_blood_testandpayment_type_paid_by_groupandblood_collection_campandauto_booking_on",
    "engagement_type_blood_testandpayment_type_paid_by_groupandblood_collection_campandauto_booking_off",
    "engagement_type_blood_testandpayment_type_paid_by_groupandblood_collection_homeandauto_booking_on",
    "engagement_type_blood_testandpayment_type_paid_by_groupandblood_collection_homeandauto_booking_off",
    "engagement_type_blood_testandpayment_type_paid_by_patientandblood_collection_campandauto_booking_on",
    "engagement_type_blood_testandpayment_type_paid_by_patientandblood_collection_campandauto_booking_off",
    "engagement_type_blood_testandpayment_type_paid_by_patientandblood_collection_homeandauto_booking_on",
    "engagement_type_blood_testandpayment_type_paid_by_patientandblood_collection_homeandauto_booking_off",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_groupandconsultation_mode_onlineandblood_collection_campandauto_booking_on",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_groupandconsultation_mode_onlineandblood_collection_campandauto_booking_off",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_groupandconsultation_mode_onlineandblood_collection_homeandauto_booking_on",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_groupandconsultation_mode_onlineandblood_collection_homeandauto_booking_off",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_groupandconsultation_mode_offlineandblood_collection_campandauto_booking_on",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_groupandconsultation_mode_offlineandblood_collection_campandauto_booking_off",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_groupandconsultation_mode_offlineandblood_collection_homeandauto_booking_on",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_groupandconsultation_mode_offlineandblood_collection_homeandauto_booking_off",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_patientandconsultation_mode_onlineandblood_collection_campandauto_booking_on",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_patientandconsultation_mode_onlineandblood_collection_campandauto_booking_off",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_patientandconsultation_mode_onlineandblood_collection_homeandauto_booking_on",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_patientandconsultation_mode_onlineandblood_collection_homeandauto_booking_off",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_patientandconsultation_mode_offlineandblood_collection_campandauto_booking_on",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_patientandconsultation_mode_offlineandblood_collection_campandauto_booking_off",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_patientandconsultation_mode_offlineandblood_collection_homeandauto_booking_on",
    "engagement_type_bio_ai_with_consultationandpayment_type_paid_by_patientandconsultation_mode_offlineandblood_collection_homeandauto_booking_off",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_groupandconsultation_mode_onlineandblood_collection_campandauto_booking_on",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_groupandconsultation_mode_onlineandblood_collection_campandauto_booking_off",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_groupandconsultation_mode_onlineandblood_collection_homeandauto_booking_on",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_groupandconsultation_mode_onlineandblood_collection_homeandauto_booking_off",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_groupandconsultation_mode_offlineandblood_collection_campandauto_booking_on",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_groupandconsultation_mode_offlineandblood_collection_campandauto_booking_off",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_groupandconsultation_mode_offlineandblood_collection_homeandauto_booking_on",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_groupandconsultation_mode_offlineandblood_collection_homeandauto_booking_off",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_patientandconsultation_mode_onlineandblood_collection_campandauto_booking_on",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_patientandconsultation_mode_onlineandblood_collection_campandauto_booking_off",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_patientandconsultation_mode_onlineandblood_collection_homeandauto_booking_on",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_patientandconsultation_mode_onlineandblood_collection_homeandauto_booking_off",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_patientandconsultation_mode_offlineandblood_collection_campandauto_booking_on",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_patientandconsultation_mode_offlineandblood_collection_campandauto_booking_off",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_patientandconsultation_mode_offlineandblood_collection_homeandauto_booking_on",
    "engagement_type_blood_test_with_consultationandpayment_type_paid_by_patientandconsultation_mode_offlineandblood_collection_homeandauto_booking_off",
    "engagement_type_consultationandpayment_type_paid_by_groupandconsultation_mode_online",
    "engagement_type_consultationandpayment_type_paid_by_groupandconsultation_mode_offline",
    "engagement_type_consultationandpayment_type_paid_by_patientandconsultation_mode_online",
    "engagement_type_consultationandpayment_type_paid_by_patientandconsultation_mode_offline",
    "engagement_type_vifcandpayment_type_paid_by_group",
    "engagement_type_vifcandpayment_type_paid_by_patient",
    "engagement_type_platformandpayment_type_paid_by_group",
    "engagement_type_platformandpayment_type_paid_by_patient",
)


def profile_from_profile_key(key: str) -> EngagementNotificationProfile:
    kind: str | None = None
    payment: str | None = None
    consult_mode: str | None = None
    blood: str | None = None
    auto: bool | None = None
    for segment in key.split("and"):
        if segment.startswith("engagement_type_"):
            kind = segment.removeprefix("engagement_type_")
        elif segment.startswith("payment_type_"):
            payment = segment.removeprefix("payment_type_")
        elif segment.startswith("consultation_mode_"):
            consult_mode = segment.removeprefix("consultation_mode_")
        elif segment.startswith("blood_collection_"):
            token = segment.removeprefix("blood_collection_")
            blood = f"{token}_collection" if token in {"camp", "home"} else token
        elif segment.startswith("auto_booking_"):
            auto = segment.removeprefix("auto_booking_") == "on"
    return _profile_from_row_fields(
        engagement_kind=kind,
        blood_collection_type=blood,
        auto_booking=auto,
        consultation_mode=consult_mode,
        payment_type=payment,
    )


def profile_key(profile: EngagementNotificationProfile) -> str:
    kind = (profile.engagement_kind or "unknown").strip()
    parts = [f"engagement_type_{kind}", f"payment_type_{profile.payment_type or 'paid_by_group'}"]
    if kind in _KINDS_WITH_CONSULTATION:
        parts.append(f"consultation_mode_{profile.consultation_mode or 'online'}")
    if kind in _KINDS_WITH_BLOOD:
        bct = profile.blood_collection_type or ""
        if bct == "camp_collection":
            parts.append("blood_collection_camp")
        elif bct == "home_collection":
            parts.append("blood_collection_home")
        elif bct:
            parts.append(f"blood_collection_{bct}")
        ab = profile.auto_booking if profile.auto_booking is not None else False
        parts.append(f"auto_booking_{'on' if ab else 'off'}")
    return "and".join(parts)


def profile_label(profile: EngagementNotificationProfile) -> str:
    kind = profile.engagement_kind or "—"
    pay = (profile.payment_type or "paid_by_group").replace("paid_by_", "")
    chunks = [kind, pay]
    if profile.consultation_mode:
        chunks.append(profile.consultation_mode)
    if profile.blood_collection_type:
        short = "camp" if profile.blood_collection_type == "camp_collection" else "home"
        chunks.append(short)
    if profile.auto_booking is not None and profile.blood_collection_type:
        chunks.append("auto on" if profile.auto_booking else "auto off")
    return " · ".join(chunks)


def profile_keys_for_event(event_code: str) -> tuple[str, ...]:
    out: list[str] = []
    for key in CANONICAL_ENGAGEMENT_PROFILE_KEYS:
        if engagement_notification_applicable(event_code, profile_from_profile_key(key)):
            out.append(key)
    return tuple(out)


def _match_tri(rule: TriStateRule, value: bool | None) -> bool:
    if rule == "any":
        return True
    if value is None:
        return rule == "any"
    if rule == "true":
        return value is True
    return value is False


def engagement_notification_applicable(
    event_code: str,
    profile: EngagementNotificationProfile,
) -> bool:
    meta = EVENT_CATALOG_META.get(event_code)
    if meta is None:
        return False
    rule: EngagementApplicability | None = meta.get("applicability")
    if rule is None or rule.platform_only:
        return False

    kind = (profile.engagement_kind or "").strip()
    if rule.engagement_kinds and kind not in rule.engagement_kinds:
        return False

    if rule.requires_consultation_type and not profile.has_consultation_type:
        return False

    if rule.blood_collection == "none":
        if profile.has_blood_collection:
            return False
    elif rule.blood_collection == "camp_collection":
        if profile.blood_collection_type != "camp_collection":
            return False
    elif rule.blood_collection == "home_collection":
        if profile.blood_collection_type != "home_collection":
            return False

    if not _match_tri(rule.auto_booking, profile.auto_booking):
        return False

    if rule.consultation_mode != "any":
        cm = (profile.consultation_mode or "").strip()
        if cm != rule.consultation_mode:
            return False

    return True


def catalog_groups_for_event(event_code: str) -> tuple[str, ...]:
    meta = EVENT_CATALOG_META.get(event_code)
    if not meta:
        return ("Platform",)
    return meta.get("groups") or ("Platform",)


def engagement_applicability_for_event(event_code: str) -> EngagementApplicability | None:
    meta = EVENT_CATALOG_META.get(event_code)
    if not meta:
        return None
    return meta.get("applicability")


def show_on_engagement_wizard(event_code: str) -> bool:
    rule = engagement_applicability_for_event(event_code)
    return rule is not None and not rule.platform_only


def list_applicable_event_codes(profile: EngagementNotificationProfile) -> list[str]:
    from modules.notifications.events_catalog import list_events

    out: list[str] = []
    for ev in list_events():
        if engagement_notification_applicable(ev.code, profile):
            out.append(ev.code)
    return sorted(out)


_BLOOD_KINDS = ("bio_ai", "blood_test", "blood_test_with_consultation", "bio_ai_with_consultation")
_CONSULT_KINDS = ("consultation", "blood_test_with_consultation", "bio_ai_with_consultation")
_ALL_ENGAGEMENT_KINDS = _BLOOD_KINDS + ("consultation",)


def _eng(
    *,
    kinds: tuple[str, ...] = _ALL_ENGAGEMENT_KINDS,
    blood: BloodCollectionRule = "any",
    auto: TriStateRule = "any",
    consult_mode: ConsultationModeRule = "any",
    requires_consultation_type: bool = False,
) -> EngagementApplicability:
    return EngagementApplicability(
        engagement_kinds=kinds,
        blood_collection=blood,
        auto_booking=auto,
        consultation_mode=consult_mode,
        requires_consultation_type=requires_consultation_type,
    )


EVENT_CATALOG_META: dict[str, dict[str, Any]] = {
    "auth.otp_sent": {
        "groups": ("Platform",),
        "applicability": EngagementApplicability(platform_only=True),
    },
    "users.registered": {
        "groups": ("Platform",),
        "applicability": EngagementApplicability(platform_only=True),
    },
    "users.onboarded": {
        "groups": ("Camp & enrollment", "Reports & reminders"),
        "applicability": _eng(),
    },
    "payments.completed": {
        "groups": ("Payments & checkout",),
        "applicability": EngagementApplicability(platform_only=True),
    },
    "payments.failed": {
        "groups": ("Payments & checkout",),
        "applicability": EngagementApplicability(platform_only=True),
    },
    "bookings.confirmed": {
        "groups": ("Home blood booking",),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="home_collection", auto="true"),
    },
    "bookings.rescheduled": {
        "groups": ("Home blood booking", "Camp & enrollment"),
        "applicability": _eng(kinds=_BLOOD_KINDS),
    },
    "bookings.cancelled": {
        "groups": ("Home blood booking", "Camp & enrollment"),
        "applicability": _eng(kinds=_BLOOD_KINDS),
    },
    "bookings.phlebo_assigned": {
        "groups": ("Home blood booking",),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="home_collection"),
    },
    "bookings.phlebo_reassigned": {
        "groups": ("Home blood booking",),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="home_collection"),
    },
    "bookings.phlebo_enroute": {
        "groups": ("Home blood booking",),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="home_collection"),
    },
    "bookings.phlebo_delayed": {
        "groups": ("Home blood booking",),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="home_collection"),
    },
    "bookings.sample_collected": {
        "groups": ("Home blood booking", "Camp & enrollment"),
        "applicability": _eng(kinds=_BLOOD_KINDS),
    },
    "consultations.booked": {
        "groups": ("Consultations",),
        "applicability": _eng(kinds=_CONSULT_KINDS, requires_consultation_type=True),
    },
    "consultations.confirmed": {
        "groups": ("Consultations",),
        "applicability": _eng(kinds=_CONSULT_KINDS, requires_consultation_type=True),
    },
    "consultations.rescheduled": {
        "groups": ("Consultations",),
        "applicability": _eng(kinds=_CONSULT_KINDS, requires_consultation_type=True),
    },
    "consultations.cancelled": {
        "groups": ("Consultations",),
        "applicability": _eng(kinds=_CONSULT_KINDS, requires_consultation_type=True),
    },
    "consultations.completed": {
        "groups": ("Consultations",),
        "applicability": _eng(kinds=_CONSULT_KINDS, requires_consultation_type=True),
    },
    "reports.blood_ready": {
        "groups": ("Reports & reminders",),
        "applicability": _eng(kinds=_BLOOD_KINDS),
    },
    "reports.bioai_ready": {
        "groups": ("Reports & reminders",),
        "applicability": _eng(kinds=("bio_ai", "bio_ai_with_consultation")),
    },
    "reports.face_scan_ready": {
        "groups": ("Reports & reminders",),
        "applicability": _eng(kinds=("bio_ai", "bio_ai_with_consultation")),
    },
    "reminders.pretest": {
        "groups": ("Camp & enrollment", "Reports & reminders"),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="camp_collection"),
    },
    "reminders.questionnaire_before": {
        "groups": ("Camp & enrollment", "Reports & reminders"),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="camp_collection"),
    },
    "reminders.questionnaire_after": {
        "groups": ("Camp & enrollment", "Reports & reminders"),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="camp_collection"),
    },
    "reminders.booking_guide": {
        "groups": ("Camp & enrollment",),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="camp_collection"),
    },
    "reminders.consultation_ready_to_book": {
        "groups": ("Consultations", "Reports & reminders"),
        "applicability": _eng(kinds=_CONSULT_KINDS, requires_consultation_type=True),
    },
    "reminders.consultation_reminder": {
        "groups": ("Consultations", "Reports & reminders"),
        "applicability": _eng(kinds=_CONSULT_KINDS, requires_consultation_type=True),
    },
    "support.ticket_created": {
        "groups": ("Platform",),
        "applicability": EngagementApplicability(platform_only=True),
    },
    "support.ticket_resolved": {
        "groups": ("Platform",),
        "applicability": EngagementApplicability(platform_only=True),
    },
    "system.auto_booking_failed": {
        "groups": ("Home blood booking",),
        "applicability": _eng(kinds=_BLOOD_KINDS, blood="home_collection", auto="true"),
    },
    "system.error": {
        "groups": ("Platform",),
        "applicability": EngagementApplicability(platform_only=True),
    },
    "system.cpu_threshold": {
        "groups": ("Platform",),
        "applicability": EngagementApplicability(platform_only=True),
    },
}

# Ensure every catalog event has metadata (unknown → Platform)
def _ensure_all_catalog_events() -> None:
    from modules.notifications.events_catalog import list_events

    for ev in list_events():
        if ev.code not in EVENT_CATALOG_META:
            EVENT_CATALOG_META[ev.code] = {
                "groups": ("Platform",),
                "applicability": EngagementApplicability(platform_only=True),
            }


_ensure_all_catalog_events()
