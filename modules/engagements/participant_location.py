"""Home vs camp collection address on engagement_participants and engagements."""

from __future__ import annotations

from typing import Any, Literal, Protocol

from modules.engagements.enums import BloodCollectionType
from modules.engagements.models import Engagement, EngagementParticipant


class _OnboardAddressPayload(Protocol):
    address: str | None
    pincode: str | None
    city: str | None
    state: str | None
    country: str | None


def _blood_collection_value(engagement: Engagement) -> str:
    bct = engagement.blood_collection_type
    if bct is None:
        return ""
    if hasattr(bct, "value"):
        return str(bct.value).strip().lower()
    return str(bct or "").strip().lower()


def blood_collection_type_value(blood_collection_type: Any) -> str:
    """Normalize blood_collection_type from ORM row or enum."""
    if blood_collection_type is None:
        return ""
    if hasattr(blood_collection_type, "value"):
        return str(blood_collection_type.value).strip().lower()
    return str(blood_collection_type or "").strip().lower()


def is_home_blood_collection(engagement: Engagement) -> bool:
    return _blood_collection_value(engagement) == BloodCollectionType.home_collection.value


def is_camp_blood_collection(engagement: Engagement) -> bool:
    return _blood_collection_value(engagement) == BloodCollectionType.camp_collection.value


def participant_has_home_location(participant: EngagementParticipant) -> bool:
    address = (participant.address or "").strip()
    pincode = (participant.pincode or "").strip()
    return bool(address or pincode)


def is_b2c_engagement(engagement: Engagement) -> bool:
    return engagement.organization_id is None


def can_seed_participant_address_from_engagement(
    engagement: Engagement,
    *,
    participant_count: int,
) -> bool:
    """Allow copying engagement text address onto a home participant (legacy repair)."""
    if participant_count != 1:
        return False
    if not is_home_blood_collection(engagement):
        return False
    return is_b2c_engagement(engagement)


def copy_engagement_text_address_to_participant(
    participant: EngagementParticipant,
    engagement: Engagement,
) -> None:
    """Copy textual address fields only (no lat/long/zone)."""
    participant.address = engagement.address
    participant.sub_locality = engagement.sub_locality
    participant.landmark = engagement.landmark
    participant.pincode = engagement.pincode
    participant.city = engagement.city
    participant.state = engagement.state
    participant.country = engagement.country


def apply_geocoded_home_location_to_participant(
    participant: EngagementParticipant,
    *,
    geocoded: dict[str, Any],
    address_line: str | None = None,
    landmark: str | None = None,
    city: str | None = None,
    pincode_fallback: str | None = None,
) -> None:
    """Persist home-collection address and geocode result on the participant."""
    if address_line is not None:
        stripped = address_line.strip()
        participant.address = stripped or None
        participant.sub_locality = stripped or None
    if landmark is not None:
        participant.landmark = landmark.strip() or None
    if city is not None:
        participant.city = city.strip() or None
    if pincode_fallback is not None:
        participant.pincode = pincode_fallback.strip() or None
    lat = geocoded.get("latitude")
    lng = geocoded.get("longitude")
    if lat is not None:
        participant.latitude = float(lat)
    if lng is not None:
        participant.longitude = float(lng)
    if geocoded.get("pincode") and not (participant.pincode or "").strip():
        participant.pincode = str(geocoded["pincode"]).strip() or None
    if geocoded.get("state"):
        participant.state = geocoded.get("state")
    if geocoded.get("country"):
        participant.country = geocoded.get("country")


def copy_engagement_location_to_participant(
    participant: EngagementParticipant,
    engagement: Engagement,
) -> None:
    """Mirror engagement location onto participant (camp / legacy flows)."""
    participant.address = engagement.address
    participant.sub_locality = engagement.sub_locality
    participant.landmark = engagement.landmark
    participant.pincode = engagement.pincode
    participant.city = engagement.city
    participant.state = engagement.state
    participant.country = engagement.country
    participant.latitude = engagement.latitude
    participant.longitude = engagement.longitude
    participant.healthians_zone_id = engagement.healthians_zone_id


def copy_onboard_payload_location_to_participant(
    participant: EngagementParticipant,
    payload: _OnboardAddressPayload,
) -> None:
    """Set participant home address from onboard payload (no lat/long until booking geocodes)."""
    if payload.address is not None:
        stripped = payload.address.strip()
        participant.address = stripped or None
        participant.sub_locality = stripped or None
    if payload.pincode is not None:
        participant.pincode = payload.pincode.strip() or None
    if payload.city is not None:
        participant.city = payload.city.strip() or None
    if payload.state is not None:
        participant.state = payload.state.strip() or None
    if payload.country is not None:
        participant.country = payload.country.strip() or None


def onboard_payload_has_address(payload: _OnboardAddressPayload) -> bool:
    for value in (
        payload.address,
        payload.pincode,
        payload.city,
        payload.state,
        payload.country,
    ):
        if value is not None and str(value).strip():
            return True
    return False


def address_profile_update_from_onboard_payload(payload: _OnboardAddressPayload) -> dict[str, Any]:
    """Map onboard payload location fields to users table column names."""
    update: dict[str, Any] = {}
    if payload.address is not None:
        update["address"] = payload.address
    if payload.pincode is not None:
        update["pin_code"] = payload.pincode
    if payload.city is not None:
        update["city"] = payload.city
    if payload.state is not None:
        update["state"] = payload.state
    if payload.country is not None:
        update["country"] = payload.country
    return update


def use_participant_home_for_booking(
    engagement: Engagement,
    participant: EngagementParticipant,
) -> bool:
    """Home collection bookings use participant address and persist geo on participant."""
    return is_home_blood_collection(engagement)


def booking_location_field(
    participant: EngagementParticipant,
    engagement: Engagement,
    field: str,
) -> Any:
    """Resolve location for partner booking and slot APIs by blood collection type."""
    if is_home_blood_collection(engagement):
        return getattr(participant, field, None)
    return getattr(engagement, field, None)


def _strip_text(value: str | None) -> str:
    return (value or "").strip()


def resolve_upcoming_slot_location_type(
    *,
    blood_collection_type: Any,
    organization_id: int | None,
) -> Literal["venue", "home_collection"]:
    """Map collection mode to upcoming-slot location.type."""
    bct = blood_collection_type_value(blood_collection_type)
    if bct == BloodCollectionType.home_collection.value:
        return "home_collection"
    if bct == BloodCollectionType.camp_collection.value:
        return "venue"
    return "venue" if organization_id is not None else "home_collection"


def resolve_upcoming_slot_location_display(
    *,
    blood_collection_type: Any,
    participant_address: str | None,
    engagement_address: str | None,
    engagement_city: str | None,
    user_address: str | None,
    user_city: str | None,
    organization_id: int | None,
    participant_count: int,
) -> str:
    """Resolve location.display for GET /users/me/upcoming-slot."""
    bct = blood_collection_type_value(blood_collection_type)
    engagement = Engagement(
        blood_collection_type=blood_collection_type,  # type: ignore[arg-type]
        organization_id=organization_id,
    )

    if bct == BloodCollectionType.camp_collection.value:
        eng_addr = _strip_text(engagement_address)
        if eng_addr:
            return eng_addr
        return _strip_text(engagement_city)

    if bct == BloodCollectionType.home_collection.value:
        participant_addr = _strip_text(participant_address)
        if participant_addr:
            return participant_addr
        if can_seed_participant_address_from_engagement(
            engagement,
            participant_count=participant_count,
        ):
            seeded = _strip_text(engagement_address)
            if seeded:
                return seeded
        user_addr = _strip_text(user_address)
        if user_addr:
            return user_addr
        return _strip_text(user_city)

    eng_addr = _strip_text(engagement_address)
    if eng_addr:
        return eng_addr
    if organization_id is not None:
        return _strip_text(engagement_city)
    user_addr = _strip_text(user_address)
    if user_addr:
        return user_addr
    return _strip_text(user_city)


def overlay_console_display_address(
    payload: dict[str, Any],
    *,
    participant: EngagementParticipant | None,
    engagement: Engagement,
) -> None:
    """Prefill console home-collection forms from participant (home) or engagement (camp)."""
    if is_home_blood_collection(engagement):
        if participant is not None and participant_has_home_location(participant):
            payload["address"] = participant.address
            payload["pin_code"] = participant.pincode
            payload["city"] = participant.city
            payload["state"] = participant.state
            payload["landmark"] = participant.landmark
        return

    engagement_address = (engagement.address or "").strip()
    engagement_pincode = (engagement.pincode or "").strip()
    if engagement_address or engagement_pincode:
        payload["address"] = engagement.address
        payload["pin_code"] = engagement.pincode
        payload["city"] = engagement.city
        payload["state"] = engagement.state
        payload["landmark"] = engagement.landmark
        return

    payload.setdefault("landmark", None)
