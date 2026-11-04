"""Booking flow service — check serviceability, get slots, lock, and create bookings via Healthians."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.phone import to_healthians_mobile
from core.config import settings
from core.exceptions import AppError
from db.transaction import release_request_transaction
from modules.bookings import orange_health_flow
from modules.diagnostics.external_codes import healthians_package_deal_id
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.healthians.sync_log import log_healthians_call
from modules.diagnostics.models import DiagnosticPackage
from modules.discounts.schemas import CartLine, CheckoutContext
from modules.diagnostics.orange_health.booking_helpers import is_orange_health
from modules.engagements.blood_bookings_access import (
    apply_schedule,
    current_booking_id,
    get_current_row,
    get_or_create_current,
    read_schedule_from_row,
)
from modules.engagements.blood_bookings_repository import BloodBookingsRepository
from modules.engagements.diagnostic_package_resolution import (
    healthians_gender_code,
    is_split_diagnostic_engagement,
    resolve_diagnostic_package_id,
)
from modules.engagements.enums import ConsultationMode, EngagementPaymentType
from modules.engagements.models import BloodCollectionType, Engagement, EngagementKind, EngagementParticipant, EngagementType
from modules.experts.consultation_bookings_repository import ConsultationBookingsRepository
from modules.experts.consultations import normalize_hhmm
from modules.experts.models import ConsultationBooking, Expert, ExpertAvailabilityOverrideModel
from modules.experts.repository import (
    ExpertAvailabilityOverrideRepository,
    ExpertAvailabilityRepository,
    ExpertsRepository,
)
from modules.experts.service import ExpertAvailabilityService
from modules.experts.slot_engine import parse_slot_time
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService, _generate_engagement_code
from modules.geocoding.client import search_places_for_booking
from modules.payments.models import Booking
from modules.payments.services import PaymentsService
from modules.users.models import User

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")
_CONSULTATION_PAY_REUSE_MINUTES = 10


def _is_healthians(pkg: DiagnosticPackage) -> bool:
    return (pkg.diagnostic_provider or "").strip().lower() == "healthians"


def _is_orange_health(pkg: DiagnosticPackage) -> bool:
    return is_orange_health(pkg)


async def _get_diagnostic_package(db: AsyncSession, package_id: int) -> DiagnosticPackage:
    result = await db.execute(
        select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == package_id)
    )
    pkg = result.scalar_one_or_none()
    if pkg is None:
        raise AppError(status_code=404, error_code="PACKAGE_NOT_FOUND", message=f"Diagnostic package {package_id} not found")
    return pkg


async def _get_healthians_token() -> str:
    """Fetch Healthians access token; map failures to a clear 502 for API clients."""
    try:
        return await healthians_client.get_access_token()
    except Exception as exc:
        logger.exception("Healthians access-token fetch failed")
        is_blocked = "403" in str(exc) or "blocked" in str(exc).lower()
        raise AppError(
            status_code=502,
            error_code="HEALTHIANS_IP_BLOCKED" if is_blocked else "HEALTHIANS_API_ERROR",
            message=(
                "Healthians is blocking requests from this server. "
                "The server IP needs to be whitelisted by Healthians."
                if is_blocked
                else f"Failed to authenticate with Healthians: {exc}"
            ),
        ) from exc


def _healthians_zone_id(resp: dict[str, Any]) -> Any:
    data = resp.get("data")
    if isinstance(data, dict):
        return data.get("zone_id")
    return None


async def _safe_log_healthians_call(db: AsyncSession, **kwargs: Any) -> None:
    """Best-effort Healthians sync log; never fails the booking request."""
    try:
        await log_healthians_call(db, **kwargs)
    except Exception:
        logger.exception("Failed to persist Healthians sync log")


async def _geocode_for_booking(
    db: AsyncSession,
    *,
    address_line: str | None = None,
    landmark: str | None = None,
    city: str | None = None,
    pincode: str | None = None,
    engagement_id: int | None = None,
    user_id: int | None = None,
) -> dict[str, Any]:
    from modules.platform_settings.repository import PlatformSettingsRepository

    primary = await PlatformSettingsRepository().resolve_geocoding_provider(db)
    results = await search_places_for_booking(
        address_line=address_line,
        landmark=landmark,
        city=city,
        pincode=pincode,
        primary=primary,
        limit=1,
        engagement_id=engagement_id,
        user_id=user_id,
    )
    if not results:
        return {}
    return results[0]


def _parse_slot_time(slot_str: str) -> time:
    text = (slot_str or "").strip()
    if not text:
        raise ValueError("Empty slot time")

    meridiem_match = re.search(r"\b(AM|PM)\b", text, re.IGNORECASE)
    meridiem = meridiem_match.group(1).upper() if meridiem_match else None
    time_part = re.sub(r"\s*(AM|PM)\s*", "", text, flags=re.IGNORECASE).strip()

    segments = time_part.split(":")
    if len(segments) < 2:
        raise ValueError(f"Invalid slot time format: {slot_str}")

    hour = int(segments[0])
    minute = int(segments[1])
    if meridiem == "PM" and hour != 12:
        hour += 12
    elif meridiem == "AM" and hour == 12:
        hour = 0
    return time(hour, minute)


def _slim_healthians_slot(slot: dict[str, Any]) -> dict[str, Any]:
    return {
        "end_time": slot.get("end_time"),
        "slot_date": slot.get("slot_date"),
        "slot_time": slot.get("slot_time"),
        "stm_id": slot.get("stm_id"),
    }


_RESCHEDULE_SLOT_STATUSES = {"scheduled", "running"}


def _slot_ops_blocked_reason(engagement: Engagement) -> str | None:
    """Draft bookings can pick a first slot; booked home collections can pick a reschedule slot."""
    status = (engagement.status or "").strip().lower()
    if status == "draft":
        return None
    if status in _RESCHEDULE_SLOT_STATUSES:
        bct = engagement.blood_collection_type
        bct_val = (bct.value if hasattr(bct, "value") else str(bct or "")).strip().lower()
        if bct_val == BloodCollectionType.home_collection.value:
            return None
        return "Slot selection after booking is only allowed for home collection"
    return "Engagement is not in draft status"


def _healthians_location_for_slots(
    engagement: Engagement,
    participant: EngagementParticipant,
) -> dict[str, str]:
    from modules.bookings.healthians_location import healthians_location_for_slots

    return healthians_location_for_slots(engagement, participant)


_WITH_CONSULTATION_CODE = {
    "bio_ai": "bio_ai_with_consultation",
    "blood_test": "blood_test_with_consultation",
}


async def _resolve_engagement_type_id(db, code: str) -> int | None:
    """Resolve an engagement type code to its integer ID."""
    from sqlalchemy import select
    result = await db.execute(
        select(EngagementType.id).where(EngagementType.code == code)
    )
    return result.scalar_one_or_none()


async def _apply_complementary_consultation(
    db,
    engagement: Engagement,
    pkg: DiagnosticPackage,
    base_type_id: int | None,
) -> None:
    """Set engagement.consultations and engagement_type from package complementary_consultation."""
    cc = pkg.complementary_consultation
    has_any = isinstance(cc, dict) and any(v is True for v in cc.values())
    if has_any and base_type_id is not None:
        engagement.consultations = cc
        base_type = await db.execute(
            select(EngagementType).where(EngagementType.id == base_type_id)
        )
        base_obj = base_type.scalar_one_or_none()
        if base_obj and base_obj.code in _WITH_CONSULTATION_CODE:
            new_code = _WITH_CONSULTATION_CODE[base_obj.code]
            new_id = await _resolve_engagement_type_id(db, new_code)
            engagement.engagement_type = new_id or base_type_id
        else:
            engagement.engagement_type = base_type_id
    elif base_type_id is not None:
        engagement.engagement_type = base_type_id


async def check_service_availability(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    engagements_service: EngagementsService,
    booked_by_user_id: int,
) -> list[dict[str, Any]]:
    """Check provider serviceability and create draft engagements for serviceable members."""
    results: list[dict[str, Any]] = []

    for member in members:
        user_id = member["user_id"]
        diagnostic_package_id = member["diagnostic_package_id"]
        address_line = member["address_line"]
        landmark = member.get("landmark")
        city = member["city"]
        pincode = member["pincode"]

        pkg = await _get_diagnostic_package(db, diagnostic_package_id)
        if not (_is_healthians(pkg) or _is_orange_health(pkg)):
            results.append({"user_id": user_id, "status": "error", "message": "Diagnostic provider is not supported"})
            continue

        geocoded = await _geocode_for_booking(
            db,
            address_line=address_line,
            landmark=landmark,
            city=city,
            pincode=pincode,
            user_id=user_id,
        )
        latitude = geocoded.get("latitude")
        longitude = geocoded.get("longitude")
        if latitude is None or longitude is None:
            results.append({"user_id": user_id, "status": "error", "message": "Could not geocode address"})
            continue

        user_result = await db.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        if user is None:
            results.append({"user_id": user_id, "status": "error", "message": "User not found"})
            continue

        address = address_line.strip()

        engagement = Engagement(
            engagement_name=f"{(user.first_name or 'user').strip()}-draft",
            metsights_engagement_id=None,
            organization_id=None,
            camp_no=None,
            engagement_code=_generate_engagement_code(),
            engagement_type=None,
            assessment_package_id=None,
            diagnostic_package_id=diagnostic_package_id,
            city=city,
            address=address,
            sub_locality=address,
            landmark=landmark,
            pincode=pincode,
            state=geocoded.get("state"),
            country=geocoded.get("country"),
            latitude=latitude,
            longitude=longitude,
            slot_duration=20,
            start_date=None,
            end_date=None,
            status="draft",
            healthians_zone_id=None,
            blood_collection_type=BloodCollectionType.home_collection,
            create_profile_on_metsights=False,
            enroll_for_fitprint_full=False,
            payment_type=EngagementPaymentType.paid_by_patient,
            auto_booking=False,
        )
        db.add(engagement)
        await db.flush()

        participant = EngagementParticipant(
            engagement_id=engagement.engagement_id,
            user_id=user_id,
            booked_by_user_id=booked_by_user_id,
            paid=False,
        )
        db.add(participant)
        await db.flush()
        from modules.engagements.participant_location import apply_geocoded_home_location_to_participant

        apply_geocoded_home_location_to_participant(
            participant,
            geocoded=geocoded,
            address_line=address,
            landmark=landmark,
            city=city,
            pincode_fallback=pincode,
        )
        db.add(participant)
        await db.flush()

        if _is_orange_health(pkg):
            await db.commit()
            oh_result = await orange_health_flow.check_serviceability_at_location(
                db,
                latitude=float(latitude),
                longitude=float(longitude),
                engagement_id=int(engagement.engagement_id),
            )
            if oh_result.get("status") != "success":
                engagement.status = "cancelled"
                await db.flush()
                await db.commit()
                results.append({
                    "user_id": user_id,
                    "engagement_id": engagement.engagement_id,
                    "status": "error",
                    "message": oh_result.get("message", "Location is not serviceable"),
                })
                continue
            results.append({
                "user_id": user_id,
                "engagement_id": engagement.engagement_id,
                "status": "success",
                "message": oh_result.get("message", "Serviceable"),
            })
            continue

        lat = str(latitude)
        lng = str(longitude)
        zipcode = pincode
        access_token = await _get_healthians_token()

        try:
            resp = await healthians_client.check_serviceability_by_location_v2(
                access_token,
                lat=lat,
                long=lng,
                zipcode=zipcode,
                is_ppmc_booking=0,
            )
        except Exception as exc:
            logger.exception("Healthians serviceability check failed for user %s", user_id)
            await _safe_log_healthians_call(
                db,
                engagement_id=engagement.engagement_id,
                user_id=user_id,
                provider="healthians",
                api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2",
                request_payload={"lat": lat, "long": lng, "zipcode": zipcode, "is_ppmc_booking": 0},
                status="failed",
                error_message=str(exc),
            )
            results.append({"user_id": user_id, "status": "error", "message": str(exc)})
            await db.commit()
            continue

        await _safe_log_healthians_call(
            db,
            engagement_id=engagement.engagement_id,
            user_id=user_id,
            provider="healthians",
            api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2",
            request_payload={"lat": lat, "long": lng, "zipcode": zipcode, "is_ppmc_booking": 0},
            response_payload=resp,
            status="success" if resp.get("status") else "failed",
        )

        if not resp.get("status"):
            engagement.status = "cancelled"
            await db.flush()
            await db.commit()
            results.append({
                "user_id": user_id,
                "engagement_id": engagement.engagement_id,
                "status": "not_serviceable",
                "message": resp.get("message", "This location is not serviceable."),
            })
            continue

        zone_id = _healthians_zone_id(resp)
        engagement.healthians_zone_id = str(zone_id) if zone_id else None
        participant.healthians_zone_id = str(zone_id) if zone_id else None
        db.add(participant)
        await db.flush()
        await db.commit()

        results.append({
            "user_id": user_id,
            "engagement_id": engagement.engagement_id,
            "status": "serviceable",
            "message": resp.get("message", "Serviceable"),
            "zone_id": zone_id,
        })

    return results


async def _ensure_healthians_zone_for_slots(
    db: AsyncSession,
    *,
    engagement: Engagement,
    participant: EngagementParticipant,
    loc: dict[str, str],
    user_id: int,
) -> tuple[dict[str, str], str | None]:
    from modules.bookings.healthians_location import ensure_healthians_zone_for_slots

    return await ensure_healthians_zone_for_slots(
        db,
        engagement=engagement,
        participant=participant,
        loc=loc,
        user_id=user_id,
    )


async def get_available_slots(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Fetch available slots for draft bookings and booked home-collection reschedules."""
    results: list[dict[str, Any]] = []

    for member in members:
        user_id = member["user_id"]
        engagement_id = member["engagement_id"]
        blood_collection_date: date = member["blood_collection_date"]

        engagement_result = await db.execute(
            select(Engagement).where(Engagement.engagement_id == engagement_id)
        )
        engagement = engagement_result.scalar_one_or_none()
        if engagement is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Engagement not found"})
            continue

        blocked = _slot_ops_blocked_reason(engagement)
        if blocked:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": blocked})
            continue

        participant_result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.user_id == user_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = participant_result.scalar_one_or_none()
        if participant is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "User is not a participant"})
            continue

        if not engagement.diagnostic_package_id:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "No diagnostic package"})
            continue

        pkg = await _get_diagnostic_package(db, engagement.diagnostic_package_id)
        if _is_orange_health(pkg):
            loc = _healthians_location_for_slots(engagement, participant)
            if not loc["lat"] or not loc["long"]:
                results.append({
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "status": "error",
                    "message": "Missing location on engagement",
                })
                continue
            slot_resp = await orange_health_flow.fetch_slots_for_location(
                db,
                blood_collection_date=blood_collection_date,
                latitude=float(loc["lat"]),
                longitude=float(loc["long"]),
                engagement_id=engagement_id,
            )
            if slot_resp.get("status") != "success":
                results.append({
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "status": "error",
                    "message": slot_resp.get("message", "Failed to fetch slots"),
                })
                continue
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "success",
                "slots": slot_resp.get("slots") or [],
            })
            continue
        if not _is_healthians(pkg):
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Not a supported package"})
            continue

        user_result = await db.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        has_female = 1 if user and (user.gender or "").strip().lower().startswith("f") else 0

        amount = float(pkg.original_price or pkg.price or 0)
        if not (pkg.external_package_code or "").strip():
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "Diagnostic package is missing Healthians deal id",
            })
            continue
        deal_id = healthians_package_deal_id(pkg.external_package_code)
        loc = _healthians_location_for_slots(engagement, participant)
        loc, loc_error = await _ensure_healthians_zone_for_slots(
            db,
            engagement=engagement,
            participant=participant,
            loc=loc,
            user_id=user_id,
        )
        if loc_error:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": loc_error,
            })
            continue
        access_token = await _get_healthians_token()

        payload = {
            "slot_date": blood_collection_date.isoformat(),
            "zone_id": loc["zone_id"],
            "lat": loc["lat"],
            "long": loc["long"],
            "zipcode": loc["zipcode"],
            "get_ppmc_slots": 0,
            "has_female_patient": has_female,
            "amount": amount,
            "package": [{"deal_id": [deal_id]}],
        }

        await release_request_transaction(db)

        try:
            resp = await healthians_client.get_slots_by_location(access_token, payload)
        except Exception as exc:
            logger.exception("Healthians getSlotsByLocation failed for user %s", user_id)
            await log_healthians_call(
                db,
                engagement_id=engagement_id,
                user_id=user_id,
                provider="healthians",
                api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/getSlotsByLocation",
                request_payload=payload,
                status="failed",
                error_message=str(exc),
            )
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": str(exc)})
            continue

        await log_healthians_call(
            db,
            engagement_id=engagement_id,
            user_id=user_id,
            provider="healthians",
            api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/getSlotsByLocation",
            request_payload=payload,
            response_payload=resp,
            status="success" if resp.get("status") else "failed",
        )

        if not resp.get("status"):
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": resp.get("message", "Failed to fetch slots"),
            })
            continue

        raw_slots = resp.get("data", []) or []
        slim_slots = [_slim_healthians_slot(slot) for slot in raw_slots if isinstance(slot, dict)]

        results.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "success",
            "slots": slim_slots,
        })

    return results


async def lock_slots(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Freeze or confirm slots for draft bookings and booked home-collection reschedules."""
    results: list[dict[str, Any]] = []

    for member in members:
        user_id = member["user_id"]
        engagement_id = member["engagement_id"]
        blood_collection_date: date = member["blood_collection_date"]
        slot_id: str = member["blood_collection_time_slot_id"]
        blood_collection_time_slot: str = member["blood_collection_time_slot"]

        engagement_result = await db.execute(
            select(Engagement).where(Engagement.engagement_id == engagement_id)
        )
        engagement = engagement_result.scalar_one_or_none()
        if engagement is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Engagement not found"})
            continue

        blocked = _slot_ops_blocked_reason(engagement)
        if blocked:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": blocked})
            continue

        participant_result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.user_id == user_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = participant_result.scalar_one_or_none()
        if participant is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "User is not a participant"})
            continue

        if not engagement.diagnostic_package_id:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "No diagnostic package"})
            continue

        pkg = await _get_diagnostic_package(db, engagement.diagnostic_package_id)
        if _is_orange_health(pkg):
            try:
                await orange_health_flow.lock_slot_local(
                    db,
                    participant=participant,
                    blood_collection_date=blood_collection_date,
                    slot_id=slot_id,
                    slot_time_label=blood_collection_time_slot,
                )
            except Exception as exc:
                results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": str(exc)})
                continue
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "success",
                "message": "Slot confirmed",
            })
            continue
        if not _is_healthians(pkg):
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Not a supported package"})
            continue

        vendor_billing_user_id = str(participant.booked_by_user_id)

        await release_request_transaction(db)
        access_token = await _get_healthians_token()

        try:
            resp = await healthians_client.freeze_slot_v1(
                access_token,
                slot_id=slot_id,
                vendor_billing_user_id=vendor_billing_user_id,
            )
        except Exception as exc:
            logger.exception("Healthians freezeSlot_v1 failed for user %s", user_id)
            await log_healthians_call(
                db,
                engagement_id=engagement_id,
                user_id=user_id,
                provider="healthians",
                api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/freezeSlot_v1",
                request_payload={"slot_id": slot_id, "vendor_billing_user_id": vendor_billing_user_id},
                status="failed",
                error_message=str(exc),
            )
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": str(exc)})
            continue

        await log_healthians_call(
            db,
            engagement_id=engagement_id,
            user_id=user_id,
            provider="healthians",
            api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/freezeSlot_v1",
            request_payload={"slot_id": slot_id, "vendor_billing_user_id": vendor_billing_user_id},
            response_payload=resp,
            status="success" if resp.get("status") and resp.get("resCode") == "RES0001" else "failed",
        )

        if not resp.get("status") or resp.get("resCode") != "RES0001":
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": resp.get("message", "Slot not available"),
            })
            continue

        try:
            slot_start_time = _parse_slot_time(blood_collection_time_slot)
        except ValueError as exc:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": str(exc),
            })
            continue

        await apply_schedule(
            db,
            participant,
            engagement_date=blood_collection_date,
            slot_start_time=slot_start_time,
            blood_collection_time_slot_id=slot_id,
        )

        results.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "success",
            "message": resp.get("message", "Slot locked"),
            "slot_id": resp.get("data", {}).get("slot_id") if resp.get("data") else slot_id,
            "freeze_time": resp.get("data", {}).get("freeze_time") if resp.get("data") else None,
        })

    return results


async def _validate_locked_draft_for_pay(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
    caller_user_id: int,
) -> tuple[Engagement, EngagementParticipant, DiagnosticPackage]:
    """Validate a draft engagement is locked and ready for payment."""
    engagement_result = await db.execute(
        select(Engagement).where(Engagement.engagement_id == engagement_id)
    )
    engagement = engagement_result.scalar_one_or_none()
    if engagement is None:
        raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement not found")
    if (engagement.status or "").lower() != "draft":
        raise AppError(status_code=422, error_code="INVALID_STATE", message="Engagement is not in draft status")
    if not engagement.diagnostic_package_id:
        raise AppError(status_code=422, error_code="INVALID_STATE", message="No diagnostic package on engagement")

    participant_result = await db.execute(
        select(EngagementParticipant)
        .where(EngagementParticipant.engagement_id == engagement_id)
        .where(EngagementParticipant.user_id == user_id)
        .order_by(EngagementParticipant.engagement_participant_id.desc())
        .limit(1)
    )
    participant = participant_result.scalar_one_or_none()
    if participant is None:
        raise AppError(status_code=404, error_code="NOT_ENROLLED", message="User is not a participant")
    if caller_user_id not in (participant.user_id, participant.booked_by_user_id):
        raise AppError(status_code=403, error_code="FORBIDDEN", message="Not authorized to pay for this participant")
    coll = await get_current_row(db, participant)
    sched = read_schedule_from_row(coll)
    if not sched["blood_collection_time_slot_id"]:
        raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection slot is not locked")
    if sched["engagement_date"] is None:
        raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection date is not set")
    if sched["slot_start_time"] is None:
        raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection time is not set")

    pkg = await _get_diagnostic_package(db, engagement.diagnostic_package_id)
    if not (_is_healthians(pkg) or _is_orange_health(pkg)):
        raise AppError(status_code=422, error_code="INVALID_PROVIDER", message="Not a supported diagnostic package")
    if (pkg.status or "").strip().lower() != "active":
        raise AppError(status_code=422, error_code="PACKAGE_INACTIVE", message="Diagnostic package is not active")

    return engagement, participant, pkg


async def preview_checkout_price(
    db: AsyncSession,
    *,
    payer_user_id: int,
    items: list[CartLine],
    context: CheckoutContext,
    discount_code: str | None = None,
    client_ip: str | None = None,
) -> dict[str, Any]:
    from modules.discounts.service import DiscountService

    service = DiscountService()
    result = await service.preview_checkout_price(
        db,
        code=discount_code,
        user_id=payer_user_id,
        items=items,
        context=context,
        client_ip=client_ip,
    )
    err = result.get("_error")
    if err:
        status_code, message = err
        raise AppError(status_code=status_code, error_code="CHECKOUT_PREVIEW_ERROR", message=message)
    return result


async def create_pay_order_for_draft_engagements(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    payer_user_id: int,
    discount_code: str | None = None,
    client_ip: str | None = None,
) -> dict[str, Any]:
    """Create a Razorpay order for locked draft engagements."""
    validated_items: list[tuple[int, str, int]] = []
    metadata_by_user: dict[int, dict[str, Any]] = {}
    member_statuses: list[dict[str, Any]] = []
    discount_context: dict[str, Any] | None = None

    for member in members:
        user_id = member["user_id"]
        engagement_id = member["engagement_id"]
        engagement, _participant, pkg = await _validate_locked_draft_for_pay(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
            caller_user_id=payer_user_id,
        )
        package_id = int(engagement.diagnostic_package_id)
        validated_items.append((user_id, "diagnostic_package", package_id))
        metadata_by_user[user_id] = {"engagement_id": engagement_id}
        member_statuses.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "success",
        })
        if discount_context is None:
            discount_context = {
                "organization_id": engagement.organization_id,
                "camp_no": str(engagement.camp_no) if engagement.camp_no is not None else None,
                "engagement_id": engagement.engagement_id,
                "city": getattr(engagement, "city", None),
            }

    payments_service = PaymentsService()
    result = await payments_service.create_order(
        db,
        payer_user_id=payer_user_id,
        items=validated_items,
        authenticated_user_id=payer_user_id,
        booking_type=None,
        metadata_by_user=metadata_by_user,
        discount_code=discount_code.strip() if discount_code else None,
        discount_context=discount_context,
        client_ip=client_ip,
    )
    err = result.get("_error")
    if err:
        code, msg = err
        raise AppError(status_code=code, error_code="PAYMENT_ERROR", message=msg)

    out = {
        "razorpay_order_id": result["razorpay_order_id"],
        "amount_paise": result["amount_paise"],
        "amount_rupees": result["amount_rupees"],
        "currency": result["currency"],
        "key_id": result["key_id"],
        "booking_ids": result["booking_ids"],
        "booking_id": result["booking_id"],
        "members": member_statuses,
    }
    if result.get("discount"):
        out["discount"] = result["discount"]
    return out


async def _validate_engagement_participant_for_code_pay(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
    payer_user_id: int,
) -> tuple[Engagement, EngagementParticipant, DiagnosticPackage]:
    from modules.engagements.participant_eligibility import engagement_payment_type_value
    from modules.engagements.enums import EngagementPaymentType

    engagement_result = await db.execute(
        select(Engagement).where(Engagement.engagement_id == engagement_id)
    )
    engagement = engagement_result.scalar_one_or_none()
    if engagement is None:
        raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement not found")
    if engagement_payment_type_value(engagement) != EngagementPaymentType.paid_by_patient.value:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Engagement does not require participant payment",
        )

    participant_result = await db.execute(
        select(EngagementParticipant)
        .where(EngagementParticipant.engagement_id == engagement_id)
        .where(EngagementParticipant.user_id == user_id)
        .order_by(EngagementParticipant.engagement_participant_id.desc())
        .limit(1)
    )
    participant = participant_result.scalar_one_or_none()
    if participant is None:
        raise AppError(status_code=404, error_code="NOT_ENROLLED", message="User is not a participant")
    if bool(participant.paid):
        raise AppError(status_code=422, error_code="ALREADY_PAID", message="Participant has already paid")
    if payer_user_id not in (participant.user_id, participant.booked_by_user_id):
        raise AppError(status_code=403, error_code="FORBIDDEN", message="Not authorized to pay for this participant")

    user_result = await db.execute(select(User).where(User.user_id == user_id))
    user = user_result.scalar_one_or_none()
    if user is None:
        raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User not found")

    package_id = resolve_diagnostic_package_id(engagement, user_gender=user.gender)
    if package_id is None:
        raise AppError(status_code=422, error_code="INVALID_STATE", message="No diagnostic package on engagement")
    pkg = await _get_diagnostic_package(db, package_id)
    if not (_is_healthians(pkg) or _is_orange_health(pkg)):
        raise AppError(status_code=422, error_code="INVALID_PROVIDER", message="Not a supported diagnostic package")
    if (pkg.status or "").strip().lower() != "active":
        raise AppError(status_code=422, error_code="PACKAGE_INACTIVE", message="Diagnostic package is not active")

    if bool(engagement.auto_booking):
        coll = await get_current_row(db, participant)
        sched = read_schedule_from_row(coll)
        if not sched["blood_collection_time_slot_id"]:
            raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection slot is not locked")
        if sched["engagement_date"] is None or sched["slot_start_time"] is None:
            raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection schedule is incomplete")

    return engagement, participant, pkg


async def create_pay_order_for_family_onboard(
    db: AsyncSession,
    *,
    members: list[dict[str, int]],
    payer_user_id: int,
    discount_code: str | None = None,
    client_ip: str | None = None,
) -> dict[str, Any]:
    """Create one Razorpay order for all family-onboard session members."""
    validated_items: list[tuple[int, str, int]] = []
    metadata_by_user: dict[int, dict[str, Any]] = {}
    member_statuses: list[dict[str, Any]] = []
    discount_context: dict[str, Any] | None = None

    for member in members:
        user_id = int(member["user_id"])
        engagement_id = int(member["engagement_id"])
        engagement, _participant, pkg = await _validate_engagement_participant_for_code_pay(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
            payer_user_id=payer_user_id,
        )
        if (engagement.status or "").strip().lower() != "draft":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Engagement is not in draft status",
            )
        package_id = int(pkg.diagnostic_package_id)
        validated_items.append((user_id, "diagnostic_package", package_id))
        metadata_by_user[user_id] = {"engagement_id": engagement_id}
        member_statuses.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "success",
        })
        if discount_context is None:
            discount_context = {
                "organization_id": engagement.organization_id,
                "camp_no": str(engagement.camp_no) if engagement.camp_no is not None else None,
                "engagement_id": engagement.engagement_id,
                "city": getattr(engagement, "city", None),
            }

    payments_service = PaymentsService()
    result = await payments_service.create_order(
        db,
        payer_user_id=payer_user_id,
        items=validated_items,
        authenticated_user_id=payer_user_id,
        booking_type=None,
        metadata_by_user=metadata_by_user,
        discount_code=discount_code.strip() if discount_code else None,
        discount_context=discount_context,
        client_ip=client_ip,
    )
    err = result.get("_error")
    if err:
        code, msg = err
        raise AppError(status_code=code, error_code="PAYMENT_ERROR", message=msg)

    out: dict[str, Any] = {
        "razorpay_order_id": result["razorpay_order_id"],
        "amount_paise": result["amount_paise"],
        "amount_rupees": result["amount_rupees"],
        "currency": result["currency"],
        "key_id": result["key_id"],
        "booking_ids": result["booking_ids"],
        "booking_id": result["booking_id"],
        "members": member_statuses,
    }
    if result.get("discount"):
        out["discount"] = result["discount"]
    return out


async def create_pay_order_for_engagement_participant(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
    payer_user_id: int,
    discount_code: str | None = None,
    client_ip: str | None = None,
) -> dict[str, Any]:
    """Create Razorpay order for a paid_by_patient engagement participant (code onboard flow)."""
    engagement, _participant, pkg = await _validate_engagement_participant_for_code_pay(
        db,
        user_id=user_id,
        engagement_id=engagement_id,
        payer_user_id=payer_user_id,
    )
    package_id = int(pkg.diagnostic_package_id)
    metadata_by_user = {user_id: {"engagement_id": engagement_id}}
    discount_context = {
        "organization_id": engagement.organization_id,
        "camp_no": str(engagement.camp_no) if engagement.camp_no is not None else None,
        "engagement_id": engagement.engagement_id,
        "city": getattr(engagement, "city", None),
    }
    payments_service = PaymentsService()
    result = await payments_service.create_order(
        db,
        payer_user_id=payer_user_id,
        items=[(user_id, "diagnostic_package", package_id)],
        authenticated_user_id=payer_user_id,
        booking_type=None,
        metadata_by_user=metadata_by_user,
        discount_code=discount_code.strip() if discount_code else None,
        discount_context=discount_context,
        client_ip=client_ip,
    )
    err = result.get("_error")
    if err:
        code, msg = err
        raise AppError(status_code=code, error_code="PAYMENT_ERROR", message=msg)
    return {
        "razorpay_order_id": result["razorpay_order_id"],
        "amount_paise": result["amount_paise"],
        "amount_rupees": result["amount_rupees"],
        "currency": result["currency"],
        "key_id": result["key_id"],
        "booking_ids": result["booking_ids"],
        "booking_id": result["booking_id"],
        "user_id": user_id,
        "engagement_id": engagement_id,
        "discount": result.get("discount"),
    }


async def verify_and_finalize_engagement_code_payment(
    db: AsyncSession,
    *,
    razorpay_payment_id: str,
    razorpay_order_id: str,
    razorpay_signature: str,
    caller_user_id: int,
    engagements_service: EngagementsService | None = None,
) -> dict[str, Any]:
    """Verify Razorpay payment for code-onboard flow; mark paid and optionally auto-book."""
    payments_service = PaymentsService()
    verify_result = await payments_service.verify_payment(
        db,
        razorpay_payment_id=razorpay_payment_id,
        razorpay_order_id=razorpay_order_id,
        razorpay_signature=razorpay_signature,
        authenticated_user_id=caller_user_id,
        skip_fulfillment=True,
    )
    if verify_result.get("_signature_invalid"):
        raise AppError(status_code=400, error_code="INVALID_SIGNATURE", message="Invalid payment signature")
    err = verify_result.get("_error")
    if err:
        code, msg = err
        raise AppError(status_code=code, error_code="PAYMENT_VERIFY_FAILED", message=msg)

    bookings = verify_result.get("bookings") or []
    if not bookings:
        booking_ids = verify_result.get("booking_ids") or []
        if booking_ids:
            bookings_result = await db.execute(
                select(Booking).where(Booking.booking_id.in_(booking_ids))
            )
            bookings = list(bookings_result.scalars().all())

    members = _members_from_order_bookings(bookings)
    for member in members:
        participant_result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == member["engagement_id"])
            .where(EngagementParticipant.user_id == member["user_id"])
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = participant_result.scalar_one_or_none()
        if participant is not None:
            participant.paid = True
            db.add(participant)
    await db.flush()

    member_results: list[dict[str, Any]] = []
    for member in members:
        engagement_result = await db.execute(
            select(Engagement).where(Engagement.engagement_id == member["engagement_id"])
        )
        engagement = engagement_result.scalar_one_or_none()
        if engagement is not None and bool(engagement.auto_booking):
            booking_results = await create_healthians_booking_after_payment(
                db,
                members=[member],
                caller_user_id=caller_user_id,
                engagements_service=engagements_service,
                allow_active_engagement_status=True,
                preserve_engagement_status=True,
            )
            member_results.extend(booking_results)
        else:
            member_results.append({
                "user_id": member["user_id"],
                "engagement_id": member["engagement_id"],
                "status": "success",
                "message": "Payment recorded",
            })

    return {
        "payment_verified": True,
        "razorpay_order_id": razorpay_order_id,
        "razorpay_payment_id": verify_result.get("payment_id") or razorpay_payment_id,
        "booking_ids": verify_result.get("booking_ids") or [],
        "members": member_results,
    }


def _members_from_order_bookings(bookings: list[Booking]) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    for booking in bookings:
        meta = booking.metadata_ or {}
        engagement_id = meta.get("engagement_id")
        if engagement_id is None:
            raise AppError(
                status_code=422,
                error_code="MISSING_ENGAGEMENT_ID",
                message=f"Booking {booking.booking_id} is missing engagement_id in metadata",
            )
        members.append({
            "user_id": int(booking.user_id),
            "engagement_id": int(engagement_id),
        })
    return members


async def verify_and_finalize_draft_bookings(
    db: AsyncSession,
    *,
    razorpay_payment_id: str,
    razorpay_order_id: str,
    razorpay_signature: str,
    caller_user_id: int,
    engagement_type_code: str | None = None,
    engagements_service: EngagementsService | None = None,
) -> dict[str, Any]:
    """Verify Razorpay payment and finalize Healthians bookings for draft engagements."""
    payments_service = PaymentsService()
    verify_result = await payments_service.verify_payment(
        db,
        razorpay_payment_id=razorpay_payment_id,
        razorpay_order_id=razorpay_order_id,
        razorpay_signature=razorpay_signature,
        authenticated_user_id=caller_user_id,
        skip_fulfillment=True,
    )

    if verify_result.get("_signature_invalid"):
        raise AppError(status_code=400, error_code="INVALID_SIGNATURE", message="Invalid payment signature")
    err = verify_result.get("_error")
    if err:
        code, msg = err
        raise AppError(status_code=code, error_code="PAYMENT_VERIFY_FAILED", message=msg)

    bookings = verify_result.get("bookings") or []
    if not bookings:
        booking_ids = verify_result.get("booking_ids") or []
        if booking_ids:
            bookings_result = await db.execute(
                select(Booking).where(Booking.booking_id.in_(booking_ids))
            )
            bookings = list(bookings_result.scalars().all())

    members = _members_from_order_bookings(bookings)
    member_results = await create_healthians_booking_after_payment(
        db,
        members=members,
        caller_user_id=caller_user_id,
        engagement_type_code=engagement_type_code,
        engagements_service=engagements_service,
    )

    return {
        "payment_verified": True,
        "razorpay_order_id": razorpay_order_id,
        "razorpay_payment_id": verify_result.get("payment_id") or razorpay_payment_id,
        "booking_ids": verify_result.get("booking_ids") or [],
        "members": member_results,
    }


def _booking_location_field(participant: EngagementParticipant, engagement: Engagement, field: str) -> Any:
    from modules.engagements.participant_location import booking_location_field

    return booking_location_field(participant, engagement, field)


async def _freeze_healthians_slot_for_participant(
    db: AsyncSession,
    *,
    engagement_id: int,
    user_id: int,
    slot_id: str,
    vendor_billing_user_id: str,
    access_token: str | None = None,
) -> str:
    """Freeze a Healthians slot before createBooking_v3 (console-aligned billing user)."""
    slot_id = (slot_id or "").strip()
    if not slot_id:
        return access_token or await _get_healthians_token()

    billing_id = (vendor_billing_user_id or "").strip() or str(user_id)
    token = access_token or await _get_healthians_token()
    api_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/freezeSlot_v1"
    req_payload = {"slot_id": slot_id, "vendor_billing_user_id": billing_id}

    await release_request_transaction(db)
    try:
        resp = await healthians_client.freeze_slot_v1(
            token,
            slot_id=slot_id,
            vendor_billing_user_id=billing_id,
        )
    except Exception as exc:
        logger.exception("Healthians freezeSlot_v1 failed for user %s", user_id)
        await log_healthians_call(
            db,
            engagement_id=engagement_id,
            user_id=user_id,
            provider="healthians",
            api_url=api_url,
            request_payload=req_payload,
            status="failed",
            error_message=str(exc),
        )
        raise AppError(status_code=502, error_code="HEALTHIANS_LOCK_FAILED", message=str(exc)) from exc

    await log_healthians_call(
        db,
        engagement_id=engagement_id,
        user_id=user_id,
        provider="healthians",
        api_url=api_url,
        request_payload=req_payload,
        response_payload=resp,
        status="success" if resp.get("status") and resp.get("resCode") == "RES0001" else "failed",
    )

    if not resp.get("status") or resp.get("resCode") != "RES0001":
        raise AppError(
            status_code=422,
            error_code="SLOT_LOCK_FAILED",
            message=resp.get("message", "Slot not available"),
        )
    return token


async def fulfill_auto_partner_booking_for_members(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    caller_user_id: int,
    engagements_service: EngagementsService | None = None,
    allow_active_engagement_status: bool = False,
    preserve_engagement_status: bool = False,
) -> list[dict[str, Any]]:
    """Create partner lab orders for members whose engagements have auto_booking enabled."""
    auto_members: list[dict[str, Any]] = []
    for member in members:
        engagement_id = member.get("engagement_id")
        if engagement_id is None:
            continue
        engagement_result = await db.execute(
            select(Engagement).where(Engagement.engagement_id == int(engagement_id))
        )
        engagement = engagement_result.scalar_one_or_none()
        if engagement is not None and bool(engagement.auto_booking):
            auto_members.append(member)
    if not auto_members:
        return []
    return await create_healthians_booking_after_payment(
        db,
        members=auto_members,
        caller_user_id=caller_user_id,
        engagements_service=engagements_service,
        allow_active_engagement_status=allow_active_engagement_status,
        preserve_engagement_status=preserve_engagement_status,
    )


async def create_healthians_booking_after_payment(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    caller_user_id: int,
    engagement_type_code: str | None = None,
    engagements_service: EngagementsService | None = None,
    vendor_billing_user_id_override: str | None = None,
    allow_active_engagement_status: bool = False,
    preserve_engagement_status: bool = False,
) -> list[dict[str, Any]]:
    """After payment succeeds, create provider booking for each member using their drafted engagement."""
    results: list[dict[str, Any]] = []
    access_token: str | None = None

    for member in members:
        user_id = member["user_id"]
        engagement_id = member["engagement_id"]

        engagement_result = await db.execute(
            select(Engagement).where(Engagement.engagement_id == engagement_id)
        )
        engagement = engagement_result.scalar_one_or_none()
        if engagement is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Engagement not found"})
            continue

        participant_result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.user_id == user_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = participant_result.scalar_one_or_none()
        if participant is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "User not enrolled"})
            continue

        if caller_user_id not in (participant.user_id, participant.booked_by_user_id):
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "Not authorized to book for this participant",
            })
            continue

        status_lower = (engagement.status or "").lower()
        existing_bid = await current_booking_id(db, participant)
        if status_lower == "scheduled" and existing_bid:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "success",
                "message": "Booking already placed",
                "booking_id": existing_bid,
            })
            continue

        allowed_statuses = {"draft"}
        if allow_active_engagement_status:
            allowed_statuses.update({"scheduled", "running"})
        if status_lower not in allowed_statuses:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Engagement is not in draft status"})
            continue

        user_result = await db.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        if user is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "User not found"})
            continue

        try:
            package_id = resolve_diagnostic_package_id(engagement, user_gender=user.gender)
        except AppError as exc:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": exc.message,
            })
            continue

        pkg = await _get_diagnostic_package(db, package_id)
        if _is_orange_health(pkg):
            oh_result = await orange_health_flow.create_booking_for_member(
                db,
                engagement_id=engagement_id,
                user_id=user_id,
                participant=participant,
                engagement=engagement,
                user=user,
                pkg=pkg,
                partner_notes="",
            )
            if oh_result.get("status") == "success":
                participant.paid = True
                db.add(participant)
                base_type_id = await _resolve_engagement_type_id(db, engagement_type_code) if engagement_type_code else None
                await _apply_complementary_consultation(db, engagement, pkg, base_type_id)
                if not preserve_engagement_status:
                    engagement.status = "scheduled"
                if engagements_service is not None and engagement.organization_id is None:
                    coll_row = await get_current_row(db, participant)
                    sched_after = read_schedule_from_row(coll_row)
                    await engagements_service.apply_b2c_defaults_and_notify_after_booking(
                        db,
                        engagement=engagement,
                        user=user,
                        collection_date=sched_after.get("engagement_date"),
                        collection_time=sched_after.get("slot_start_time"),
                    )
                await db.flush()
            results.append(oh_result)
            continue
        if not _is_healthians(pkg):
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Not a supported package"})
            continue

        full_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip() or "User"
        if is_split_diagnostic_engagement(engagement):
            gender_code = healthians_gender_code(user.gender) or ""
        else:
            gender_code = "M"
            if (user.gender or "").strip().lower().startswith("f"):
                gender_code = "F"

        age = user.age or 30
        dob = ""
        if user.date_of_birth:
            dob = user.date_of_birth.strftime("%d/%m/%Y")

        deal_id = healthians_package_deal_id(pkg.external_package_code)
        coll_row = await get_current_row(db, participant)
        sched = read_schedule_from_row(coll_row)
        slot_id = sched["blood_collection_time_slot_id"] or ""
        relation = (user.relationship or "self").strip() or "self"
        vendor_billing_user_id = vendor_billing_user_id_override or str(
            participant.booked_by_user_id or user_id
        )

        phone = to_healthians_mobile((user.phone or "").strip())
        if len(phone) != 10:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "Participant phone must be a valid 10-digit mobile number",
            })
            continue

        loc = _healthians_location_for_slots(engagement, participant)
        loc, loc_error = await _ensure_healthians_zone_for_slots(
            db,
            engagement=engagement,
            participant=participant,
            loc=loc,
            user_id=user_id,
        )
        if loc_error:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": loc_error,
            })
            continue

        user_email = (user.email or "").strip()
        booking_payload = {
            "customer": [{
                "customer_id": str(user_id),
                "customer_name": full_name.upper(),
                "relation": relation,
                "age": age,
                "dob": dob,
                "gender": gender_code,
                "contact_number": phone,
                "email": user_email,
            }],
            "slot": {"slot_id": slot_id},
            "package": [{"deal_id": [deal_id]}],
            "customer_calling_number": phone,
            "billing_cust_name": full_name.upper(),
            "gender": gender_code,
            "mobile": phone,
            "email": user_email,
            "sub_locality": _booking_location_field(participant, engagement, "sub_locality") or "",
            "latitude": loc["lat"],
            "longitude": loc["long"],
            "address": _booking_location_field(participant, engagement, "address") or "",
            "zipcode": loc["zipcode"] or _booking_location_field(participant, engagement, "pincode") or "",
            "landmark": _booking_location_field(participant, engagement, "landmark") or "",
            "hard_copy": 0,
            "vendor_billing_user_id": vendor_billing_user_id,
            "payment_option": "prepaid",
            "discounted_price": 0,
            "zone_id": int(loc["zone_id"] or 0),
            "is_ppmc_booking": 0,
        }
        if not booking_payload["zone_id"]:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "Healthians zone could not be resolved for this address",
            })
            continue

        try:
            access_token = await _freeze_healthians_slot_for_participant(
                db,
                engagement_id=engagement_id,
                user_id=user_id,
                slot_id=slot_id,
                vendor_billing_user_id=vendor_billing_user_id,
                access_token=access_token,
            )
        except AppError as exc:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": exc.message,
            })
            continue

        await release_request_transaction(db)

        if access_token is None:
            access_token = await _get_healthians_token()

        try:
            resp = await healthians_client.create_booking_v3(
                access_token,
                booking_payload,
                checksum_key=settings.HEALTHIANS_CHECKSUM_KEY,
            )
        except Exception as exc:
            logger.exception("Healthians createBooking_v3 failed for user %s", user_id)
            await log_healthians_call(
                db,
                engagement_id=engagement_id,
                user_id=user_id,
                provider="healthians",
                api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/createBooking_v3",
                request_payload=booking_payload,
                status="failed",
                error_message=str(exc),
            )
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": str(exc)})
            continue

        await log_healthians_call(
            db,
            engagement_id=engagement_id,
            user_id=user_id,
            provider="healthians",
            api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/createBooking_v3",
            request_payload=booking_payload,
            response_payload=resp,
            status="success" if resp.get("status") is True else "failed",
        )

        if resp.get("status") is not True:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": resp.get("message", "Booking failed"),
            })
            continue

        booking_id = resp.get("booking_id", "")
        participant.paid = True
        db.add(participant)
        coll_row = await get_or_create_current(db, participant)
        coll_row.booking_id = str(booking_id)
        coll_row.barcode = str(booking_id)
        db.add(coll_row)
        await db.flush()
        sched_after = read_schedule_from_row(coll_row)
        base_type_id = await _resolve_engagement_type_id(db, engagement_type_code) if engagement_type_code else None
        await _apply_complementary_consultation(db, engagement, pkg, base_type_id)
        if not preserve_engagement_status:
            engagement.status = "scheduled"

        if engagements_service is not None and engagement.organization_id is None:
            await engagements_service.apply_b2c_defaults_and_notify_after_booking(
                db,
                engagement=engagement,
                user=user,
                collection_date=sched_after.get("engagement_date"),
                collection_time=sched_after.get("slot_start_time"),
            )

        await db.flush()

        results.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "success",
            "message": resp.get("message", "Booking placed"),
            "booking_id": booking_id,
        })

    return results


async def _get_healthians_package_for_engagement(
    db: AsyncSession,
    engagement: Engagement,
    *,
    user_id: int,
) -> DiagnosticPackage:
    user_result = await db.execute(select(User).where(User.user_id == user_id))
    user = user_result.scalar_one_or_none()
    package_id = resolve_diagnostic_package_id(
        engagement,
        user_gender=user.gender if user is not None else None,
    )
    pkg = await _get_diagnostic_package(db, package_id)
    if _is_orange_health(pkg):
        raise AppError(
            status_code=422,
            error_code="UNSUPPORTED_PROVIDER",
            message="Cancel and reschedule are not supported for Orange Health bookings yet",
        )
    if not _is_healthians(pkg):
        raise AppError(
            status_code=422,
            error_code="INVALID_DIAGNOSTIC_PROVIDER",
            message="Diagnostic provider is not Healthians",
        )
    return pkg


async def cancel_healthians_participant_booking(
    db: AsyncSession,
    *,
    participant: EngagementParticipant,
    engagement: Engagement,
    remarks: str,
    repository: EngagementsRepository,
) -> dict[str, Any]:
    """Cancel a Healthians booking for an engagement participant and clear local booking fields."""
    booking_id = (await current_booking_id(db, participant)) or ""
    if not booking_id:
        raise AppError(
            status_code=422,
            error_code="NO_BOOKING",
            message="No Healthians booking exists for this participant",
        )

    await _get_healthians_package_for_engagement(db, engagement, user_id=participant.user_id)

    access_token = await _get_healthians_token()
    cancel_payload = {
        "booking_id": booking_id,
        "vendor_billing_user_id": str(participant.booked_by_user_id),
        "vendor_customer_id": str(participant.user_id),
        "remarks": remarks,
    }

    await release_request_transaction(db)

    try:
        resp = await healthians_client.cancel_booking(
            access_token,
            booking_id=booking_id,
            vendor_billing_user_id=str(participant.booked_by_user_id),
            vendor_customer_id=str(participant.user_id),
            remarks=remarks,
        )
    except Exception as exc:
        logger.exception(
            "Healthians cancelBooking failed for participant %s",
            participant.engagement_participant_id,
        )
        await log_healthians_call(
            db,
            engagement_id=engagement.engagement_id,
            user_id=participant.user_id,
            provider="healthians",
            api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/cancelBooking",
            request_payload=cancel_payload,
            status="failed",
            error_message=str(exc),
        )
        raise AppError(
            status_code=502,
            error_code="HEALTHIANS_CANCEL_FAILED",
            message=str(exc),
        ) from exc

    await log_healthians_call(
        db,
        engagement_id=engagement.engagement_id,
        user_id=participant.user_id,
        provider="healthians",
        api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/cancelBooking",
        request_payload=cancel_payload,
        response_payload=resp,
        status="success" if resp.get("status") is True else "failed",
    )

    if resp.get("status") is not True:
        raise AppError(
            status_code=422,
            error_code="HEALTHIANS_CANCEL_FAILED",
            message=resp.get("message", "Booking cancellation failed"),
        )

    await repository.clear_participant_healthians_booking(
        db,
        engagement_participant_id=participant.engagement_participant_id,
    )

    return {
        "status": True,
        "message": resp.get("message", "Order Cancelled Successfully!"),
        "booking_id": booking_id,
    }


def _healthians_reschedule_success(resp: dict) -> bool:
    if resp.get("status") is not True:
        return False
    res_code = resp.get("resCode")
    if res_code is not None and res_code != "RES0001":
        return False
    return True


async def reschedule_healthians_participant_booking(
    db: AsyncSession,
    *,
    participant: EngagementParticipant,
    engagement: Engagement,
    blood_collection_date: date,
    blood_collection_time_slot_id: str,
    blood_collection_time_slot: str,
    reschedule_reason: str,
) -> dict[str, Any]:
    """Reschedule a Healthians home-collection booking and update local participant fields."""
    blood_repo = BloodBookingsRepository()
    current = await blood_repo.get_current_collection(
        db, engagement_participant_id=participant.engagement_participant_id
    )
    if current is None:
        raise AppError(
            status_code=422,
            error_code="NO_BOOKING",
            message="No Healthians booking exists for this participant",
        )
    booking_id = (current.booking_id or "").strip()
    if not booking_id:
        raise AppError(
            status_code=422,
            error_code="NO_BOOKING",
            message="No Healthians booking exists for this participant",
        )

    slot_id = (blood_collection_time_slot_id or "").strip()
    if not slot_id:
        raise AppError(
            status_code=422,
            error_code="SLOT_NOT_LOCKED",
            message="Blood collection slot has not been locked",
        )

    locked_id = (current.collection_time_slot_id or "").strip()
    if not locked_id:
        raise AppError(
            status_code=422,
            error_code="SLOT_NOT_LOCKED",
            message="Blood collection slot has not been locked",
        )
    if locked_id != slot_id:
        raise AppError(
            status_code=422,
            error_code="SLOT_MISMATCH",
            message="Locked slot does not match the requested slot",
        )

    if engagement.blood_collection_type != BloodCollectionType.home_collection:
        raise AppError(
            status_code=422,
            error_code="NOT_HOME_COLLECTION",
            message="Reschedule is only allowed for home collection engagements",
        )

    await _get_healthians_package_for_engagement(db, engagement, user_id=participant.user_id)

    try:
        slot_start_time = _parse_slot_time(blood_collection_time_slot)
    except ValueError as exc:
        raise AppError(status_code=422, error_code="INVALID_SLOT_TIME", message=str(exc)) from exc

    access_token = await _get_healthians_token()
    reschedule_payload = {
        "booking_id": booking_id,
        "slot": {"slot_id": slot_id},
        "customers": [{"vendor_customer_id": str(participant.user_id)}],
        "reschedule_reason": reschedule_reason.strip(),
    }
    api_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/rescheduleBookingByCustomer_v1"

    await release_request_transaction(db)

    try:
        resp = await healthians_client.reschedule_booking_by_customer_v1(
            access_token,
            reschedule_payload,
        )
    except Exception as exc:
        logger.exception(
            "Healthians rescheduleBookingByCustomer_v1 failed for participant %s",
            participant.engagement_participant_id,
        )
        await log_healthians_call(
            db,
            engagement_id=engagement.engagement_id,
            user_id=participant.user_id,
            provider="healthians",
            api_url=api_url,
            request_payload=reschedule_payload,
            status="failed",
            error_message=str(exc),
        )
        raise AppError(
            status_code=502,
            error_code="HEALTHIANS_RESCHEDULE_FAILED",
            message=str(exc),
        ) from exc

    await log_healthians_call(
        db,
        engagement_id=engagement.engagement_id,
        user_id=participant.user_id,
        provider="healthians",
        api_url=api_url,
        request_payload=reschedule_payload,
        response_payload=resp,
        status="success" if _healthians_reschedule_success(resp) else "failed",
    )

    if not _healthians_reschedule_success(resp):
        raise AppError(
            status_code=422,
            error_code="HEALTHIANS_RESCHEDULE_FAILED",
            message=resp.get("message", "Booking reschedule failed"),
        )

    data = resp.get("data") or {}
    new_booking_id = data.get("new_booking_id")
    new_booking_id_str = str(new_booking_id).strip() if new_booking_id else booking_id
    await blood_repo.record_reschedule(
        db,
        current=current,
        new_booking_id=new_booking_id_str,
        collection_date=blood_collection_date,
        collection_time=slot_start_time,
        collection_time_slot_id=slot_id,
    )

    return {
        "status": True,
        "message": resp.get("message", "Booking Successfully Rescheduled."),
        "booking_id": new_booking_id_str,
        "previous_booking_id": booking_id,
        "blood_collection_date": blood_collection_date.isoformat(),
        "blood_collection_time_slot_id": slot_id,
        "slot_start_time": slot_start_time.isoformat(),
        "resCode": resp.get("resCode"),
    }


async def reschedule_healthians_bookings_batch(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    caller_user_id: int,
) -> list[dict[str, Any]]:
    """Reschedule Healthians bookings for multiple engagement participants."""
    results: list[dict[str, Any]] = []

    for member in members:
        user_id = member["user_id"]
        engagement_id = member["engagement_id"]
        blood_collection_date = member["blood_collection_date"]
        blood_collection_time_slot_id = member["blood_collection_time_slot_id"]
        blood_collection_time_slot = member["blood_collection_time_slot"]
        reschedule_reason = member["reschedule_reason"]

        engagement_result = await db.execute(
            select(Engagement).where(Engagement.engagement_id == engagement_id)
        )
        engagement = engagement_result.scalar_one_or_none()
        if engagement is None:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "Engagement not found",
            })
            continue

        participant_result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.user_id == user_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = participant_result.scalar_one_or_none()
        if participant is None:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "User not enrolled",
            })
            continue

        if caller_user_id not in (participant.user_id, participant.booked_by_user_id):
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "Not authorized to reschedule this booking",
            })
            continue

        try:
            reschedule_result = await reschedule_healthians_participant_booking(
                db,
                participant=participant,
                engagement=engagement,
                blood_collection_date=blood_collection_date,
                blood_collection_time_slot_id=blood_collection_time_slot_id,
                blood_collection_time_slot=blood_collection_time_slot,
                reschedule_reason=reschedule_reason,
            )
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "success",
                "message": reschedule_result.get("message"),
                "booking_id": reschedule_result.get("booking_id"),
                "blood_collection_date": reschedule_result.get("blood_collection_date"),
                "blood_collection_time_slot_id": reschedule_result.get("blood_collection_time_slot_id"),
                "slot_start_time": reschedule_result.get("slot_start_time"),
            })
        except AppError as exc:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": exc.message,
            })

    return results


async def cancel_healthians_bookings_batch(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    caller_user_id: int,
    repository: EngagementsRepository,
) -> list[dict[str, Any]]:
    """Cancel Healthians bookings for multiple engagement participants."""
    results: list[dict[str, Any]] = []

    for member in members:
        user_id = member["user_id"]
        engagement_id = member["engagement_id"]
        remarks = member["remarks"]

        engagement_result = await db.execute(
            select(Engagement).where(Engagement.engagement_id == engagement_id)
        )
        engagement = engagement_result.scalar_one_or_none()
        if engagement is None:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "Engagement not found",
            })
            continue

        participant_result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.user_id == user_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = participant_result.scalar_one_or_none()
        if participant is None:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "User not enrolled",
            })
            continue

        if caller_user_id not in (participant.user_id, participant.booked_by_user_id):
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": "Not authorized to cancel this booking",
            })
            continue

        try:
            cancel_result = await cancel_healthians_participant_booking(
                db,
                participant=participant,
                engagement=engagement,
                remarks=remarks,
                repository=repository,
            )
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "success",
                "message": cancel_result.get("message", "Order Cancelled Successfully!"),
                "booking_id": cancel_result.get("booking_id"),
            })
        except AppError as exc:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "error",
                "message": exc.message,
            })

    return results


async def get_user_draft_engagements(
    db: AsyncSession,
    *,
    user_id: int,
) -> list[dict[str, Any]]:
    """Return draft engagements where user is participant or booker."""
    # Distinct IDs only: full-row DISTINCT fails on PG json (consultations).
    draft_ids = (
        select(Engagement.engagement_id)
        .join(EngagementParticipant, EngagementParticipant.engagement_id == Engagement.engagement_id)
        .where(
            or_(
                EngagementParticipant.user_id == user_id,
                EngagementParticipant.booked_by_user_id == user_id,
            )
        )
        .where(Engagement.status == "draft")
        .distinct()
    )
    query = select(Engagement).where(Engagement.engagement_id.in_(draft_ids))
    result = await db.execute(query)
    engagements = result.scalars().all()

    output: list[dict[str, Any]] = []
    for engagement in engagements:
        address = (engagement.address or "").strip() or None
        resume_step = "booking_date" if address else "address"
        output.append({
            "engagement_id": engagement.engagement_id,
            "status": engagement.status,
            "resume_step": resume_step,
            "address": address,
        })
    return output


def _consultation_availability_service() -> ExpertAvailabilityService:
    return ExpertAvailabilityService(
        experts_repository=ExpertsRepository(),
        availability_repository=ExpertAvailabilityRepository(),
        override_repository=ExpertAvailabilityOverrideRepository(),
        consultation_bookings_repository=ConsultationBookingsRepository(),
    )


async def _find_reusable_consultation_draft(
    db: AsyncSession,
    *,
    user_id: int,
    expert_type: str,
    expert_id: int,
    consultation_date: date,
    slot_hhmm: str,
    consultation_type_id: int | None,
) -> tuple[Engagement, EngagementParticipant] | None:
    if consultation_type_id is None:
        return None
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=_CONSULTATION_PAY_REUSE_MINUTES)
    result = await db.execute(
        select(Engagement, EngagementParticipant)
        .join(
            EngagementParticipant,
            EngagementParticipant.engagement_id == Engagement.engagement_id,
        )
        .join(
            ConsultationBooking,
            ConsultationBooking.engagement_participant_id
            == EngagementParticipant.engagement_participant_id,
        )
        .where(EngagementParticipant.user_id == user_id)
        .where(Engagement.status == "draft")
        .where(Engagement.engagement_type == consultation_type_id)
        .where(ConsultationBooking.expert_type == expert_type)
        .where(ConsultationBooking.expert_id == expert_id)
        .where(ConsultationBooking.consultation_date == consultation_date)
        .where(ConsultationBooking.consultation_slot == slot_hhmm)
        .where(ConsultationBooking.want.is_(True))
        .where(Engagement.created_at >= cutoff)
        .order_by(Engagement.engagement_id.desc())
        .limit(1)
    )
    row = result.first()
    if row is None:
        return None
    return row[0], row[1]


async def _create_consultation_slot_hold(
    db: AsyncSession,
    *,
    expert_id: int,
    consultation_date: date,
    slot_hhmm: str,
) -> ExpertAvailabilityOverrideModel:
    override = ExpertAvailabilityOverrideModel(
        expert_id=expert_id,
        override_date=consultation_date,
        status="booked",
        start_time=parse_slot_time(slot_hhmm),
        end_time=None,
        buffer_time=None,
    )
    repo = ExpertAvailabilityOverrideRepository()
    return await repo.create(db, override)


async def _release_consultation_slot_hold(
    db: AsyncSession,
    *,
    expert_id: int,
    consultation_date: date,
    slot_hhmm: str,
) -> None:
    slot_time = parse_slot_time(slot_hhmm)
    await db.execute(
        delete(ExpertAvailabilityOverrideModel).where(
            ExpertAvailabilityOverrideModel.expert_id == expert_id,
            ExpertAvailabilityOverrideModel.override_date == consultation_date,
            ExpertAvailabilityOverrideModel.status == "booked",
            ExpertAvailabilityOverrideModel.start_time == slot_time,
        )
    )


async def _assert_caller_may_book_consultation_for(
    db: AsyncSession,
    *,
    caller_user_id: int,
    member_user_id: int,
) -> User:
    """Primary account may book for self or sub-profiles (parent_id == caller)."""
    if member_user_id == caller_user_id:
        user = await db.get(User, caller_user_id)
        if user is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="User not found")
        return user

    target = await db.get(User, member_user_id)
    if target is None or target.parent_id != caller_user_id:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to book for this user",
        )
    return target


async def _validate_consultation_expert_for_pay(
    db: AsyncSession,
    *,
    expert_type: str,
    expert_id: int,
) -> Expert:
    expert_type = expert_type.strip()
    experts_repo = ExpertsRepository()
    expert = await experts_repo.get_by_id(db, expert_id)
    if expert is None:
        raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert not found")
    if (expert.status or "").strip().lower() != "active":
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Expert is not active")
    if expert.expert_type != expert_type:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="expert_id does not match expert_type",
        )
    fee = expert.appointment_fee_paise
    if fee is None or int(fee) <= 0:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Expert has no appointment fee configured",
        )
    return expert


async def _prepare_consultation_draft_for_member(
    db: AsyncSession,
    *,
    caller_user_id: int,
    member_user_id: int,
    expert_type: str,
    expert_id: int,
    consultation_date: date,
    slot_hhmm: str,
    consultation_type_id: int | None,
    availability: ExpertAvailabilityService,
    expert: Expert,
    member_user: User,
) -> Engagement:
    reusable = await _find_reusable_consultation_draft(
        db,
        user_id=member_user_id,
        expert_type=expert_type,
        expert_id=expert_id,
        consultation_date=consultation_date,
        slot_hhmm=slot_hhmm,
        consultation_type_id=consultation_type_id,
    )
    if reusable is not None:
        return reusable[0]

    available = await availability._slot_is_available(
        db,
        expert_type=expert_type,
        day=consultation_date,
        slot_hhmm=slot_hhmm,
        expert_id=expert_id,
    )
    if not available:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Selected slot is not available",
        )

    city = (member_user.city or "Online").strip() or "Online"
    duration = expert.session_duration_mins or 30

    engagement = Engagement(
        engagement_name=f"{(member_user.first_name or 'user').strip()}-draft",
        organization_id=None,
        camp_no=None,
        engagement_code=_generate_engagement_code(),
        engagement_type=consultation_type_id,
        consultations={expert_type: True},
        consultation_mode=ConsultationMode.online,
        diagnostic_package_id=None,
        city=city,
        slot_duration=duration,
        status="draft",
        create_profile_on_metsights=False,
        enroll_for_fitprint_full=False,
    )
    db.add(engagement)
    await db.flush()

    participant = EngagementParticipant(
        engagement_id=engagement.engagement_id,
        user_id=member_user_id,
        booked_by_user_id=caller_user_id,
    )
    db.add(participant)
    await db.flush()

    cb_repo = ConsultationBookingsRepository()
    await cb_repo.create_or_update_for_type(
        db,
        participant,
        expert_type,
        want=True,
        consultation_date=consultation_date,
        consultation_slot=slot_hhmm,
        expert_id=expert_id,
    )
    await _create_consultation_slot_hold(
        db,
        expert_id=expert_id,
        consultation_date=consultation_date,
        slot_hhmm=slot_hhmm,
    )
    return engagement


async def create_consultation_pay_order(
    db: AsyncSession,
    *,
    caller_user_id: int,
    members: list[dict[str, Any]],
    discount_code: str | None = None,
    client_ip: str | None = None,
) -> dict[str, Any]:
    """Validate slots, create draft consultation engagements per member, and open one Razorpay checkout."""
    if not members:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="No members provided")

    caller = await db.get(User, caller_user_id)
    if caller is None:
        raise AppError(status_code=404, error_code="NOT_FOUND", message="User not found")
    if caller.parent_id is not None:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="Consultation checkout is only available for primary accounts",
        )

    today_ist = datetime.now(_IST).date()
    consultation_type_id = await _resolve_engagement_type_id(db, "consultation")
    availability = _consultation_availability_service()

    validated_items: list[tuple[int, str, int]] = []
    metadata_by_user: dict[int, dict[str, Any]] = {}
    member_statuses: list[dict[str, Any]] = []
    discount_context: dict[str, Any] | None = None
    slot_keys_seen: set[tuple[int, date, str]] = set()

    for member in members:
        member_user_id = int(member["user_id"])
        expert_type = str(member["expert_type"]).strip()
        expert_id = int(member["expert_id"])
        consultation_date = member["date"]
        if not isinstance(consultation_date, date):
            consultation_date = date.fromisoformat(str(consultation_date))
        slot_hhmm = normalize_hhmm(str(member["slot"]))

        if consultation_date < today_ist:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Consultation date must be today or later",
            )

        slot_key = (expert_id, consultation_date, slot_hhmm)
        if slot_key in slot_keys_seen:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Duplicate expert slot in the same checkout",
            )
        slot_keys_seen.add(slot_key)

        member_user = await _assert_caller_may_book_consultation_for(
            db,
            caller_user_id=caller_user_id,
            member_user_id=member_user_id,
        )
        expert = await _validate_consultation_expert_for_pay(
            db,
            expert_type=expert_type,
            expert_id=expert_id,
        )
        engagement = await _prepare_consultation_draft_for_member(
            db,
            caller_user_id=caller_user_id,
            member_user_id=member_user_id,
            expert_type=expert_type,
            expert_id=expert_id,
            consultation_date=consultation_date,
            slot_hhmm=slot_hhmm,
            consultation_type_id=consultation_type_id,
            availability=availability,
            expert=expert,
            member_user=member_user,
        )
        engagement_id = int(engagement.engagement_id)

        validated_items.append((member_user_id, "expert", expert_id))
        metadata_by_user[member_user_id] = {
            "engagement_id": engagement_id,
            "expert_type": expert_type,
            "expert_id": expert_id,
            "date": consultation_date.isoformat(),
            "slot": slot_hhmm,
        }
        member_statuses.append({
            "user_id": member_user_id,
            "engagement_id": engagement_id,
            "status": "success",
        })
        if discount_context is None:
            discount_context = {
                "organization_id": engagement.organization_id,
                "camp_no": str(engagement.camp_no) if engagement.camp_no is not None else None,
                "engagement_id": engagement_id,
                "city": getattr(engagement, "city", None),
            }

    payments_service = PaymentsService()
    result = await payments_service.create_order(
        db,
        payer_user_id=caller_user_id,
        items=validated_items,
        authenticated_user_id=caller_user_id,
        booking_type="consultation",
        metadata_by_user=metadata_by_user,
        discount_code=discount_code.strip() if discount_code else None,
        discount_context=discount_context,
        client_ip=client_ip,
    )
    err = result.get("_error")
    if err:
        code, msg = err
        raise AppError(status_code=code, error_code="PAYMENT_ERROR", message=msg)

    out = {
        "razorpay_order_id": result["razorpay_order_id"],
        "amount_paise": result["amount_paise"],
        "amount_rupees": result["amount_rupees"],
        "currency": result["currency"],
        "key_id": result["key_id"],
        "booking_ids": result["booking_ids"],
        "booking_id": result["booking_id"],
        "members": member_statuses,
    }
    if result.get("discount"):
        out["discount"] = result["discount"]
    return out


async def _finalize_one_consultation_engagement(
    db: AsyncSession,
    *,
    engagement_id: int,
    user_id: int,
    engagements_service: EngagementsService | None,
) -> dict[str, Any]:
    consultation_type_id = await _resolve_engagement_type_id(db, "consultation")
    engagement = await db.get(Engagement, engagement_id)
    if engagement is None:
        return {
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "error",
            "message": "Engagement not found",
        }

    if (engagement.status or "").lower() == "scheduled":
        return {
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "success",
            "message": "Already scheduled",
        }

    if (engagement.status or "").lower() != "draft":
        return {
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "error",
            "message": "Engagement is not in draft status",
        }
    if consultation_type_id is not None and engagement.engagement_type != consultation_type_id:
        return {
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "error",
            "message": "Engagement is not a consultation booking",
        }

    participant_result = await db.execute(
        select(EngagementParticipant)
        .where(EngagementParticipant.engagement_id == engagement_id)
        .where(EngagementParticipant.user_id == user_id)
        .order_by(EngagementParticipant.engagement_participant_id.desc())
        .limit(1)
    )
    participant = participant_result.scalar_one_or_none()
    if participant is None:
        return {
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "error",
            "message": "Participant not found",
        }

    cb_result = await db.execute(
        select(ConsultationBooking)
        .where(ConsultationBooking.engagement_participant_id == participant.engagement_participant_id)
        .where(ConsultationBooking.want.is_(True))
        .limit(1)
    )
    consultation_row = cb_result.scalar_one_or_none()
    if consultation_row is None or consultation_row.consultation_date is None:
        return {
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "error",
            "message": "Consultation slot is not set on engagement",
        }

    consultation_date = consultation_row.consultation_date
    slot_hhmm = consultation_row.consultation_slot or ""
    expert_type = consultation_row.expert_type

    engagement.status = "scheduled"
    engagement.start_date = consultation_date
    engagement.end_date = consultation_date
    user = await db.get(User, user_id)
    if user is not None:
        name_part = (user.first_name or "user").strip() or "user"
        current_name = (engagement.engagement_name or "").strip()
        if current_name.endswith("-draft"):
            engagement.engagement_name = f"{name_part}-{consultation_date.isoformat()}"

    db.add(engagement)
    await db.flush()

    if engagements_service is not None and user is not None:
        slot_time = None
        if slot_hhmm:
            try:
                slot_time = parse_slot_time(slot_hhmm)
            except (ValueError, IndexError):
                slot_time = None
        await engagements_service.apply_b2c_defaults_and_notify_after_booking(
            db,
            engagement=engagement,
            user=user,
            collection_date=consultation_date,
            collection_time=slot_time,
        )
        from modules.engagements.repository import EngagementsRepository
        from modules.notifications.consultation_booking_alert_notify import (
            notify_onboarding_assistants_on_consultation_booking,
        )
        from modules.notifications.repository import NotificationsRepository
        from modules.notifications.service import NotificationsService

        await notify_onboarding_assistants_on_consultation_booking(
            db,
            notifications_service=NotificationsService(repository=NotificationsRepository()),
            notifications_repository=NotificationsRepository(),
            engagements_repository=EngagementsRepository(),
            engagement=engagement,
            participant_user=user,
            participant_user_id=user_id,
            expert_type=expert_type,
            expert_id=consultation_row.expert_id,
            consultation_date=consultation_date,
            consultation_slot=slot_hhmm,
            consultation_cabin=consultation_row.consultation_cabin,
        )
        engagement.end_date = consultation_date
        db.add(engagement)
        await db.flush()

    return {
        "user_id": user_id,
        "engagement_id": engagement_id,
        "status": "success",
        "message": "Consultation scheduled",
        "booking_id": None,
    }


async def verify_and_finalize_consultation_booking(
    db: AsyncSession,
    *,
    razorpay_payment_id: str,
    razorpay_order_id: str,
    razorpay_signature: str,
    caller_user_id: int,
    engagements_service: EngagementsService | None = None,
) -> dict[str, Any]:
    """Verify Razorpay payment and schedule paid consultation draft engagements.

    One Razorpay order may include multiple consultation booking lines; each line
    is finalized via ``members`` (source of truth). Top-level ``engagement_id`` is
    the first booking's engagement when present for backward compatibility.
    """
    payments_service = PaymentsService()
    verify_result = await payments_service.verify_payment(
        db,
        razorpay_payment_id=razorpay_payment_id,
        razorpay_order_id=razorpay_order_id,
        razorpay_signature=razorpay_signature,
        authenticated_user_id=caller_user_id,
        skip_fulfillment=True,
    )

    if verify_result.get("_signature_invalid"):
        raise AppError(status_code=400, error_code="INVALID_SIGNATURE", message="Invalid payment signature")
    err = verify_result.get("_error")
    if err:
        code, msg = err
        raise AppError(status_code=code, error_code="PAYMENT_VERIFY_FAILED", message=msg)

    bookings = verify_result.get("bookings") or []
    if not bookings:
        booking_ids = verify_result.get("booking_ids") or []
        if booking_ids:
            bookings_result = await db.execute(
                select(Booking).where(Booking.booking_id.in_(booking_ids))
            )
            bookings = list(bookings_result.scalars().all())

    member_results: list[dict[str, Any]] = []
    for booking in bookings:
        meta = booking.metadata_ or {}
        engagement_id = meta.get("engagement_id")
        if engagement_id is None:
            member_results.append({
                "user_id": int(booking.user_id),
                "status": "error",
                "message": "Missing engagement_id in booking metadata",
            })
            continue
        member_results.append(
            await _finalize_one_consultation_engagement(
                db,
                engagement_id=int(engagement_id),
                user_id=int(booking.user_id),
                engagements_service=engagements_service,
            )
        )

    primary_engagement_id = None
    if bookings:
        meta0 = bookings[0].metadata_ or {}
        if meta0.get("engagement_id") is not None:
            primary_engagement_id = int(meta0["engagement_id"])

    return {
        "payment_verified": True,
        "razorpay_order_id": razorpay_order_id,
        "razorpay_payment_id": verify_result.get("payment_id") or razorpay_payment_id,
        "booking_ids": verify_result.get("booking_ids") or [],
        "engagement_id": primary_engagement_id,
        "status": "scheduled",
        "members": member_results,
    }


async def release_consultation_checkout_on_payment_failure(
    db: AsyncSession,
    booking_ids: list[int],
) -> None:
    """Cancel draft consultation engagements and release expert slot holds after failed checkout."""
    if not booking_ids:
        return
    bookings_result = await db.execute(
        select(Booking).where(Booking.booking_id.in_(booking_ids))
    )
    bookings = list(bookings_result.scalars().all())
    for booking in bookings:
        if (booking.booking_type or "") != "consultation" and (booking.entity_type or "") != "expert":
            continue
        meta = booking.metadata_ or {}
        engagement_id = meta.get("engagement_id")
        expert_id = meta.get("expert_id")
        date_str = meta.get("date")
        slot_val = meta.get("slot")
        if expert_id is not None and date_str and slot_val:
            try:
                hold_date = date.fromisoformat(str(date_str))
                await _release_consultation_slot_hold(
                    db,
                    expert_id=int(expert_id),
                    consultation_date=hold_date,
                    slot_hhmm=str(slot_val),
                )
            except (ValueError, TypeError):
                logger.warning(
                    "release_consultation_checkout: invalid hold metadata booking_id=%s",
                    booking.booking_id,
                )
        if engagement_id is None:
            continue
        engagement = await db.get(Engagement, int(engagement_id))
        if engagement is not None and (engagement.status or "").lower() == "draft":
            engagement.status = "cancelled"
            db.add(engagement)
    await db.flush()
