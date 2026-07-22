"""Booking flow service — check serviceability, get slots, lock, and create bookings via Healthians."""

from __future__ import annotations

import logging
import re
from datetime import date, time
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.healthians.sync_log import log_healthians_call
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import BloodCollectionType, Engagement, EngagementKind, EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService, _generate_engagement_code
from modules.geocoding.client import search_places
from modules.payments.models import Booking
from modules.payments.services import PaymentsService
from modules.users.models import User

logger = logging.getLogger(__name__)


def _is_healthians(pkg: DiagnosticPackage) -> bool:
    return (pkg.diagnostic_provider or "").strip().lower() == "healthians"


async def _get_diagnostic_package(db: AsyncSession, package_id: int) -> DiagnosticPackage:
    result = await db.execute(
        select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == package_id)
    )
    pkg = result.scalar_one_or_none()
    if pkg is None:
        raise AppError(status_code=404, error_code="PACKAGE_NOT_FOUND", message=f"Diagnostic package {package_id} not found")
    return pkg


async def _get_healthians_token() -> str:
    return await healthians_client.get_access_token()


def _build_booking_address(
    house_flat_no: str,
    building_area: str,
    landmark: str | None,
    city: str,
    pincode: str,
) -> str:
    parts: list[str] = []
    for value in (house_flat_no, building_area, landmark):
        text = (value or "").strip()
        if text:
            parts.append(text)
    city_pin = f"{city.strip()} - {pincode.strip()}"
    parts.append(city_pin)
    return ", ".join(parts)


def _build_geocode_query(
    building_area: str,
    landmark: str | None,
    city: str,
    pincode: str,
) -> str:
    parts: list[str] = []
    for value in (building_area, landmark):
        text = (value or "").strip()
        if text:
            parts.append(text)
    parts.append(f"{city.strip()} - {pincode.strip()}")
    return ", ".join(parts)


async def _geocode_for_booking(query: str) -> dict[str, Any]:
    results = await search_places(query, limit=1)
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


async def check_service_availability(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    engagements_service: EngagementsService,
    booked_by_user_id: int,
) -> list[dict[str, Any]]:
    """Check Healthians serviceability and create draft engagements for serviceable members."""
    results: list[dict[str, Any]] = []
    access_token = await _get_healthians_token()

    for member in members:
        user_id = member["user_id"]
        diagnostic_package_id = member["diagnostic_package_id"]
        house_flat_no = member["house_flat_no"]
        building_area = member["building_area"]
        landmark = member.get("landmark")
        city = member["city"]
        pincode = member["pincode"]

        pkg = await _get_diagnostic_package(db, diagnostic_package_id)
        if not _is_healthians(pkg):
            results.append({"user_id": user_id, "status": "error", "message": "Diagnostic provider is not Healthians"})
            continue

        geocode_query = _build_geocode_query(building_area, landmark, city, pincode)
        geocoded = await _geocode_for_booking(geocode_query)
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

        address = _build_booking_address(house_flat_no, building_area, landmark, city, pincode)

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
            sub_locality=building_area,
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
            onboarding_notification="booking-alert-whatsapp",
        )
        db.add(engagement)
        await db.flush()

        participant = EngagementParticipant(
            engagement_id=engagement.engagement_id,
            user_id=user_id,
            booked_by_user_id=booked_by_user_id,
            engagement_date=None,
            slot_start_time=None,
        )
        db.add(participant)
        await db.flush()

        lat = str(latitude)
        lng = str(longitude)
        zipcode = pincode

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
            await log_healthians_call(
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
            continue

        await log_healthians_call(
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
            results.append({
                "user_id": user_id,
                "engagement_id": engagement.engagement_id,
                "status": "not_serviceable",
                "message": resp.get("message", "This location is not serviceable."),
            })
            continue

        zone_id = resp.get("data", {}).get("zone_id") if resp.get("data") else None
        engagement.healthians_zone_id = str(zone_id) if zone_id else None
        await db.flush()

        results.append({
            "user_id": user_id,
            "engagement_id": engagement.engagement_id,
            "status": "serviceable",
            "message": resp.get("message", "Serviceable"),
            "zone_id": zone_id,
        })

    return results


async def get_available_slots(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Fetch available slots from Healthians for drafted engagements."""
    results: list[dict[str, Any]] = []
    access_token = await _get_healthians_token()

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

        if (engagement.status or "").lower() != "draft":
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Engagement is not in draft status"})
            continue

        participant_result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.user_id == user_id)
            .limit(1)
        )
        if participant_result.scalar_one_or_none() is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "User is not a participant"})
            continue

        if not engagement.diagnostic_package_id:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "No diagnostic package"})
            continue

        pkg = await _get_diagnostic_package(db, engagement.diagnostic_package_id)
        if not _is_healthians(pkg):
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Not a Healthians package"})
            continue

        user_result = await db.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        has_female = 1 if user and (user.gender or "").strip().lower().startswith("f") else 0

        amount = float(pkg.original_price) if pkg.original_price else 0
        external_package_id = pkg.external_package_id or 0

        payload = {
            "slot_date": blood_collection_date.isoformat(),
            "zone_id": str(engagement.healthians_zone_id or ""),
            "lat": str(engagement.latitude or ""),
            "long": str(engagement.longitude or ""),
            "zipcode": engagement.pincode or "",
            "get_ppmc_slots": 0,
            "has_female_patient": has_female,
            "amount": amount,
            "package": [{"deal_id": [f"package_{external_package_id}"]}],
        }

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
    """Freeze slots on Healthians and update participant records."""
    results: list[dict[str, Any]] = []
    access_token = await _get_healthians_token()

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

        if (engagement.status or "").lower() != "draft":
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Engagement is not in draft status"})
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
        if not _is_healthians(pkg):
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Not a Healthians package"})
            continue

        vendor_billing_user_id = str(participant.booked_by_user_id)

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

        participant.blood_collection_time_slot_id = slot_id
        participant.engagement_date = blood_collection_date
        participant.slot_start_time = slot_start_time
        await db.flush()

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
    if not participant.blood_collection_time_slot_id:
        raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection slot is not locked")
    if participant.engagement_date is None:
        raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection date is not set")
    if participant.slot_start_time is None:
        raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection time is not set")

    pkg = await _get_diagnostic_package(db, engagement.diagnostic_package_id)
    if not _is_healthians(pkg):
        raise AppError(status_code=422, error_code="INVALID_PROVIDER", message="Not a Healthians package")
    if (pkg.status or "").strip().lower() != "active":
        raise AppError(status_code=422, error_code="PACKAGE_INACTIVE", message="Diagnostic package is not active")

    return engagement, participant, pkg


async def create_pay_order_for_draft_engagements(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    payer_user_id: int,
) -> dict[str, Any]:
    """Create a Razorpay order for locked draft engagements."""
    validated_items: list[tuple[int, str, int]] = []
    metadata_by_user: dict[int, dict[str, Any]] = {}
    member_statuses: list[dict[str, Any]] = []

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

    payments_service = PaymentsService()
    result = await payments_service.create_order(
        db,
        payer_user_id=payer_user_id,
        items=validated_items,
        authenticated_user_id=payer_user_id,
        booking_type=None,
        metadata_by_user=metadata_by_user,
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
        "members": member_statuses,
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
    engagement_type: EngagementKind | None = None,
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
        engagement_type=engagement_type,
        engagements_service=engagements_service,
    )

    return {
        "payment_verified": True,
        "razorpay_order_id": razorpay_order_id,
        "razorpay_payment_id": verify_result.get("payment_id") or razorpay_payment_id,
        "booking_ids": verify_result.get("booking_ids") or [],
        "members": member_results,
    }


async def create_healthians_booking_after_payment(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    caller_user_id: int,
    engagement_type: EngagementKind | None = None,
    engagements_service: EngagementsService | None = None,
) -> list[dict[str, Any]]:
    """After payment succeeds, create Healthians booking for each member using their drafted engagement."""
    results: list[dict[str, Any]] = []
    access_token = await _get_healthians_token()

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
        if status_lower == "scheduled" and participant.booking_id:
            results.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "status": "success",
                "message": "Booking already placed",
                "booking_id": participant.booking_id,
            })
            continue

        if status_lower != "draft":
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Engagement is not in draft status"})
            continue

        if not engagement.diagnostic_package_id:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "No diagnostic package"})
            continue

        pkg = await _get_diagnostic_package(db, engagement.diagnostic_package_id)
        if not _is_healthians(pkg):
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "Not a Healthians package"})
            continue

        user_result = await db.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        if user is None:
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "User not found"})
            continue

        full_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip() or "User"
        gender_code = "M"
        if (user.gender or "").strip().lower().startswith("f"):
            gender_code = "F"

        age = user.age or 30
        dob = ""
        if user.date_of_birth:
            dob = user.date_of_birth.strftime("%d/%m/%Y")

        external_package_id = pkg.external_package_id or 0
        slot_id = participant.blood_collection_time_slot_id or ""
        relation = (user.relationship or "self").strip() or "self"
        vendor_billing_user_id = str(participant.booked_by_user_id)

        booking_payload = {
            "customer": [{
                "customer_id": str(user_id),
                "customer_name": full_name.upper(),
                "relation": relation,
                "age": age,
                "dob": dob,
                "gender": gender_code,
            }],
            "slot": {"slot_id": slot_id},
            "package": [{"deal_id": [f"package_{external_package_id}"]}],
            "customer_calling_number": user.phone or "",
            "billing_cust_name": full_name.upper(),
            "gender": gender_code,
            "mobile": user.phone or "",
            "email": user.email or "",
            "sub_locality": engagement.sub_locality or "",
            "latitude": str(engagement.latitude or ""),
            "longitude": str(engagement.longitude or ""),
            "address": engagement.address or "",
            "zipcode": engagement.pincode or "",
            "landmark": engagement.landmark or "",
            "hard_copy": 0,
            "vendor_billing_user_id": vendor_billing_user_id,
            "payment_option": "prepaid",
            "discounted_price": 0,
            "zone_id": int(engagement.healthians_zone_id) if engagement.healthians_zone_id else 0,
            "is_ppmc_booking": 0,
        }

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
        participant.booking_id = str(booking_id)
        participant.barcode = str(booking_id)
        if engagement_type is not None and engagement.engagement_type is None:
            engagement.engagement_type = engagement_type
        engagement.status = "scheduled"

        if engagements_service is not None:
            await engagements_service.apply_b2c_defaults_and_notify_after_booking(
                db,
                engagement=engagement,
                user=user,
                collection_date=participant.engagement_date,
                collection_time=participant.slot_start_time,
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
) -> DiagnosticPackage:
    if not engagement.diagnostic_package_id:
        raise AppError(status_code=422, error_code="INVALID_STATE", message="No diagnostic package")
    pkg = await _get_diagnostic_package(db, engagement.diagnostic_package_id)
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
    booking_id = (participant.booking_id or "").strip()
    if not booking_id:
        raise AppError(
            status_code=422,
            error_code="NO_BOOKING",
            message="No Healthians booking exists for this participant",
        )

    await _get_healthians_package_for_engagement(db, engagement)

    access_token = await _get_healthians_token()
    cancel_payload = {
        "booking_id": booking_id,
        "vendor_billing_user_id": str(participant.booked_by_user_id),
        "vendor_customer_id": str(participant.user_id),
        "remarks": remarks,
    }

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
