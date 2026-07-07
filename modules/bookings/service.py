"""Booking flow service — check serviceability, get slots, lock, and create bookings via Healthians."""

from __future__ import annotations

import logging
from datetime import date, time
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.healthians.sync_log import log_healthians_call
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import BloodCollectionType, Engagement, EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService, _generate_engagement_code
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


async def check_service_availability(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
    engagements_service: EngagementsService,
) -> list[dict[str, Any]]:
    """Check Healthians serviceability and create draft engagements for serviceable members."""
    results: list[dict[str, Any]] = []
    access_token = await _get_healthians_token()

    for member in members:
        user_id = member["user_id"]
        diagnostic_package_id = member["diagnostic_package_id"]

        pkg = await _get_diagnostic_package(db, diagnostic_package_id)
        if not _is_healthians(pkg):
            results.append({"user_id": user_id, "status": "error", "message": "Diagnostic provider is not Healthians"})
            continue

        lat = str(member["latitude"])
        lng = str(member["longitude"])
        zipcode = member.get("pincode") or ""

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
                engagement_id=None,
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
            engagement_id=None,
            user_id=user_id,
            provider="healthians",
            api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2",
            request_payload={"lat": lat, "long": lng, "zipcode": zipcode, "is_ppmc_booking": 0},
            response_payload=resp,
            status="success" if resp.get("status") else "failed",
        )

        if not resp.get("status"):
            results.append({
                "user_id": user_id,
                "status": "not_serviceable",
                "message": resp.get("message", "This location is not serviceable."),
            })
            continue

        zone_id = resp.get("data", {}).get("zone_id") if resp.get("data") else None

        user_result = await db.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        if user is None:
            results.append({"user_id": user_id, "status": "error", "message": "User not found"})
            continue

        engagement = Engagement(
            engagement_name=f"{(user.first_name or 'user').strip()}-draft",
            metsights_engagement_id=None,
            organization_id=None,
            camp_no=None,
            engagement_code=_generate_engagement_code(),
            engagement_type=None,
            assessment_package_id=None,
            diagnostic_package_id=diagnostic_package_id,
            city=member.get("city"),
            address=member.get("address"),
            sub_locality=member.get("sub_locality"),
            landmark=member.get("landmark"),
            pincode=zipcode or None,
            state=member.get("state"),
            country=member.get("country"),
            latitude=member["latitude"],
            longitude=member["longitude"],
            slot_duration=20,
            start_date=None,
            end_date=None,
            status="draft",
            healthians_zone_id=str(zone_id) if zone_id else None,
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
            engagement_date=date.today(),
            slot_start_time=time(0, 0),
        )
        db.add(participant)
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

        results.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "success",
            "slots": resp.get("data", []),
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

        try:
            resp = await healthians_client.freeze_slot_v1(
                access_token,
                slot_id=slot_id,
                vendor_billing_user_id=str(user_id),
            )
        except Exception as exc:
            logger.exception("Healthians freezeSlot_v1 failed for user %s", user_id)
            await log_healthians_call(
                db,
                engagement_id=engagement_id,
                user_id=user_id,
                provider="healthians",
                api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/freezeSlot_v1",
                request_payload={"slot_id": slot_id, "vendor_billing_user_id": str(user_id)},
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
            request_payload={"slot_id": slot_id, "vendor_billing_user_id": str(user_id)},
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

        participant.blood_collection_time_slot_id = slot_id
        participant.engagement_date = blood_collection_date
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


async def create_healthians_booking_after_payment(
    db: AsyncSession,
    *,
    members: list[dict[str, Any]],
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
            results.append({"user_id": user_id, "engagement_id": engagement_id, "status": "error", "message": "User not enrolled"})
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

        booking_payload = {
            "customer": [{
                "customer_id": str(user_id),
                "customer_name": full_name.upper(),
                "relation": "self",
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
            "vendor_billing_user_id": str(user_id),
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
        engagement.status = "scheduled"
        await db.flush()

        results.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "status": "success",
            "message": resp.get("message", "Booking placed"),
            "booking_id": booking_id,
        })

    return results


async def get_user_draft_engagement_ids(
    db: AsyncSession,
    *,
    user_id: int,
) -> list[int]:
    """Return engagement IDs where user is a participant and engagement is in draft status."""
    query = (
        select(Engagement.engagement_id)
        .join(EngagementParticipant, EngagementParticipant.engagement_id == Engagement.engagement_id)
        .where(EngagementParticipant.user_id == user_id)
        .where(Engagement.status == "draft")
        .distinct()
    )
    result = await db.execute(query)
    return [row[0] for row in result.all()]
