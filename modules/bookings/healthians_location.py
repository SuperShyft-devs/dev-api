"""Healthians home-collection location: geocode, zone resolve, persist on participant."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.transaction import release_request_transaction
from modules.diagnostics.healthians import client as healthians_client
from modules.engagements.models import Engagement, EngagementParticipant
from modules.engagements.participant_location import (
    can_seed_participant_address_from_engagement,
    copy_engagement_text_address_to_participant,
    participant_has_home_location,
    use_participant_home_for_booking,
)
from modules.engagements.repository import EngagementsRepository

logger = logging.getLogger(__name__)


def healthians_location_for_slots(
    engagement: Engagement,
    participant: EngagementParticipant,
) -> dict[str, str]:
    from modules.engagements.participant_location import booking_location_field

    lat = booking_location_field(participant, engagement, "latitude")
    lng = booking_location_field(participant, engagement, "longitude")
    zipcode = booking_location_field(participant, engagement, "pincode") or ""
    zipcode = str(zipcode).strip() if zipcode is not None else ""
    zone = booking_location_field(participant, engagement, "healthians_zone_id") or ""
    zone = str(zone).strip() if zone is not None else ""
    return {
        "zone_id": zone,
        "lat": str(lat or ""),
        "long": str(lng or ""),
        "zipcode": zipcode,
    }


async def ensure_healthians_zone_for_slots(
    db: AsyncSession,
    *,
    engagement: Engagement,
    participant: EngagementParticipant,
    loc: dict[str, str],
    user_id: int,
) -> tuple[dict[str, str], str | None]:
    """Fill lat/long/zone for Healthians slot fetch; persist on participant (home) or engagement (camp)."""
    from modules.bookings.service import (
        _geocode_for_booking,
        _get_healthians_token,
        _healthians_zone_id,
        _safe_log_healthians_call,
    )

    participant_home = use_participant_home_for_booking(engagement, participant)
    if loc["zone_id"] and loc["lat"] and loc["long"] and loc["zipcode"]:
        return loc, None

    if participant_home and not participant_has_home_location(participant):
        participant_count = await EngagementsRepository().count_distinct_participants_for_engagement(
            db,
            engagement_id=int(engagement.engagement_id),
        )
        if can_seed_participant_address_from_engagement(
            engagement,
            participant_count=participant_count,
        ):
            copy_engagement_text_address_to_participant(participant, engagement)
            db.add(participant)
            await db.flush()
        else:
            return loc, (
                "Home collection address is missing on participant. "
                "Re-check service availability at this address."
            )

    if not loc["lat"] or not loc["long"]:
        if participant_home:
            address = (participant.address or "").strip() or None
            city = (participant.city or "").strip() or None
            pincode = loc["zipcode"] or (participant.pincode or "").strip() or None
            landmark = participant.landmark
        else:
            address = (participant.address or "").strip() or (engagement.address or "").strip() or None
            city = (participant.city or "").strip() or (engagement.city or "").strip() or None
            pincode = loc["zipcode"] or (participant.pincode or "").strip() or None
            landmark = participant.landmark
        if address or city or pincode:
            geocoded = await _geocode_for_booking(
                db,
                address_line=address,
                landmark=landmark,
                city=city,
                pincode=pincode,
                engagement_id=engagement.engagement_id,
                user_id=user_id,
            )
            if geocoded.get("latitude") is not None and geocoded.get("longitude") is not None:
                loc["lat"] = str(geocoded["latitude"])
                loc["long"] = str(geocoded["longitude"])
                if not loc["zipcode"] and geocoded.get("pincode"):
                    loc["zipcode"] = str(geocoded["pincode"])
                elif not loc["zipcode"] and pincode:
                    loc["zipcode"] = str(pincode).strip()
                if participant_home:
                    participant.latitude = float(loc["lat"])
                    participant.longitude = float(loc["long"])
                    if address and not (participant.address or "").strip():
                        participant.address = address
                        participant.sub_locality = address
                    if pincode and not (participant.pincode or "").strip():
                        participant.pincode = str(pincode).strip()
                    if geocoded.get("state"):
                        participant.state = geocoded.get("state")
                    if geocoded.get("country"):
                        participant.country = geocoded.get("country")
                    db.add(participant)
                    await db.flush()
                else:
                    engagement.latitude = float(loc["lat"])
                    engagement.longitude = float(loc["long"])
                    db.add(engagement)
                    await db.flush()

    missing = [key for key in ("lat", "long", "zipcode") if not loc[key]]
    if missing:
        if participant_home:
            return loc, (
                "Home collection address is missing on participant. "
                "Re-check service availability at this address."
            )
        return loc, (
            "Missing Healthians location fields: "
            + ", ".join(missing)
            + ". Re-check service availability at this address."
        )

    if loc["zone_id"]:
        return loc, None

    access_token = await _get_healthians_token()
    svc_payload = {
        "lat": loc["lat"],
        "long": loc["long"],
        "zipcode": loc["zipcode"],
        "is_ppmc_booking": 0,
    }
    await release_request_transaction(db)
    try:
        resp = await healthians_client.check_serviceability_by_location_v2(
            access_token,
            lat=loc["lat"],
            long=loc["long"],
            zipcode=loc["zipcode"],
            is_ppmc_booking=0,
        )
    except Exception as exc:
        logger.exception("Healthians serviceability for slots failed for user %s", user_id)
        await _safe_log_healthians_call(
            db,
            engagement_id=engagement.engagement_id,
            user_id=user_id,
            provider="healthians",
            api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2",
            request_payload=svc_payload,
            status="failed",
            error_message=str(exc),
        )
        return loc, str(exc)

    await _safe_log_healthians_call(
        db,
        engagement_id=engagement.engagement_id,
        user_id=user_id,
        provider="healthians",
        api_url=f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2",
        request_payload=svc_payload,
        response_payload=resp,
        status="success" if resp.get("status") else "failed",
    )

    zone_id = _healthians_zone_id(resp)
    if not resp.get("status") or not zone_id:
        return loc, resp.get("message") or "Healthians zone could not be resolved for this address"

    loc["zone_id"] = str(zone_id)
    if participant_home:
        participant.healthians_zone_id = loc["zone_id"]
        db.add(participant)
        await db.flush()
    else:
        engagement_row = (
            await db.execute(select(Engagement).where(Engagement.engagement_id == engagement.engagement_id))
        ).scalar_one_or_none()
        if engagement_row is not None:
            engagement_row.healthians_zone_id = loc["zone_id"]
            await db.flush()
    return loc, None


async def ensure_participant_home_healthians_location(
    db: AsyncSession,
    *,
    engagement: Engagement,
    participant: EngagementParticipant,
    user_id: int,
) -> str | None:
    """Geocode + zone on participant for home collection; returns error message or None."""
    loc = healthians_location_for_slots(engagement, participant)
    _, loc_error = await ensure_healthians_zone_for_slots(
        db,
        engagement=engagement,
        participant=participant,
        loc=loc,
        user_id=user_id,
    )
    return loc_error
