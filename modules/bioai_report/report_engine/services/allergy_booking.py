"""Resolve the blood booking that belongs to one assessment engagement.

The lookup uses user id and engagement id together. It never picks the latest
booking for a user across engagements.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance
from modules.engagements.models import EngagementParticipant, ParticipantBloodBooking


def resolve_bookings(
    bookings: list[dict[str, Any]],
    *,
    user_id: int,
    engagement_id: int,
) -> tuple[str, dict[str, Any] | None, str | None]:
    """Return ``(status, booking, reason)``.

    Status is ``resolved``, ``not_found``, or ``unresolved``.
    """
    matched = [
        booking
        for booking in bookings
        if _same(booking.get("user_id"), user_id) and _same(booking.get("engagement_id"), engagement_id)
    ]
    if not matched:
        return "not_found", None, "no blood booking for this engagement"
    if len(matched) == 1:
        return "resolved", matched[0], None
    return "unresolved", None, "multiple blood bookings for this engagement"


async def load_booking_for_assessment(
    db: AsyncSession,
    *,
    assessment_instance_id: int,
) -> tuple[str, dict[str, Any] | None, str | None]:
    instance = await db.get(AssessmentInstance, assessment_instance_id)
    if instance is None:
        return "not_found", None, "assessment not found"
    rows = (
        await db.execute(
            select(ParticipantBloodBooking, EngagementParticipant.user_id, EngagementParticipant.engagement_id)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id
                == ParticipantBloodBooking.engagement_participant_id,
            )
            .where(
                EngagementParticipant.user_id == instance.user_id,
                EngagementParticipant.engagement_id == instance.engagement_id,
            )
        )
    ).all()
    payloads = [_booking_payload(booking, user_id, engagement_id) for booking, user_id, engagement_id in rows]
    return resolve_bookings(
        payloads,
        user_id=int(instance.user_id),
        engagement_id=int(instance.engagement_id),
    )


def _booking_payload(
    booking: ParticipantBloodBooking,
    user_id: int,
    engagement_id: int,
) -> dict[str, Any]:
    collected = getattr(booking, "collection_date", None)
    return {
        "id": booking.id,
        "user_id": int(user_id),
        "engagement_id": int(engagement_id),
        "booking_id": booking.booking_id,
        "collection_date": collected.isoformat() if collected is not None else None,
        "parameters_source": "participant_blood_bookings.blood_parameters",
        "blood_parameters": booking.blood_parameters,
        "blood_report_raw": booking.blood_report_raw,
    }


def _same(left: Any, right: int) -> bool:
    try:
        return int(left) == int(right)
    except (TypeError, ValueError):
        return False
