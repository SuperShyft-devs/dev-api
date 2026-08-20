"""Shared consultation cabin slot validation for bookings and onboard."""

from __future__ import annotations

from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.enums import ConsultationMode
from modules.engagements.models import Engagement
from modules.engagements.slot_info_repository import EngagementSlotInfoRepository
from modules.engagements.slot_availability import (
    coerce_time,
    format_hhmm,
    require_available_consultation_slot,
    slot_unavailable,
)
from modules.experts.consultation_bookings_repository import ConsultationBookingsRepository


def effective_consultation_mode(engagement: Engagement) -> ConsultationMode:
    if engagement.consultation_mode is not None:
        return engagement.consultation_mode
    if engagement.organization_id is None:
        return ConsultationMode.online
    return ConsultationMode.offline


async def validate_consultation_cabin_slot_for_booking(
    db: AsyncSession,
    *,
    engagement: Engagement,
    expert_type: str,
    consultation_date: date,
    cabin_key: str,
    slot_val: str,
    consultation_bookings: ConsultationBookingsRepository,
) -> tuple[str, str]:
    cabin_val = (cabin_key or "").strip() or None
    if not cabin_val or not slot_val:
        raise slot_unavailable()
    slot_time = coerce_time(slot_val)
    if slot_time is None:
        raise slot_unavailable()
    slot_detail = None
    if engagement.slot_detail_id is not None:
        slot_detail = await EngagementSlotInfoRepository().get_by_id(
            db,
            int(engagement.slot_detail_id),
        )
    cabin = require_available_consultation_slot(
        slot_detail,
        expert_type=str(expert_type),
        consultation_date=consultation_date,
        cabin_key=cabin_val,
        slot_time=slot_time,
    )
    persisted_cabin = (cabin.get("cabin_key") or "").strip()
    slot_hhmm = format_hhmm(slot_time)
    count = await consultation_bookings.count_cabin_slot_bookings(
        db,
        engagement_id=int(engagement.engagement_id),
        consultation_cabin=persisted_cabin,
        consultation_date=consultation_date,
        consultation_slot=slot_hhmm,
        slot_detail_id=(
            int(engagement.slot_detail_id) if engagement.slot_detail_id is not None else None
        ),
    )
    capacity = int(cabin.get("capacity_per_slot") or 0)
    if count >= capacity:
        raise slot_unavailable()
    return persisted_cabin, slot_hhmm
