"""Resolve Healthians booking id from participant or Metsights fetch-collections."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.service import MetsightsService
from modules.reports.blood_parameters_schemas import (
    booking_id_from_fetch_collections,
    provider_code_from_field,
)


class HealthiansBookingSource(str, Enum):
    PARTICIPANT = "participant"
    METSIGHTS = "metsights"


@dataclass(frozen=True)
class ResolvedHealthiansBooking:
    booking_id: str
    source: HealthiansBookingSource
    collection_data: dict[str, Any] | None = None


def is_healthians_diagnostic_provider(diagnostic_provider: str | None) -> bool:
    return (diagnostic_provider or "").strip().lower() == "healthians"


def try_participant_booking_id(
    participant_booking_id: str | None,
    diagnostic_provider: str | None,
) -> str | None:
    """Return participant booking_id when the engagement package is Healthians."""
    booking_id = (participant_booking_id or "").strip()
    if not booking_id:
        return None
    if not is_healthians_diagnostic_provider(diagnostic_provider):
        return None
    return booking_id


async def _load_participant_booking_context(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
) -> tuple[str | None, str | None]:
    result = await db.execute(
        select(EngagementParticipant.booking_id, DiagnosticPackage.diagnostic_provider)
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .outerjoin(
            DiagnosticPackage,
            DiagnosticPackage.diagnostic_package_id == Engagement.diagnostic_package_id,
        )
        .where(EngagementParticipant.user_id == user_id)
        .where(EngagementParticipant.engagement_id == engagement_id)
        .order_by(EngagementParticipant.engagement_participant_id.desc())
        .limit(1)
    )
    row = result.one_or_none()
    if row is None:
        return None, None
    return row[0], row[1]


async def resolve_healthians_booking_id(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
    record_id: str,
    metsights_service: MetsightsService,
    participant_booking_id: str | None = None,
    diagnostic_provider: str | None = None,
) -> ResolvedHealthiansBooking:
    """Prefer engagement_participants.booking_id for Healthians; else Metsights fetch-collections."""
    rid = (record_id or "").strip()
    if not rid:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Metsights record id is missing",
        )

    if participant_booking_id is None and diagnostic_provider is None:
        participant_booking_id, diagnostic_provider = await _load_participant_booking_context(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )

    from_participant = try_participant_booking_id(participant_booking_id, diagnostic_provider)
    if from_participant:
        return ResolvedHealthiansBooking(
            booking_id=from_participant,
            source=HealthiansBookingSource.PARTICIPANT,
        )

    collection_data = await metsights_service.get_fetch_collections(record_id=rid)
    provider_code = provider_code_from_field(collection_data.get("provider"))
    if provider_code.lower() != "healthians":
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message=(
                f"Blood report provider is '{provider_code or 'unknown'}', "
                "only Healthians is supported for provider load"
            ),
        )

    reference_id = booking_id_from_fetch_collections(collection_data)
    if not reference_id:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Metsights collection is missing the provider booking id",
        )

    return ResolvedHealthiansBooking(
        booking_id=reference_id,
        source=HealthiansBookingSource.METSIGHTS,
        collection_data=collection_data,
    )
