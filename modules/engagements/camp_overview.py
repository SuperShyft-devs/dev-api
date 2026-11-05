"""B2B camp overview: progress KPIs + day-wise summary for org engagements."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance
from modules.engagements.blood_booking_enums import BloodBookingStatus
from modules.engagements.enums import BloodCollectionType
from modules.engagements.models import Engagement, EngagementParticipant, ParticipantBloodBooking
from modules.engagements.slot_detail_dates import enabled_blood_collection_dates
from modules.experts.models import ConsultationBooking
from modules.reports.models import IndividualHealthReport

_ACTIVE = BloodBookingStatus.active.value


def _summary_dates(
    engagement: Engagement,
    slot_detail: dict[str, Any] | None,
) -> list[date]:
    """Camp collection uses on-site blood dates. Other modes use the engagement window."""
    mode = engagement.blood_collection_type
    mode_value = mode.value if isinstance(mode, BloodCollectionType) else mode
    if mode_value == BloodCollectionType.camp_collection.value:
        return enabled_blood_collection_dates(slot_detail)

    start = engagement.start_date
    end = engagement.end_date
    if not start or not end or end < start:
        return []
    if (end - start).days > 60:
        end = start + timedelta(days=60)
    days: list[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


async def build_camp_overview(
    db: AsyncSession,
    *,
    engagement: Engagement,
    slot_detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    eng_id = int(engagement.engagement_id)

    registered = int(
        (
            await db.execute(
                select(func.count())
                .select_from(EngagementParticipant)
                .where(EngagementParticipant.engagement_id == eng_id)
            )
        ).scalar_one()
        or 0
    )

    collected = int(
        (
            await db.execute(
                select(func.count(func.distinct(EngagementParticipant.user_id)))
                .select_from(EngagementParticipant)
                .join(
                    ParticipantBloodBooking,
                    and_(
                        ParticipantBloodBooking.engagement_participant_id
                        == EngagementParticipant.engagement_participant_id,
                        ParticipantBloodBooking.status == _ACTIVE,
                        ParticipantBloodBooking.collected_at.is_not(None),
                    ),
                )
                .where(EngagementParticipant.engagement_id == eng_id)
            )
        ).scalar_one()
        or 0
    )

    blood_report = int(
        (
            await db.execute(
                select(func.count(func.distinct(EngagementParticipant.user_id)))
                .select_from(EngagementParticipant)
                .join(
                    ParticipantBloodBooking,
                    and_(
                        ParticipantBloodBooking.engagement_participant_id
                        == EngagementParticipant.engagement_participant_id,
                        ParticipantBloodBooking.status == _ACTIVE,
                        ParticipantBloodBooking.diagnostic_report_url.is_not(None),
                        func.trim(ParticipantBloodBooking.diagnostic_report_url) != "",
                    ),
                )
                .where(EngagementParticipant.engagement_id == eng_id)
            )
        ).scalar_one()
        or 0
    )

    questionnaire_done = int(
        (
            await db.execute(
                select(func.count(func.distinct(AssessmentInstance.user_id)))
                .select_from(AssessmentInstance)
                .where(AssessmentInstance.engagement_id == eng_id)
                .where(
                    or_(
                        AssessmentInstance.completed_at.is_not(None),
                        AssessmentInstance.metsights_record_id.is_not(None),
                    )
                )
            )
        ).scalar_one()
        or 0
    )

    bio_ai = int(
        (
            await db.execute(
                select(func.count(func.distinct(IndividualHealthReport.user_id)))
                .select_from(IndividualHealthReport)
                .where(IndividualHealthReport.engagement_id == eng_id)
                .where(
                    or_(
                        and_(
                            IndividualHealthReport.report_url.is_not(None),
                            func.trim(IndividualHealthReport.report_url) != "",
                        ),
                        IndividualHealthReport.reports.is_not(None),
                    )
                )
            )
        ).scalar_one()
        or 0
    )

    consultations_done = int(
        (
            await db.execute(
                select(func.count(func.distinct(EngagementParticipant.user_id)))
                .select_from(ConsultationBooking)
                .join(
                    EngagementParticipant,
                    EngagementParticipant.engagement_participant_id
                    == ConsultationBooking.engagement_participant_id,
                )
                .where(EngagementParticipant.engagement_id == eng_id)
                .where(ConsultationBooking.done.is_(True))
            )
        ).scalar_one()
        or 0
    )

    collection_pending = int(
        (
            await db.execute(
                select(func.count())
                .select_from(ParticipantBloodBooking)
                .join(
                    EngagementParticipant,
                    EngagementParticipant.engagement_participant_id
                    == ParticipantBloodBooking.engagement_participant_id,
                )
                .where(EngagementParticipant.engagement_id == eng_id)
                .where(ParticipantBloodBooking.status == _ACTIVE)
                .where(ParticipantBloodBooking.collected_at.is_(None))
            )
        ).scalar_one()
        or 0
    )

    reports_pending = max(0, collected - blood_report)
    questionnaire_pending = max(0, registered - questionnaire_done)
    consultations_pending = max(0, registered - consultations_done)

    days: list[dict[str, Any]] = []
    for day_i, cur in enumerate(_summary_dates(engagement, slot_detail), start=1):
        day_rows = await _day_metrics(db, engagement_id=eng_id, day=cur)
        days.append({"date": cur.isoformat(), "day_index": day_i, **day_rows})

    return {
        "engagement_id": eng_id,
        "engagement_code": engagement.engagement_code,
        "organization_id": engagement.organization_id,
        "progress": {
            "registered": registered,
            "collected": collected,
            "questionnaire_done": questionnaire_done,
            "blood_report": blood_report,
            "bio_ai": bio_ai,
            "consultations_done": consultations_done,
        },
        "needs_attention": {
            "collection_pending": collection_pending,
            "questionnaire_pending": questionnaire_pending,
            "reports_pending": reports_pending,
            "consultations_pending": consultations_pending,
        },
        "days": days,
    }


async def _day_metrics(db: AsyncSession, *, engagement_id: int, day: date) -> dict[str, int]:
    scheduled = int(
        (
            await db.execute(
                select(func.count())
                .select_from(ParticipantBloodBooking)
                .join(
                    EngagementParticipant,
                    EngagementParticipant.engagement_participant_id
                    == ParticipantBloodBooking.engagement_participant_id,
                )
                .where(EngagementParticipant.engagement_id == engagement_id)
                .where(ParticipantBloodBooking.status == _ACTIVE)
                .where(ParticipantBloodBooking.collection_date == day)
            )
        ).scalar_one()
        or 0
    )
    collected = int(
        (
            await db.execute(
                select(func.count())
                .select_from(ParticipantBloodBooking)
                .join(
                    EngagementParticipant,
                    EngagementParticipant.engagement_participant_id
                    == ParticipantBloodBooking.engagement_participant_id,
                )
                .where(EngagementParticipant.engagement_id == engagement_id)
                .where(ParticipantBloodBooking.status == _ACTIVE)
                .where(func.date(ParticipantBloodBooking.collected_at) == day)
            )
        ).scalar_one()
        or 0
    )
    report = int(
        (
            await db.execute(
                select(func.count())
                .select_from(ParticipantBloodBooking)
                .join(
                    EngagementParticipant,
                    EngagementParticipant.engagement_participant_id
                    == ParticipantBloodBooking.engagement_participant_id,
                )
                .where(EngagementParticipant.engagement_id == engagement_id)
                .where(ParticipantBloodBooking.status == _ACTIVE)
                .where(ParticipantBloodBooking.collection_date == day)
                .where(ParticipantBloodBooking.diagnostic_report_url.is_not(None))
                .where(func.trim(ParticipantBloodBooking.diagnostic_report_url) != "")
            )
        ).scalar_one()
        or 0
    )
    questionnaire = int(
        (
            await db.execute(
                select(func.count())
                .select_from(AssessmentInstance)
                .where(AssessmentInstance.engagement_id == engagement_id)
                .where(func.date(AssessmentInstance.completed_at) == day)
            )
        ).scalar_one()
        or 0
    )
    consultation = int(
        (
            await db.execute(
                select(func.count())
                .select_from(ConsultationBooking)
                .join(
                    EngagementParticipant,
                    EngagementParticipant.engagement_participant_id
                    == ConsultationBooking.engagement_participant_id,
                )
                .where(EngagementParticipant.engagement_id == engagement_id)
                .where(ConsultationBooking.done.is_(True))
                .where(ConsultationBooking.consultation_date == day)
            )
        ).scalar_one()
        or 0
    )
    # Recollect proxy: active bookings on this day that are still uncollected past the day.
    recollect = 0
    if day < date.today():
        recollect = int(
            (
                await db.execute(
                    select(func.count())
                    .select_from(ParticipantBloodBooking)
                    .join(
                        EngagementParticipant,
                        EngagementParticipant.engagement_participant_id
                        == ParticipantBloodBooking.engagement_participant_id,
                    )
                    .where(EngagementParticipant.engagement_id == engagement_id)
                    .where(ParticipantBloodBooking.status == _ACTIVE)
                    .where(ParticipantBloodBooking.collection_date == day)
                    .where(ParticipantBloodBooking.collected_at.is_(None))
                )
            ).scalar_one()
            or 0
        )

    return {
        "registered": scheduled,
        "collected": collected,
        "questionnaire": questionnaire,
        "report": report,
        "consultation": consultation,
        "recollect": recollect,
    }
