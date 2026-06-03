"""Load BioAI reports from MetSights and send notifications.

For participants in running engagements where assessment_instance.status == 'complete'
and today >= engagement_date:
1. Check MetSights blood parameters for is_complete.
2. If individual_health_report.reports or report_url is null, fetch from MetSights.
3. After loading, send notifications using engagement.bioai_report_notification.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.service import MetsightsService
from modules.notifications.dedup import has_notification_been_sent
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport

logger = logging.getLogger(__name__)

_PRO_BASIC_TYPE_CODES = {"1", "2"}
_FITPRINT_TYPE_CODES = {"7"}


async def _get_eligible_participants(
    db: AsyncSession,
    today: date,
) -> list[tuple]:
    """Return participants with complete assessments where today >= engagement_date.

    Includes both MetSights Pro/Basic and FitPrint instances.  The IHR is
    joined on ``assessment_instance_id`` so each assessment type gets its own
    report row.
    """
    query = (
        select(
            EngagementParticipant.user_id,
            Engagement.engagement_id,
            AssessmentInstance.metsights_record_id,
            AssessmentPackage.assessment_type_code,
            IndividualHealthReport.reports,
            IndividualHealthReport.report_url,
            Engagement.bioai_report_notification,
            IndividualHealthReport.report_id,
            AssessmentInstance.assessment_instance_id,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .join(
            AssessmentInstance,
            (AssessmentInstance.engagement_id == EngagementParticipant.engagement_id)
            & (AssessmentInstance.user_id == EngagementParticipant.user_id),
        )
        .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
        .outerjoin(
            IndividualHealthReport,
            (IndividualHealthReport.assessment_instance_id == AssessmentInstance.assessment_instance_id),
        )
        .where(Engagement.status.ilike("running"))
        .where(AssessmentInstance.status == "complete")
        .where(EngagementParticipant.engagement_date <= today)
        .where(AssessmentInstance.metsights_record_id.isnot(None))
        .where(AssessmentInstance.metsights_record_id != "")
        .where(AssessmentPackage.assessment_type_code.in_(_PRO_BASIC_TYPE_CODES | _FITPRINT_TYPE_CODES))
    )
    result = await db.execute(query)
    return result.all()


async def load_bioai_reports(
    db: AsyncSession,
    *,
    metsights_service: MetsightsService,
    notifications_service: NotificationsService,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    today = as_of or date.today()
    participants = await _get_eligible_participants(db, today)
    matched = len(participants)
    loaded = 0
    notified = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []

    for row in participants:
        (
            user_id, engagement_id, record_id, type_code,
            existing_reports, existing_report_url,
            bioai_notification, ihr_id, instance_id,
        ) = row

        record_id = (record_id or "").strip()
        type_code = (type_code or "").strip()

        if not record_id:
            skipped += 1
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "skipped", "reason": "no metsights_record_id",
            })
            continue

        if dry_run:
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "dry_run",
                "reason": "reports_null" if not existing_reports else "reports_exists",
            })
            continue

        try:
            # Check if blood parameters are complete on MetSights
            if type_code in _PRO_BASIC_TYPE_CODES:
                try:
                    bp_data = await metsights_service.get_blood_parameters(record_id=record_id)
                    if not (bp_data and bp_data.get("is_complete", False)):
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped", "reason": "blood parameters not complete on MetSights",
                        })
                        continue
                except Exception:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "could not check blood parameters on MetSights",
                    })
                    continue

            needs_reports = existing_reports is None
            needs_url = existing_report_url is None

            if needs_reports or needs_url:
                fetched_reports = None
                fetched_url = None

                if needs_reports:
                    try:
                        report_data = await metsights_service.get_report(
                            record_id=record_id, assessment_type_code=type_code,
                        )
                        if report_data:
                            fetched_reports = report_data
                    except Exception as exc:
                        logger.warning(
                            "MetSights get_report failed for record=%s: %s",
                            record_id, exc,
                        )

                if needs_url:
                    try:
                        pdf_data = await metsights_service.get_report_pdf(
                            record_id=record_id, assessment_type_code=type_code,
                        )
                        if pdf_data:
                            file_url = pdf_data.get("file") or pdf_data.get("url")
                            if file_url:
                                fetched_url = file_url
                    except Exception as exc:
                        logger.warning(
                            "MetSights get_report_pdf failed for record=%s: %s",
                            record_id, exc,
                        )

                if fetched_reports is None and fetched_url is None:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "no report data returned from MetSights",
                    })
                    continue

                ihr = None
                if ihr_id:
                    ihr_result = await db.execute(
                        select(IndividualHealthReport).where(
                            IndividualHealthReport.report_id == ihr_id
                        )
                    )
                    ihr = ihr_result.scalar_one_or_none()

                if ihr is None:
                    ihr = IndividualHealthReport(
                        user_id=user_id,
                        engagement_id=engagement_id,
                        assessment_instance_id=instance_id,
                    )
                    db.add(ihr)

                if fetched_reports is not None:
                    ihr.reports = fetched_reports
                if fetched_url is not None:
                    ihr.report_url = fetched_url
                await db.flush()

                loaded += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "loaded", "reason": "BioAI report data fetched from MetSights",
                })

            # Send notifications
            service_keys = [
                k.strip() for k in (bioai_notification or "").split(",") if k.strip()
            ]
            if service_keys and (ihr_id or loaded):
                for sk in service_keys:
                    already_sent = await has_notification_been_sent(
                        db, service_key=sk, user_id=user_id, engagement_id=engagement_id,
                    )
                    if already_sent:
                        continue
                    await notifications_service.dispatch(
                        db,
                        payload=DispatchRequest(
                            service_key=sk,
                            user_ids=[user_id],
                            engagement_id=engagement_id,
                        ),
                        triggered_by_user_id=None,
                    )
                    notified += 1
            elif not service_keys and (existing_reports is not None and existing_report_url is not None):
                skipped += 1
                if not any(
                    d.get("user_id") == user_id and d.get("engagement_id") == engagement_id
                    for d in details
                ):
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "already loaded, no notification keys configured",
                    })

        except Exception as exc:
            await db.rollback()
            failed += 1
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "failed", "reason": str(exc)[:200],
            })
            logger.warning(
                "load_bioai_reports failed: user=%s engagement=%s: %s",
                user_id, engagement_id, exc, exc_info=True,
            )

    return {
        "as_of": today.isoformat(),
        "matched": matched,
        "loaded": loaded,
        "notified": notified,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry_run,
        "details": details,
    }
