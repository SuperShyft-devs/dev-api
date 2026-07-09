"""Load BioAI reports from MetSights and send notifications.

For participants in running engagements where the MetSights Basic/Pro assessment is
in progress or completed and today >= engagement_date:
1. Check MetSights blood parameters for is_complete (Pro/Basic only).
2. If individual_health_report.reports or report_url is null, fetch from MetSights.
3. When both reports and report_url are present, send notifications using
   engagement.bioai_report_notification (skipping services already sent).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.service import MetsightsService
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport

logger = logging.getLogger(__name__)

_PRO_BASIC_TYPE_CODES = {"1", "2"}


def _metsights_report_url(*, record_id: str, assessment_type_code: str, pdf: bool = False) -> str:
    base = settings.METSIGHTS_BASE_URL.rstrip("/")
    suffix = "/pdf/" if pdf else "/"
    if assessment_type_code == "7":
        return f"{base}/reports/fitness-reports/{record_id}{suffix}"
    return f"{base}/reports/{record_id}{suffix}"


def _metsights_blood_parameters_url(*, record_id: str) -> str:
    return f"{settings.METSIGHTS_BASE_URL.rstrip('/')}/records/{record_id}/blood-parameters/"


def _report_data_complete(reports: Any, report_url: Any) -> bool:
    return reports is not None and report_url is not None


def _extract_report_file_url(report_data: Any) -> str | None:
    if not isinstance(report_data, dict):
        return None
    file_url = report_data.get("file") or report_data.get("url")
    if file_url is None:
        return None
    normalized = str(file_url).strip()
    return normalized or None


async def _get_eligible_participants(
    db: AsyncSession,
    today: date,
) -> list[tuple]:
    """Return participants with complete MetSights Basic/Pro assessments where today >= engagement_date.

    FitPrint (type 7) fitness reports are loaded separately — they are not BioAI reports.
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
            IndividualHealthReport.assessment_instance_id
            == AssessmentInstance.assessment_instance_id,
        )
        .where(Engagement.status.ilike("running"))
        .where(func.lower(AssessmentInstance.status).in_(["active", "completed", "complete"]))
        .where(EngagementParticipant.engagement_date <= today)
        .where(AssessmentInstance.metsights_record_id.isnot(None))
        .where(AssessmentInstance.metsights_record_id != "")
        .where(AssessmentPackage.assessment_type_code.in_(_PRO_BASIC_TYPE_CODES))
    )
    result = await db.execute(query)
    return result.all()


async def _send_report_notifications(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    service_keys: list[str],
    user_id: int,
    engagement_id: int,
    details: list[dict[str, Any]],
) -> int:
    """Dispatch configured notification services that have not already been sent."""
    sent_count = 0

    for sk in service_keys:
        skip_reason = await should_skip_notification(
            db, service_key=sk, user_id=user_id, engagement_id=engagement_id,
        )
        if skip_reason:
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "skipped",
                "reason": f"notification '{sk}' {skip_reason}",
            })
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
        sent_count += 1
        details.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "action": "notified",
            "reason": f"dispatched '{sk}'",
        })

    return sent_count


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
            complete = _report_data_complete(existing_reports, existing_report_url)
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "dry_run",
                "reason": "reports_complete" if complete else "reports_incomplete",
            })
            continue

        try:
            if type_code in _PRO_BASIC_TYPE_CODES:
                bp_data = await tracked_integration_call(
                    db,
                    provider="metsights",
                    api_url=_metsights_blood_parameters_url(record_id=record_id),
                    engagement_id=engagement_id,
                    user_id=user_id,
                    request_payload={"record_id": record_id},
                    operation=lambda: metsights_service.get_blood_parameters(record_id=record_id),
                    reraise=False,
                )
                if bp_data is None:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "could not check blood parameters on MetSights",
                    })
                    continue
                if not bp_data.get("is_complete", False):
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "blood parameters not complete on MetSights",
                    })
                    continue

            reports = existing_reports
            report_url = existing_report_url

            if not _report_data_complete(reports, report_url):
                fetched_reports = None
                fetched_url = None

                if reports is None:
                    report_data = await tracked_integration_call(
                        db,
                        provider="metsights",
                        api_url=_metsights_report_url(
                            record_id=record_id,
                            assessment_type_code=type_code,
                        ),
                        engagement_id=engagement_id,
                        user_id=user_id,
                        request_payload={
                            "record_id": record_id,
                            "assessment_type_code": type_code,
                        },
                        operation=lambda: metsights_service.get_report(
                            record_id=record_id,
                            assessment_type_code=type_code,
                        ),
                        reraise=False,
                    )
                    if report_data is not None:
                        if report_data:
                            fetched_reports = report_data
                            if fetched_url is None:
                                fetched_url = _extract_report_file_url(report_data)
                    else:
                        logger.warning(
                            "MetSights get_report failed for record=%s",
                            record_id,
                        )

                if report_url is None:
                    pdf_data = await tracked_integration_call(
                        db,
                        provider="metsights",
                        api_url=_metsights_report_url(
                            record_id=record_id,
                            assessment_type_code=type_code,
                            pdf=True,
                        ),
                        engagement_id=engagement_id,
                        user_id=user_id,
                        request_payload={
                            "record_id": record_id,
                            "assessment_type_code": type_code,
                        },
                        operation=lambda: metsights_service.get_report_pdf(
                            record_id=record_id,
                            assessment_type_code=type_code,
                        ),
                        reraise=False,
                    )
                    if pdf_data is not None:
                        file_url = pdf_data.get("file") or pdf_data.get("url")
                        if file_url:
                            fetched_url = file_url
                    else:
                        logger.warning(
                            "MetSights get_report_pdf failed for record=%s",
                            record_id,
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
                    reports = fetched_reports
                if fetched_url is not None:
                    ihr.report_url = fetched_url
                    report_url = fetched_url
                await db.flush()
                await db.commit()

                if not _report_data_complete(reports, report_url):
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "report data incomplete (missing reports or report_url)",
                    })
                    continue

                loaded += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "loaded", "reason": "BioAI report data fetched from MetSights",
                })

            if not _report_data_complete(reports, report_url):
                skipped += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "skipped",
                    "reason": "BioAI report data incomplete (missing reports or report_url)",
                })
                continue

            service_keys = [
                k.strip() for k in (bioai_notification or "").split(",") if k.strip()
            ]
            if not service_keys:
                skipped += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "skipped", "reason": "reports ready, no notification keys configured",
                })
                continue

            notified += await _send_report_notifications(
                db,
                notifications_service=notifications_service,
                service_keys=service_keys,
                user_id=user_id,
                engagement_id=engagement_id,
                details=details,
            )
            await db.commit()

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
