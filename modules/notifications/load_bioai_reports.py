"""Load BioAI reports from MetSights and send notifications.

For participants in running engagements with a MetSights Basic/Pro assessment
where today >= engagement_date:
1. Check MetSights blood parameters for is_complete (Pro/Basic only).
2. If individual_health_report.reports or report_url is null, fetch from MetSights.
3. When both reports and report_url are present, send notifications using
   engagement_notifications for bioai_report_ready event (skipping services already sent).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.bioai_report.pdf_registration import register_permanent_bio_ai_report_url
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.service import MetsightsService
from modules.engagement_notifications.service_config import (
    NotificationServiceConfigItem,
    normalize_notification_services,
)
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport

if TYPE_CHECKING:
    from modules.assessments.service import AssessmentsService
    from modules.metsights.sync_service import MetsightsSyncService

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, int, int, int, int], None]

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
    """Extract a PDF/file URL from a MetSights report payload."""
    if not isinstance(report_data, dict):
        return None
    for key in ("file", "url", "report_url", "pdf_url"):
        value = report_data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    data = report_data.get("data")
    if isinstance(data, dict):
        for key in ("file", "url", "report_url", "pdf_url"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


async def _fetch_metsights_report_json(
    db: AsyncSession,
    *,
    metsights_service: MetsightsService,
    record_id: str,
    type_code: str,
    engagement_id: int,
    user_id: int,
) -> Any | None:
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
    if report_data is not None and report_data:
        return report_data
    if report_data is None:
        logger.warning(
            "MetSights get_report failed for record=%s",
            record_id,
        )
    return None


async def _try_recover_missing_vitals_bp(
    db: AsyncSession,
    *,
    assessments_service: "AssessmentsService",
    sync_service: "MetsightsSyncService",
    user_id: int,
    engagement_id: int,
    instance_id: int,
    details: list[dict[str, Any]],
) -> bool:
    """Draft default BP, push vitals to MetSights. Returns True on success."""
    if not await assessments_service.is_vitals_blood_pressure_missing(
        db,
        assessment_instance_id=instance_id,
    ):
        return False

    draft_result = await assessments_service.draft_vitals_blood_pressure_fallbacks(
        db,
        user_id=user_id,
        assessment_instance_id=instance_id,
    )
    await db.commit()
    drafted_count = int(draft_result.get("responses_drafted") or 0)
    details.append({
        "user_id": user_id,
        "engagement_id": engagement_id,
        "action": "drafted",
        "reason": (
            f"applied default BP 120/80 ({drafted_count} vitals responses drafted)"
        ),
    })

    push_result = await sync_service._push_category_to_metsights(
        db,
        assessment_instance_id=instance_id,
        user_id=user_id,
        category_key="vitals",
    )
    await db.commit()
    fields_count = len(push_result.get("fields_pushed") or [])
    details.append({
        "user_id": user_id,
        "engagement_id": engagement_id,
        "action": "pushed",
        "reason": f"pushed vitals to MetSights ({fields_count} fields)",
    })
    return True


async def _get_eligible_participants(
    db: AsyncSession,
    today: date,
    *,
    all_engagements: bool = False,
    engagement_id: int | None = None,
    user_ids: set[int] | None = None,
    ignore_engagement_date: bool = False,
) -> list[tuple]:
    """Return participants with MetSights Basic/Pro assessments where today >= engagement_date.

    FitPrint (type 7) fitness reports are loaded separately — they are not BioAI reports.
    """
    from modules.engagements.models import AutoNotificationEvent, EngagementNotification

    en_sub = (
        select(
            EngagementNotification.engagement_id,
            EngagementNotification.notification_services,
        )
        .join(AutoNotificationEvent, AutoNotificationEvent.id == EngagementNotification.notification_event_id)
        .where(AutoNotificationEvent.event_code == "bioai_report_ready")
        .subquery("en_bioai")
    )

    query = (
        select(
            EngagementParticipant.user_id,
            Engagement.engagement_id,
            AssessmentInstance.metsights_record_id,
            AssessmentPackage.assessment_type_code,
            IndividualHealthReport.reports,
            IndividualHealthReport.report_url,
            en_sub.c.notification_services.label("bioai_report_services"),
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
        .outerjoin(
            en_sub,
            en_sub.c.engagement_id == Engagement.engagement_id,
        )
        .where(AssessmentPackage.assessment_type_code.in_(_PRO_BASIC_TYPE_CODES))
    )
    if not ignore_engagement_date:
        query = query.where(EngagementParticipant.engagement_date <= today)
    if not all_engagements:
        query = query.where(Engagement.status.ilike("running"))
    if engagement_id is not None:
        query = query.where(Engagement.engagement_id == engagement_id)
    if user_ids is not None:
        if not user_ids:
            return []
        query = query.where(EngagementParticipant.user_id.in_(user_ids))
    query = (
        query.where(AssessmentInstance.metsights_record_id.isnot(None))
        .where(AssessmentInstance.metsights_record_id != "")
    )
    result = await db.execute(query)
    return result.all()


async def _send_report_notifications(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    service_configs: list[NotificationServiceConfigItem],
    user_id: int,
    engagement_id: int,
    assessment_instance_id: int,
    details: list[dict[str, Any]],
) -> int:
    """Dispatch configured notification services that have not already been sent."""
    sent_count = 0

    for cfg in service_configs:
        sk = cfg.service_key
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
                assessment_instance_id=assessment_instance_id,
                external_link=cfg.external_link,
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
    assessments_service: "AssessmentsService | None" = None,
    sync_service: "MetsightsSyncService | None" = None,
    as_of: date | None = None,
    dry_run: bool = False,
    all_engagements: bool = False,
    engagement_id: int | None = None,
    user_ids: set[int] | None = None,
    send_notifications: bool = True,
    ignore_engagement_date: bool = False,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Load BioAI reports and notify participants.

    ``on_progress(done, total, loaded, notified, skipped, failed)`` is called
    after each participant is processed so CLI runners can show a live progress bar.
    """
    today = as_of or date.today()
    participants = await _get_eligible_participants(
        db,
        today,
        all_engagements=all_engagements,
        engagement_id=engagement_id,
        user_ids=user_ids,
        ignore_engagement_date=ignore_engagement_date,
    )
    matched = len(participants)
    loaded = 0
    notified = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []

    def _report(done: int) -> None:
        if on_progress is not None:
            on_progress(done, matched, loaded, notified, skipped, failed)

    _report(0)

    for index, row in enumerate(participants, start=1):
        try:
            (
                user_id, engagement_id, record_id, type_code,
                existing_reports, existing_report_url,
                bioai_report_services, ihr_id, instance_id,
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
                        fetched_reports = await _fetch_metsights_report_json(
                            db,
                            metsights_service=metsights_service,
                            record_id=record_id,
                            type_code=type_code,
                            engagement_id=engagement_id,
                            user_id=user_id,
                        )

                    if report_url is None:
                        try:
                            fetched_url = await register_permanent_bio_ai_report_url(
                                db,
                                assessment_instance_id=instance_id,
                                engagement_id=engagement_id,
                                user_id=user_id,
                            )
                        except Exception as exc:
                            logger.warning(
                                "bio-ai-reports registration failed for instance=%s: %s",
                                instance_id,
                                exc,
                            )
                            fetched_url = None

                    if (
                        fetched_reports is None
                        and fetched_url is None
                        and assessments_service is not None
                        and sync_service is not None
                    ):
                        try:
                            recovered = await _try_recover_missing_vitals_bp(
                                db,
                                assessments_service=assessments_service,
                                sync_service=sync_service,
                                user_id=user_id,
                                engagement_id=engagement_id,
                                instance_id=instance_id,
                                details=details,
                            )
                            if recovered:
                                if reports is None:
                                    fetched_reports = await _fetch_metsights_report_json(
                                        db,
                                        metsights_service=metsights_service,
                                        record_id=record_id,
                                        type_code=type_code,
                                        engagement_id=engagement_id,
                                        user_id=user_id,
                                    )
                                if report_url is None:
                                    try:
                                        fetched_url = await register_permanent_bio_ai_report_url(
                                            db,
                                            assessment_instance_id=instance_id,
                                            engagement_id=engagement_id,
                                            user_id=user_id,
                                        )
                                    except Exception as exc:
                                        logger.warning(
                                            "bio-ai-reports registration retry failed for instance=%s: %s",
                                            instance_id,
                                            exc,
                                        )
                                        fetched_url = None
                        except Exception as exc:
                            await db.rollback()
                            logger.warning(
                                "BioAI vitals BP recovery failed for user=%s instance=%s: %s",
                                user_id,
                                instance_id,
                                exc,
                            )
                            details.append({
                                "user_id": user_id,
                                "engagement_id": engagement_id,
                                "action": "skipped",
                                "reason": f"vitals BP recovery failed: {str(exc)[:100]}",
                            })

                    if fetched_reports is None and fetched_url is None:
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped", "reason": "no report data returned from MetSights or bio-ai-reports",
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

                if send_notifications:
                    service_configs = normalize_notification_services(bioai_report_services)
                    if not service_configs:
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped", "reason": "reports ready, no notification keys configured",
                        })
                        continue

                    notified += await _send_report_notifications(
                        db,
                        notifications_service=notifications_service,
                        service_configs=service_configs,
                        user_id=user_id,
                        engagement_id=engagement_id,
                        assessment_instance_id=instance_id,
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
        finally:
            _report(index)

    return {
        "as_of": today.isoformat(),
        "engagement_id": engagement_id,
        "matched": matched,
        "loaded": loaded,
        "notified": notified,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry_run,
        "details": details,
    }
