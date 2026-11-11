"""Load BioAI reports from MetSights and send notifications.

For participants in running engagements with a MetSights Basic/Pro assessment
where today >= engagement_date:
1. Check MetSights blood parameters for is_complete (Pro/Basic only).
   If missing/incomplete, fill internal blood defaults (including LH and FSH),
   mark those categories complete, push them to MetSights, then continue.
2. If the MetSights report is missing because the record is not complete, push
   physical measurements, vitals, and diet/lifestyle from the local questionnaire
   and mark those modules complete, then request the report again.
3. If individual_health_report.reports or report_url is null, fetch from MetSights.
4. When both reports and report_url are present, send notifications using
   engagement_notifications for bioai_report_ready event (skipping services already sent).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from db.seed.blood_parameters_registry import BLOOD_PARAMETER_CATEGORY_KEY
from modules.assessments.essentials_vitals import is_metsights_essentials
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.bioai_report.pdf_registration import (
    is_bio_ai_reports_permanent_url,
    is_metsights_hosted_report_url,
    regenerate_permanent_bio_ai_report_url,
    register_permanent_bio_ai_report_url,
    resolve_canonical_bio_ai_report_url,
)
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.service import MetsightsService
from modules.engagement_notifications.service_config import normalize_notification_services
from modules.notifications.report_notifications import send_report_notifications
from modules.notifications.service import NotificationsService
from modules.notifications.load_blood_reports import _get_or_create_ihr
from modules.reports.blood_booking_reports import has_archived_diagnostic_blood_report
from modules.reports.repository import ReportsRepository

if TYPE_CHECKING:
    from modules.assessments.service import AssessmentsService
    from modules.metsights.sync_service import MetsightsSyncService

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, int, int, int, int], None]

_PRO_BASIC_TYPE_CODES = {"1", "2"}


def _metsights_report_url(*, record_id: str, assessment_type_code: str, pdf: bool = False) -> str:
    from modules.metsights.integration_logging import metsights_api_url

    branch = "fitness" if assessment_type_code == "7" else "metsights"
    path = f"reports/{branch}/{record_id}"
    if pdf:
        path = f"{path}/pdf"
    return metsights_api_url(path)


def _metsights_blood_parameters_url(*, record_id: str) -> str:
    from modules.metsights.integration_logging import metsights_api_url

    return metsights_api_url(f"records/{record_id}/blood-parameters")


def _has_usable_reports(reports: Any) -> bool:
    if reports is None:
        return False
    if isinstance(reports, str):
        return bool(reports.strip())
    if isinstance(reports, dict):
        return bool(reports)
    if isinstance(reports, list):
        return bool(reports)
    return True


def _has_usable_report_url(report_url: Any) -> bool:
    if report_url is None:
        return False
    if isinstance(report_url, str):
        return bool(report_url.strip())
    return True


def _report_data_complete(reports: Any, report_url: Any) -> bool:
    """Generic IHR completeness (FitPrint and other loaders)."""
    return _has_usable_reports(reports) and _has_usable_report_url(report_url)


def _has_registered_bio_ai_pdf_url(report_url: Any) -> bool:
    """BioAI load is complete only when PDF is not still a MetSights-hosted file URL."""
    if not _has_usable_report_url(report_url):
        return False
    text = str(report_url).strip()
    if is_metsights_hosted_report_url(text):
        return False
    if is_bio_ai_reports_permanent_url(text):
        return True
    lowered = text.lower()
    if "/media/bio-ai/" in lowered or "bio-ai-reports" in lowered:
        return True
    return True


def _bioai_report_data_complete(reports: Any, report_url: Any) -> bool:
    return _has_usable_reports(reports) and _has_registered_bio_ai_pdf_url(report_url)


def _report_url_for_bio_ai_registration(report_url: Any) -> str | None:
    """Treat MetSights PDF URLs as missing so we register a permanent bio-ai-reports link."""
    if not _has_usable_report_url(report_url):
        return None
    text = str(report_url).strip()
    if is_metsights_hosted_report_url(text):
        return None
    if _has_registered_bio_ai_pdf_url(text):
        return text
    return None


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
) -> tuple[Any | None, bool]:
    """Return ``(report, record_incomplete)``.

    ``record_incomplete`` is true when MetSights answers 404. That response means
    the record is missing or not complete, so result generation cannot start.
    """
    record_incomplete = False

    async def _load() -> Any:
        nonlocal record_incomplete
        try:
            return await metsights_service.get_report(
                record_id=record_id,
                assessment_type_code=type_code,
            )
        except AppError as exc:
            if exc.error_code == "REPORT_NOT_FOUND":
                record_incomplete = True
            raise

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
        operation=_load,
        reraise=False,
    )
    if report_data is not None and report_data:
        return report_data, False
    if report_data is None:
        logger.warning(
            "MetSights get_report failed for record=%s incomplete=%s",
            record_id,
            record_incomplete,
        )
    return None, record_incomplete


async def _push_incomplete_report_modules(
    db: AsyncSession,
    *,
    metsights_service: MetsightsService,
    sync_service: "MetsightsSyncService",
    record_id: str,
    type_code: str,
    user_id: int,
    engagement_id: int,
    instance_id: int,
    details: list[dict[str, Any]],
) -> bool:
    """Push local questionnaire modules that MetSights still marks incomplete.

    A Pro report needs physical measurements, vitals, blood, and diet/lifestyle
    complete. Blood is handled earlier in this job.
    """
    from modules.notifications.push_questionnaires_to_metsights import categories_for_type_code

    required = categories_for_type_code(type_code)
    if not required:
        return False

    incomplete: list[str] = []
    for resource in required:
        data = await metsights_service.get_record_subresource_or_none(
            record_id=record_id,
            resource=resource,
        )
        if not isinstance(data, dict) or not data.get("is_complete", False):
            incomplete.append(resource)
    if not incomplete:
        return False

    result = await sync_service.push_questionnaire_for_instance(
        db,
        assessment_instance_id=instance_id,
        categories=incomplete,
    )
    patched = list(result.get("resources_patched") or [])
    if not result.get("pushed"):
        details.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "action": "skipped",
            "reason": (
                "MetSights record is not complete and questionnaire push did not update "
                + ", ".join(incomplete)
            ),
        })
        return False

    details.append({
        "user_id": user_id,
        "engagement_id": engagement_id,
        "action": "pushed",
        "reason": (
            "pushed "
            + ", ".join(patched or incomplete)
            + " so MetSights can generate the report"
        ),
    })
    return True


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
    from modules.engagements.models import EngagementNotification

    en_sub = (
        select(
            EngagementNotification.engagement_id,
            EngagementNotification.notification_services,
        )
        .where(EngagementNotification.event_code == "reports.bioai_ready")
        .where(EngagementNotification.recipient_type == "user")
        .subquery("en_bioai")
    )

    canonical_ihr = ReportsRepository.canonical_individual_health_report_subquery()
    query = (
        select(
            EngagementParticipant.user_id,
            Engagement.engagement_id,
            AssessmentInstance.metsights_record_id,
            AssessmentPackage.assessment_type_code,
            canonical_ihr.c.reports,
            canonical_ihr.c.report_url,
            en_sub.c.notification_services.label("bioai_report_services"),
            canonical_ihr.c.report_id,
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
            canonical_ihr,
            canonical_ihr.c.assessment_instance_id == AssessmentInstance.assessment_instance_id,
        )
        .outerjoin(
            en_sub,
            en_sub.c.engagement_id == Engagement.engagement_id,
        )
        .where(AssessmentPackage.assessment_type_code.in_(_PRO_BASIC_TYPE_CODES))
    )
    if not ignore_engagement_date:
        from modules.engagements.blood_bookings_repository import current_blood_booking_subquery

        curr_pbb = current_blood_booking_subquery()
        query = query.outerjoin(
            curr_pbb,
            curr_pbb.c.engagement_participant_id
            == EngagementParticipant.engagement_participant_id,
        ).where(curr_pbb.c.collection_date <= today)
    if not all_engagements:
        query = query.where(Engagement.status == "running")
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


async def _try_recover_missing_blood_parameters(
    db: AsyncSession,
    *,
    assessments_service: "AssessmentsService",
    sync_service: "MetsightsSyncService",
    user_id: int,
    engagement_id: int,
    instance_id: int,
    details: list[dict[str, Any]],
) -> bool:
    """Draft blood questionnaire values and push blood-parameters to MetSights.

    Used when MetSights has no blood-parameters row (404) or ``is_complete`` is
    false, so BioAI load can proceed after a successful push.

    Requires an archived diagnostic-partner blood report PDF; internal averages
    only fill missing keys after that report exists.
    """
    if not await has_archived_diagnostic_blood_report(
        db,
        user_id=user_id,
        engagement_id=engagement_id,
    ):
        details.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "action": "skipped",
            "reason": "diagnostic blood report not available yet",
        })
        return False

    try:
        draft_result = await assessments_service.draft_blood_parameters_from_report(
            db,
            user_id=user_id,
            assessment_instance_id=instance_id,
            allow_completed=True,
        )
        await db.commit()
        drafted = int(draft_result.get("responses_drafted") or 0)
        if drafted > 0:
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "drafted",
                "reason": f"drafted {drafted} blood questionnaire responses from report",
            })
    except Exception as exc:
        await db.rollback()
        logger.warning(
            "Blood parameter draft-from-report failed for user=%s instance=%s: %s",
            user_id,
            instance_id,
            exc,
        )
        details.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "action": "skipped",
            "reason": f"blood draft-from-report failed: {str(exc)[:120]}",
        })

    fallback_result: dict[str, Any] = {}
    try:
        fallback_result = await assessments_service.draft_blood_parameter_internal_fallbacks(
            db,
            user_id=user_id,
            assessment_instance_id=instance_id,
        )
        await db.commit()
        fallback_count = int(fallback_result.get("responses_drafted") or 0)
        if fallback_count > 0:
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "drafted",
                "reason": (
                    f"applied {fallback_count} internal average blood "
                    "fallbacks before Metsights push"
                ),
            })
    except Exception as exc:
        await db.rollback()
        logger.warning(
            "Blood parameter fallback draft failed for user=%s instance=%s: %s",
            user_id,
            instance_id,
            exc,
        )
        details.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "action": "skipped",
            "reason": f"blood fallback draft failed: {str(exc)[:120]}",
        })

    category_keys = list(fallback_result.get("category_keys") or [BLOOD_PARAMETER_CATEGORY_KEY])

    pushed_fields = 0
    for category_key in category_keys:
        try:
            push_result = await sync_service._push_category_to_metsights(
                db,
                assessment_instance_id=instance_id,
                user_id=user_id,
                category_key=category_key,
            )
            await db.commit()
        except Exception as exc:
            message = str(getattr(exc, "message", exc))
            not_on_package = "is not available" in message or "not supported" in message
            if not_on_package:
                continue
            try:
                await db.commit()
            except Exception:
                await db.rollback()
            logger.warning(
                "Metsights %s push failed for user=%s instance=%s: %s",
                category_key,
                user_id,
                instance_id,
                exc,
            )
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "failed",
                "reason": f"metsights {category_key} push failed: {message[:100]}",
            })
            if category_key == BLOOD_PARAMETER_CATEGORY_KEY:
                return False
            continue

        fields_count = len(push_result.get("fields_pushed") or [])
        pushed_fields += fields_count
        details.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "action": "pushed",
            "reason": f"pushed {category_key} to Metsights ({fields_count} fields)",
        })
    return pushed_fields > 0


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

            needs_new_bioai = not _bioai_report_data_complete(
                existing_reports, existing_report_url
            )
            if needs_new_bioai and not await has_archived_diagnostic_blood_report(
                db,
                user_id=user_id,
                engagement_id=engagement_id,
            ):
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "action": "skipped",
                    "reason": "diagnostic blood report not available yet",
                })
                continue

            if dry_run:
                complete = _bioai_report_data_complete(existing_reports, existing_report_url)
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
                    needs_blood_recovery = bp_data is None or not bool(
                        bp_data.get("is_complete", False)
                    )
                    if (
                        needs_blood_recovery
                        and assessments_service is not None
                        and sync_service is not None
                    ):
                        try:
                            recovered = await _try_recover_missing_blood_parameters(
                                db,
                                assessments_service=assessments_service,
                                sync_service=sync_service,
                                user_id=user_id,
                                engagement_id=engagement_id,
                                instance_id=instance_id,
                                details=details,
                            )
                            if recovered:
                                bp_data = await tracked_integration_call(
                                    db,
                                    provider="metsights",
                                    api_url=_metsights_blood_parameters_url(record_id=record_id),
                                    engagement_id=engagement_id,
                                    user_id=user_id,
                                    request_payload={"record_id": record_id},
                                    operation=lambda: metsights_service.get_blood_parameters(
                                        record_id=record_id
                                    ),
                                    reraise=False,
                                )
                        except Exception as exc:
                            await db.rollback()
                            logger.warning(
                                "BioAI blood-parameters recovery failed for user=%s instance=%s: %s",
                                user_id,
                                instance_id,
                                exc,
                            )
                            details.append({
                                "user_id": user_id,
                                "engagement_id": engagement_id,
                                "action": "skipped",
                                "reason": f"blood-parameters recovery failed: {str(exc)[:100]}",
                            })
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
                report_url = _report_url_for_bio_ai_registration(existing_report_url)

                if _bioai_report_data_complete(reports, existing_report_url):
                    if not send_notifications:
                        canonical_url = await resolve_canonical_bio_ai_report_url(
                            db,
                            user_id=user_id,
                            engagement_id=engagement_id,
                            assessment_instance_id=instance_id,
                            report_url=(existing_report_url or "").strip() or None,
                        )
                        slug_url = (canonical_url or existing_report_url or "").strip()
                        if is_bio_ai_reports_permanent_url(slug_url):
                            try:
                                refreshed = await regenerate_permanent_bio_ai_report_url(
                                    db,
                                    assessment_instance_id=instance_id,
                                    report_url=slug_url,
                                    engagement_id=engagement_id,
                                    user_id=user_id,
                                )
                                ihr = await _get_or_create_ihr(
                                    db,
                                    ihr_id=ihr_id,
                                    user_id=user_id,
                                    engagement_id=engagement_id,
                                    instance_id=instance_id,
                                )
                                ihr.report_url = refreshed
                                await db.flush()
                                await db.commit()
                                loaded += 1
                                details.append({
                                    "user_id": user_id,
                                    "engagement_id": engagement_id,
                                    "action": "loaded",
                                    "reason": (
                                        "BioAI PDF regenerated at existing bio-ai-reports link"
                                    ),
                                })
                            except Exception as exc:
                                logger.warning(
                                    "BioAI PDF regenerate failed for instance=%s: %s",
                                    instance_id,
                                    exc,
                                )
                                skipped += 1
                                details.append({
                                    "user_id": user_id,
                                    "engagement_id": engagement_id,
                                    "action": "skipped",
                                    "reason": f"BioAI PDF regenerate failed: {str(exc)[:180]}",
                                })
                        else:
                            skipped += 1
                            details.append({
                                "user_id": user_id,
                                "engagement_id": engagement_id,
                                "action": "skipped",
                                "reason": (
                                    "BioAI reports JSON present but PDF URL is not a "
                                    "bio-ai-reports slug"
                                ),
                            })
                        continue

                if not _bioai_report_data_complete(reports, existing_report_url):
                    fetched_reports = None
                    fetched_url = None

                    if reports is None:
                        fetched_reports, record_incomplete = await _fetch_metsights_report_json(
                            db,
                            metsights_service=metsights_service,
                            record_id=record_id,
                            type_code=type_code,
                            engagement_id=engagement_id,
                            user_id=user_id,
                        )
                        if (
                            fetched_reports is None
                            and record_incomplete
                            and sync_service is not None
                        ):
                            try:
                                pushed_modules = await _push_incomplete_report_modules(
                                    db,
                                    metsights_service=metsights_service,
                                    sync_service=sync_service,
                                    record_id=record_id,
                                    type_code=type_code,
                                    user_id=user_id,
                                    engagement_id=engagement_id,
                                    instance_id=instance_id,
                                    details=details,
                                )
                            except Exception as exc:
                                await db.rollback()
                                logger.warning(
                                    "BioAI record-module push failed for user=%s instance=%s: %s",
                                    user_id,
                                    instance_id,
                                    exc,
                                )
                                pushed_modules = False
                            if pushed_modules:
                                fetched_reports, _record_incomplete = await _fetch_metsights_report_json(
                                    db,
                                    metsights_service=metsights_service,
                                    record_id=record_id,
                                    type_code=type_code,
                                    engagement_id=engagement_id,
                                    user_id=user_id,
                                )

                    if fetched_reports is not None:
                        ihr_for_pdf = await _get_or_create_ihr(
                            db,
                            ihr_id=ihr_id,
                            user_id=user_id,
                            engagement_id=engagement_id,
                            instance_id=instance_id,
                        )
                        ihr_for_pdf.reports = fetched_reports
                        reports = fetched_reports
                        await db.flush()

                    if report_url is None:
                        try:
                            fetched_url = await register_permanent_bio_ai_report_url(
                                db,
                                assessment_instance_id=instance_id,
                                engagement_id=engagement_id,
                                user_id=user_id,
                            )
                            if (
                                fetched_url
                                and is_bio_ai_reports_permanent_url(fetched_url)
                                and is_metsights_hosted_report_url(
                                    (existing_report_url or "").strip()
                                )
                            ):
                                fetched_url = await regenerate_permanent_bio_ai_report_url(
                                    db,
                                    assessment_instance_id=instance_id,
                                    report_url=fetched_url.strip(),
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
                        and not is_metsights_essentials(type_code=type_code)
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
                                    fetched_reports, _record_incomplete = await _fetch_metsights_report_json(
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
                            skipped += 1
                            details.append({
                                "user_id": user_id,
                                "engagement_id": engagement_id,
                                "action": "skipped",
                                "reason": f"vitals BP recovery failed: {str(exc)[:100]}",
                            })
                            continue

                    if fetched_reports is None and fetched_url is None:
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped", "reason": "no report data returned from MetSights or bio-ai-reports",
                        })
                        continue

                    ihr = await _get_or_create_ihr(
                        db,
                        ihr_id=ihr_id,
                        user_id=user_id,
                        engagement_id=engagement_id,
                        instance_id=instance_id,
                    )

                    if fetched_reports is not None:
                        ihr.reports = fetched_reports
                        reports = fetched_reports
                    if fetched_url is not None:
                        ihr.report_url = fetched_url
                        report_url = fetched_url
                    await db.flush()
                    await db.commit()

                    if not _bioai_report_data_complete(reports, report_url):
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

                if not _bioai_report_data_complete(reports, report_url):
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

                    notified += await send_report_notifications(
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
