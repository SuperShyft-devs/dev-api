"""Load blood reports from Healthians and send notifications.

For participants in running engagements with MetSights Pro/Basic assessments
where today >= engagement_date:
1. Call getBookingReport first; on failure skip (no digital-value fetch).
2. If blood_parameters_verified_at matches API verified_at, skip getBookingDigitalValue
   but still refresh blood_parameters_full_report / diagnostic_report_url metadata when
   those fields changed (partial reports keep diagnostic_report_url null; full reports
   store an archived supershyft URL only). Full reports without an archived PDF URL
   always re-enter the load path so archival can retry.
3. After a successful blood load, draft blood-parameter questionnaire responses.
4. After blood values are available, check whether the engagement primary
   package's metsights ``blood-parameters`` / ``advanced-blood-parameters``
   categories are submitted. Push (or retry) any that are not, unless BioAI
   is already generated. If advanced needs push but local blood-parameters is
   marked submitted while Metsights has no parent blood-parameters row, re-push
   blood-parameters first (Metsights rejects advanced otherwise). For *full*
   reports, fill missing mandatory answers from internal average fallbacks
   before push; on push failure, apply fallbacks again and retry once. Partial
   reports never receive average fallbacks.
5. Notifications only when full_report is true, and only when both blood fields
   are present, using engagement_notifications for blood_report_ready event
   (skipping services already sent).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.transaction import release_request_transaction
from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    BLOOD_PARAMETER_CATEGORY_KEY,
)
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService, _PACKAGE_BLOOD_CATEGORY_KEYS
from modules.questionnaire.repository import QuestionnaireRepository
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.diagnostics.repository import DiagnosticsRepository
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.diagnostics.healthians import client as healthians_client
from modules.engagement_notifications.service_config import (
    NotificationServiceConfigItem,
    normalize_notification_services,
)
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.blood_parameters_normalizer import build_grouped_from_healthians
from modules.reports.blood_report_archival import (
    diagnostic_report_url_to_persist,
    is_archived_blood_report_url,
    resolve_persistable_diagnostic_report_url,
)
from modules.reports.blood_report_resolver import _match_customer_by_name
from modules.reports.blood_parameters_schemas import (
    booking_id_from_fetch_collections,
    has_usable_provider_blood_parameters,
    provider_code_from_field,
)
from modules.reports.healthians_booking_resolver import (
    HealthiansBookingSource,
    try_participant_booking_id,
)
from modules.reports.healthians_report_fields import (
    match_booking_report_entry,
    parse_booking_report_entry,
    verified_at_unchanged,
)
from modules.reports.models import IndividualHealthReport
from modules.users.models import User

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, int, int, int, int], None]

_METSIGHTS_PRO_BASIC_TYPE_CODES = {"1", "2"}
_BLOOD_METSIGHTS_CATEGORY_ORDER = (
    BLOOD_PARAMETER_CATEGORY_KEY,
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
)
_BLOOD_METSIGHTS_CATEGORY_KEYS = frozenset(_BLOOD_METSIGHTS_CATEGORY_ORDER)


def _healthians_url(path: str) -> str:
    return f"{settings.HEALTHIANS_BASE_URL.rstrip('/')}/{path.lstrip('/')}"


def _metsights_fetch_collections_url(*, record_id: str) -> str:
    return f"{settings.METSIGHTS_BASE_URL.rstrip('/')}/records/{record_id}/fetch-collections/"


def _metsights_report_url(*, record_id: str, assessment_type_code: str) -> str:
    return f"{settings.METSIGHTS_BASE_URL.rstrip('/')}/reports/{record_id}/"


def _blood_report_data_complete(blood_parameters: Any, diagnostic_report_url: Any) -> bool:
    """True when blood values exist and the PDF URL is a permanent archived link."""
    return (
        has_usable_provider_blood_parameters(blood_parameters)
        and is_archived_blood_report_url(
            str(diagnostic_report_url).strip() if diagnostic_report_url is not None else None
        )
    )


async def _blood_metsights_category_keys_for_package(
    db: AsyncSession,
    *,
    package_id: int | None,
    package_code: str,
) -> list[str]:
    """Return metsights blood category keys linked to the primary assessment package."""
    linked: set[str] = set()
    if package_id is not None:
        assessments_repo = AssessmentsRepository()
        questionnaire_repo = QuestionnaireRepository()
        links = await assessments_repo.list_package_categories(db, package_id=int(package_id))
        for link in links:
            category = await questionnaire_repo.get_category_by_id(db, int(link.category_id))
            if category is None:
                continue
            if (category.category_of or "").strip().lower() != "metsights":
                continue
            key = (category.category_key or "").strip()
            if key in _BLOOD_METSIGHTS_CATEGORY_KEYS:
                linked.add(key)
    ordered = [key for key in _BLOOD_METSIGHTS_CATEGORY_ORDER if key in linked]
    if ordered:
        return ordered
    return list(_PACKAGE_BLOOD_CATEGORY_KEYS.get(package_code, ()))


async def _is_metsights_blood_category_submitted(
    db: AsyncSession,
    *,
    assessment_instance_id: int,
    category_key: str,
) -> bool:
    questionnaire_repo = QuestionnaireRepository()
    category = await questionnaire_repo.get_category_by_key_and_category_of(
        db,
        category_key=category_key,
        category_of="metsights",
    )
    if category is None:
        return False
    progress = await AssessmentsRepository().get_category_progress(
        db,
        assessment_instance_id=assessment_instance_id,
        category_id=int(category.category_id),
    )
    return bool(progress is not None and progress.is_submitted)


async def _sync_unsubmitted_blood_to_metsights(
    db: AsyncSession,
    *,
    metsights_service: MetsightsService,
    assessments_service: AssessmentsService,
    sync_service: MetsightsSyncService,
    user_id: int,
    engagement_id: int,
    instance_id: int,
    record_id: str,
    assessment_type_code: str,
    package_id: int | None,
    package_code: str,
    blood_loaded_this_run: bool,
    already_drafted: bool,
    is_full_report: bool,
    details: list[dict[str, Any]],
) -> None:
    """Push primary-package metsights blood categories that are not yet submitted.

    Fresh blood loads re-push even if previously submitted, because values changed.
    Later cron runs retry only unsubmitted categories.

    When ``is_full_report`` is true, missing mandatory blood answers are filled
    from internal average fallbacks before the first push attempt. On push
    failure, fallbacks are applied again and the push is retried once. Partial
    reports never receive average fallbacks.
    """
    category_keys = await _blood_metsights_category_keys_for_package(
        db,
        package_id=package_id,
        package_code=package_code,
    )
    if not category_keys:
        return

    to_push: list[str] = []
    skipped_as_submitted: list[str] = []
    for category_key in category_keys:
        submitted = await _is_metsights_blood_category_submitted(
            db,
            assessment_instance_id=instance_id,
            category_key=category_key,
        )
        if submitted and not blood_loaded_this_run:
            skipped_as_submitted.append(category_key)
            continue
        to_push.append(category_key)

    # Metsights rejects advanced-blood-parameters with
    # "Blood parameter does not exist for this record" unless the parent
    # blood-parameters sub-resource exists. Local is_submitted can be stale
    # (marked submitted while Metsights never got / lost the parent row).
    if (
        ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY in to_push
        and BLOOD_PARAMETER_CATEGORY_KEY in skipped_as_submitted
    ):
        parent = await metsights_service.get_record_subresource_or_none(
            record_id=record_id,
            resource=BLOOD_PARAMETER_CATEGORY_KEY,
        )
        if parent is None:
            skipped_as_submitted.remove(BLOOD_PARAMETER_CATEGORY_KEY)
            to_push.insert(0, BLOOD_PARAMETER_CATEGORY_KEY)
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "skipped",
                "reason": (
                    "blood-parameters marked submitted locally but missing on "
                    "Metsights; re-pushing before advanced-blood-parameters"
                ),
            })

    for category_key in skipped_as_submitted:
        details.append({
            "user_id": user_id,
            "engagement_id": engagement_id,
            "action": "skipped",
            "reason": f"{category_key} already submitted to Metsights",
        })

    if not to_push:
        return

    report_exists = await tracked_integration_call(
        db,
        provider="metsights",
        api_url=_metsights_report_url(
            record_id=record_id,
            assessment_type_code=assessment_type_code,
        ),
        engagement_id=engagement_id,
        user_id=user_id,
        request_payload={
            "record_id": record_id,
            "assessment_type_code": assessment_type_code,
            "check": "bioai_report_generated",
        },
        operation=lambda: metsights_service.is_bioai_report_generated(
            record_id=record_id,
            assessment_type_code=assessment_type_code,
        ),
        reraise=False,
    )
    if report_exists is None:
        logger.warning(
            "BioAI report check failed for user=%s record=%s",
            user_id, record_id,
        )
        details.append({
            "user_id": user_id, "engagement_id": engagement_id,
            "action": "skipped",
            "reason": "skipped metsights push: report check failed",
        })
        return

    if report_exists:
        details.append({
            "user_id": user_id, "engagement_id": engagement_id,
            "action": "skipped",
            "reason": "skipped metsights push: BioAI report already generated",
        })
        return

    if not already_drafted:
        try:
            draft_result = await assessments_service.draft_blood_parameters_from_report(
                db,
                user_id=user_id,
                assessment_instance_id=instance_id,
                allow_completed=True,
            )
            await db.commit()
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "drafted",
                "reason": (
                    f"drafted {draft_result.get('responses_drafted', 0)} "
                    "blood questionnaire responses"
                ),
            })
        except Exception as exc:
            await db.rollback()
            logger.warning(
                "Blood parameter draft failed for user=%s instance=%s: %s",
                user_id, instance_id, exc,
            )
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "skipped",
                "reason": f"blood draft failed: {str(exc)[:120]}",
            })

    if is_full_report:
        try:
            proactive_result = await assessments_service.draft_blood_parameter_internal_fallbacks(
                db,
                user_id=user_id,
                assessment_instance_id=instance_id,
                category_keys=to_push,
            )
            await db.commit()
            proactive_count = int(proactive_result.get("responses_drafted") or 0)
            if proactive_count > 0:
                details.append({
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "action": "drafted",
                    "reason": (
                        f"applied {proactive_count} internal average blood "
                        "fallbacks before Metsights push (full report)"
                    ),
                })
        except Exception as fallback_exc:
            await db.rollback()
            logger.warning(
                "Proactive blood average fallback draft failed for user=%s instance=%s: %s",
                user_id,
                instance_id,
                fallback_exc,
            )
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "skipped",
                "reason": (
                    "proactive average blood fallback draft failed: "
                    f"{str(fallback_exc)[:100]}"
                ),
            })

    for category_key in to_push:
        try:
            push_result = await sync_service._push_category_to_metsights(
                db,
                assessment_instance_id=instance_id,
                user_id=user_id,
                category_key=category_key,
            )
            await db.commit()
            fields_count = len(push_result.get("fields_pushed") or [])
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "pushed",
                "reason": (
                    f"pushed {category_key} to Metsights "
                    f"({fields_count} fields)"
                ),
            })
        except Exception as exc:
            try:
                await db.commit()
            except Exception:
                await db.rollback()
            push_error = getattr(exc, "message", None) or str(exc)
            logger.warning(
                "Metsights blood push failed for user=%s category=%s: %s",
                user_id, category_key, exc,
            )
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "failed",
                "reason": (
                    f"metsights push failed for {category_key}: "
                    f"{str(push_error)[:100]}"
                ),
            })

            if not is_full_report:
                continue

            try:
                fallback_result = await assessments_service.draft_blood_parameter_internal_fallbacks(
                    db,
                    user_id=user_id,
                    assessment_instance_id=instance_id,
                    category_keys=[category_key],
                )
                await db.commit()
                fallback_count = int(fallback_result.get("responses_drafted") or 0)
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "drafted",
                    "reason": (
                        f"applied {fallback_count} internal average blood "
                        f"fallbacks for {category_key} after push failure"
                    ),
                })
            except Exception as fallback_exc:
                await db.rollback()
                logger.warning(
                    "Blood average fallback draft failed for user=%s category=%s: %s",
                    user_id, category_key, fallback_exc,
                )
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "skipped",
                    "reason": (
                        f"average blood fallback draft failed for {category_key}: "
                        f"{str(fallback_exc)[:100]}"
                    ),
                })
                continue

            try:
                push_result = await sync_service._push_category_to_metsights(
                    db,
                    assessment_instance_id=instance_id,
                    user_id=user_id,
                    category_key=category_key,
                )
                await db.commit()
                fields_count = len(push_result.get("fields_pushed") or [])
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "pushed",
                    "reason": (
                        f"pushed {category_key} to Metsights after average "
                        f"fallbacks ({fields_count} fields)"
                    ),
                })
            except Exception as retry_exc:
                try:
                    await db.commit()
                except Exception:
                    await db.rollback()
                retry_error = getattr(retry_exc, "message", None) or str(retry_exc)
                logger.warning(
                    "Metsights blood push retry failed for user=%s category=%s: %s",
                    user_id, category_key, retry_exc,
                )
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "failed",
                    "reason": (
                        f"metsights push retry failed for {category_key} "
                        f"after average fallbacks: {str(retry_error)[:100]}"
                    ),
                })


async def _group_provider_blood(
    db: AsyncSession,
    raw_customer: dict[str, Any],
    *,
    diagnostic_package_id: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from modules.diagnostics.service import DiagnosticsService

    package_tests = await DiagnosticsService(repository=DiagnosticsRepository()).get_package_tests(
        db=db,
        package_id=diagnostic_package_id,
    )
    return build_grouped_from_healthians(raw_customer, package_groups=package_tests.groups)


async def _get_eligible_participants(
    db: AsyncSession,
    today: date,
    *,
    all_engagements: bool = False,
    engagement_id: int | None = None,
    user_ids: set[int] | None = None,
    ignore_engagement_date: bool = False,
) -> list[tuple]:
    """Return participants needing blood report loading.

    Returns tuples of:
    (user_id, engagement_id, record_id, first_name, last_name,
     blood_parameters, diagnostic_report_url, blood_report_services, ihr_id, instance_id,
     package_id, diagnostic_package_id, participant_booking_id, diagnostic_provider,
     package_code, assessment_type_code, blood_parameters_full_report,
     blood_parameters_verified_at)
    """
    from modules.engagements.models import AutoNotificationEvent, EngagementNotification

    en_sub = (
        select(
            EngagementNotification.engagement_id,
            EngagementNotification.notification_services,
        )
        .join(AutoNotificationEvent, AutoNotificationEvent.id == EngagementNotification.notification_event_id)
        .where(AutoNotificationEvent.event_code == "blood_report_ready")
        .subquery("en_blood")
    )

    query = (
        select(
            EngagementParticipant.user_id,
            Engagement.engagement_id,
            AssessmentInstance.metsights_record_id,
            User.first_name,
            User.last_name,
            IndividualHealthReport.blood_parameters,
            IndividualHealthReport.diagnostic_report_url,
            en_sub.c.notification_services.label("blood_report_services"),
            IndividualHealthReport.report_id,
            AssessmentInstance.assessment_instance_id,
            AssessmentInstance.package_id,
            Engagement.diagnostic_package_id,
            EngagementParticipant.booking_id,
            DiagnosticPackage.diagnostic_provider,
            AssessmentPackage.package_code,
            AssessmentPackage.assessment_type_code,
            IndividualHealthReport.blood_parameters_full_report,
            IndividualHealthReport.blood_parameters_verified_at,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .outerjoin(
            DiagnosticPackage,
            DiagnosticPackage.diagnostic_package_id == Engagement.diagnostic_package_id,
        )
        .join(User, User.user_id == EngagementParticipant.user_id)
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
        .where(AssessmentPackage.assessment_type_code.in_(_METSIGHTS_PRO_BASIC_TYPE_CODES))
        .where(
            or_(
                Engagement.assessment_package_id.is_(None),
                AssessmentInstance.package_id == Engagement.assessment_package_id,
            )
        )
        .where(AssessmentInstance.metsights_record_id.isnot(None))
        .where(AssessmentInstance.metsights_record_id != "")
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
    result = await db.execute(query)
    return result.all()


async def _get_or_create_ihr(
    db: AsyncSession,
    *,
    ihr_id: int | None,
    user_id: int,
    engagement_id: int,
    instance_id: int,
) -> IndividualHealthReport:
    ihr = None
    if ihr_id:
        ihr_result = await db.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.report_id == ihr_id)
        )
        ihr = ihr_result.scalar_one_or_none()

    if ihr is None:
        ihr = IndividualHealthReport(
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=instance_id,
        )
        db.add(ihr)
    return ihr


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


async def load_blood_reports(
    db: AsyncSession,
    *,
    metsights_service: MetsightsService,
    notifications_service: NotificationsService,
    assessments_service: AssessmentsService,
    sync_service: MetsightsSyncService,
    as_of: date | None = None,
    dry_run: bool = False,
    all_engagements: bool = False,
    engagement_id: int | None = None,
    user_ids: set[int] | None = None,
    send_notifications: bool = True,
    ignore_engagement_date: bool = False,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Load blood reports and notify participants.

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
    await release_request_transaction(db)
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
                user_id, engagement_id, record_id,
                first_name, last_name,
                blood_params, diag_url,
                blood_report_services, ihr_id, instance_id,
                package_id, diagnostic_package_id, participant_booking_id, diagnostic_provider,
                package_code, assessment_type_code,
                stored_full_report, stored_verified_at,
            ) = row

            record_id = (record_id or "").strip()
            package_code = (package_code or "").strip()
            assessment_type_code = (assessment_type_code or "").strip()

            if not record_id:
                skipped += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "skipped", "reason": "no metsights_record_id",
                })
                continue

            if dry_run:
                complete = _blood_report_data_complete(blood_params, diag_url)
                dry_run_reasons = [
                    "would_fetch_booking_report",
                    "would_fetch_blood_parameters_if_verified_at_changed",
                ]
                if package_code in _PACKAGE_BLOOD_CATEGORY_KEYS:
                    dry_run_reasons.append("would_draft_blood_questionnaires")
                    dry_run_reasons.append("would_retry_unsubmitted_blood_metsights_categories")
                    try:
                        await release_request_transaction(db)
                        report_exists = await metsights_service.is_bioai_report_generated(
                            record_id=record_id,
                            assessment_type_code=assessment_type_code,
                        )
                        if report_exists:
                            dry_run_reasons.append("would_skip_metsights_push_report_generated")
                        else:
                            dry_run_reasons.append("would_push_blood_to_metsights")
                    except Exception as exc:
                        dry_run_reasons.append(f"would_skip_metsights_push_check_failed: {exc}")
                dry_run_reasons.append(
                    "blood_report_complete" if complete else "blood_report_incomplete"
                )
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "dry_run",
                    "reason": ", ".join(dry_run_reasons),
                })
                continue

            try:
                blood_parameters = blood_params
                diagnostic_report_url = diag_url
                is_full_report = bool(stored_full_report) if stored_full_report is not None else False

                await release_request_transaction(db)

                reference_id = try_participant_booking_id(participant_booking_id, diagnostic_provider)
                booking_source = HealthiansBookingSource.PARTICIPANT if reference_id else None

                if not reference_id:
                    try:
                        collection_data = await tracked_integration_call(
                            db,
                            provider="metsights",
                            api_url=_metsights_fetch_collections_url(record_id=record_id),
                            engagement_id=engagement_id,
                            user_id=user_id,
                            request_payload={"record_id": record_id},
                            operation=lambda: metsights_service.get_fetch_collections(record_id=record_id),
                        )
                    except Exception:
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped", "reason": "fetch-collections not available for this record",
                        })
                        continue

                    if collection_data is None:
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped", "reason": "fetch-collections not available for this record",
                        })
                        continue

                    reference_id = booking_id_from_fetch_collections(collection_data)
                    provider_code = provider_code_from_field(collection_data.get("provider"))

                    if not reference_id:
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped", "reason": "no booking id from MetSights collections",
                        })
                        continue

                    if provider_code.lower() != "healthians":
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped",
                            "reason": f"provider code is '{provider_code or 'unknown'}', not Healthians",
                        })
                        continue
                    booking_source = HealthiansBookingSource.METSIGHTS

                access_token = await tracked_integration_call(
                    db,
                    provider="healthians",
                    api_url=_healthians_url("toast4health/getAccessToken"),
                    engagement_id=engagement_id,
                    user_id=user_id,
                    request_payload=None,
                    operation=healthians_client.get_access_token,
                )
                if access_token is None:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "Healthians authentication failed",
                    })
                    continue

                source_label = (
                    "participant booking_id"
                    if booking_source == HealthiansBookingSource.PARTICIPANT
                    else "Metsights reference_id"
                )

                # --- getBookingReport first ---
                report_data = await tracked_integration_call(
                    db,
                    provider="healthians",
                    api_url=_healthians_url("toast4health/getBookingReport"),
                    engagement_id=engagement_id,
                    user_id=user_id,
                    request_payload={"booking_id": str(reference_id)},
                    operation=lambda: healthians_client.get_booking_report(
                        access_token, reference_id
                    ),
                    reraise=False,
                )
                if report_data is None:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "Healthians getBookingReport failed",
                    })
                    continue

                report_list = report_data.get("data")
                if not isinstance(report_list, list) or not report_list:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "Healthians getBookingReport returned no report data",
                    })
                    continue

                matched_report = match_booking_report_entry(
                    report_list,
                    first_name=first_name or "",
                    last_name=last_name or "",
                )
                if matched_report is None:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "Healthians getBookingReport: no matching customer report",
                    })
                    continue

                fetched_diag_url, api_full_report, api_verified_at = parse_booking_report_entry(
                    matched_report
                )
                if fetched_diag_url is None and api_full_report is not True:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "Healthians getBookingReport: no report_url in matched entry",
                    })
                    continue

                if api_full_report is not None:
                    is_full_report = api_full_report

                persistable_url = await resolve_persistable_diagnostic_report_url(
                    fetched_diag_url or "",
                    is_full_report=is_full_report,
                    existing_url=diag_url,
                    assessment_instance_id=instance_id,
                )
                if is_full_report and persistable_url is None:
                    details.append({
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "blood report PDF archival failed",
                    })

                stored_diag_url = (diag_url or "").strip() if diag_url else ""
                url_to_store = diagnostic_report_url_to_persist(
                    is_full_report=is_full_report,
                    persistable_url=persistable_url,
                    existing_url=stored_diag_url,
                )
                # Always write the resolved value (including None) so Healthians/S3
                # signed URLs are never left in diagnostic_report_url.
                url_changed = (url_to_store or "") != stored_diag_url

                # Full reports also require a permanent archived PDF URL. If the
                # archived link is missing (or still a transient Healthians/S3 URL),
                # re-enter the load path so archival can retry even when verified_at
                # matches.
                has_archived_pdf = is_archived_blood_report_url(
                    str(diag_url).strip() if diag_url is not None else None
                )
                skip_digital_reload = (
                    verified_at_unchanged(stored_verified_at, api_verified_at)
                    and has_usable_provider_blood_parameters(blood_params)
                    and (not is_full_report or has_archived_pdf)
                )
                blood_loaded_this_run = False
                blood_drafted_this_run = False

                metadata_needs_update = (
                    (api_full_report is not None and bool(stored_full_report) != api_full_report)
                    or url_changed
                )

                if skip_digital_reload:
                    if metadata_needs_update:
                        ihr = await _get_or_create_ihr(
                            db,
                            ihr_id=ihr_id,
                            user_id=user_id,
                            engagement_id=engagement_id,
                            instance_id=instance_id,
                        )
                        ihr.diagnostic_report_url = url_to_store
                        diagnostic_report_url = url_to_store
                        if api_full_report is not None:
                            ihr.blood_parameters_full_report = api_full_report
                        if api_verified_at is not None:
                            ihr.blood_parameters_verified_at = api_verified_at
                        if ihr_id is None:
                            await db.flush()
                            ihr_id = ihr.report_id
                        await db.flush()
                        await db.commit()
                        loaded += 1
                        details.append({
                            "user_id": user_id,
                            "engagement_id": engagement_id,
                            "action": "loaded",
                            "reason": (
                                "report metadata refreshed from Healthians "
                                "(verified_at unchanged; skipped digital reload)"
                            ),
                        })
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "verified_at unchanged; skipped blood reload",
                    })
                else:
                    ihr = await _get_or_create_ihr(
                        db,
                        ihr_id=ihr_id,
                        user_id=user_id,
                        engagement_id=engagement_id,
                        instance_id=instance_id,
                    )
                    ihr.diagnostic_report_url = url_to_store
                    diagnostic_report_url = url_to_store
                    ihr.blood_parameters_full_report = api_full_report
                    ihr.blood_parameters_verified_at = api_verified_at
                    if ihr_id is None:
                        await db.flush()
                        ihr_id = ihr.report_id
                    await db.flush()
                    await db.commit()
                    loaded += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "loaded",
                        "reason": (
                            f"diagnostic_report_url and verified fields refreshed "
                            f"from Healthians via {source_label}"
                        ),
                    })

                    # --- getBookingDigitalValue only when verified_at changed ---
                    fetched_blood = None
                    digital_value = await tracked_integration_call(
                        db,
                        provider="healthians",
                        api_url=_healthians_url("toast4health/getBookingDigitalValue"),
                        engagement_id=engagement_id,
                        user_id=user_id,
                        request_payload={"booking_id": str(reference_id)},
                        operation=lambda: healthians_client.get_booking_digital_value(
                            access_token, reference_id
                        ),
                        reraise=False,
                    )
                    if digital_value is not None:
                        data_list = digital_value.get("data")
                        if isinstance(data_list, list) and data_list:
                            matched_entry = _match_customer_by_name(
                                data_list, first_name or "", last_name or ""
                            )
                            if matched_entry:
                                fetched_blood = matched_entry
                    else:
                        logger.warning(
                            "Healthians getBookingDigitalValue failed for user=%s booking=%s",
                            user_id, reference_id,
                        )

                    if fetched_blood is not None:
                        if diagnostic_package_id is None:
                            details.append({
                                "user_id": user_id, "engagement_id": engagement_id,
                                "action": "skipped",
                                "reason": "engagement has no diagnostic package for blood parameters",
                            })
                        else:
                            ihr = await _get_or_create_ihr(
                                db,
                                ihr_id=ihr_id,
                                user_id=user_id,
                                engagement_id=engagement_id,
                                instance_id=instance_id,
                            )
                            grouped, raw = await _group_provider_blood(
                                db,
                                fetched_blood,
                                diagnostic_package_id=int(diagnostic_package_id),
                            )
                            ihr.blood_parameters = grouped
                            ihr.blood_report_raw = raw
                            blood_parameters = grouped
                            blood_loaded_this_run = True
                            await db.flush()
                            await db.commit()
                            details.append({
                                "user_id": user_id, "engagement_id": engagement_id,
                                "action": "loaded",
                                "reason": f"blood data fetched from Healthians via {source_label}",
                            })

                            try:
                                draft_result = await assessments_service.draft_blood_parameters_from_report(
                                    db,
                                    user_id=user_id,
                                    assessment_instance_id=instance_id,
                                    allow_completed=True,
                                )
                                await db.commit()
                                blood_drafted_this_run = True
                                details.append({
                                    "user_id": user_id, "engagement_id": engagement_id,
                                    "action": "drafted",
                                    "reason": (
                                        f"drafted {draft_result.get('responses_drafted', 0)} "
                                        "blood questionnaire responses"
                                    ),
                                })
                            except Exception as exc:
                                await db.rollback()
                                logger.warning(
                                    "Blood parameter draft failed for user=%s instance=%s: %s",
                                    user_id, instance_id, exc,
                                )
                                details.append({
                                    "user_id": user_id, "engagement_id": engagement_id,
                                    "action": "skipped",
                                    "reason": f"blood draft failed: {str(exc)[:120]}",
                                })

                if has_usable_provider_blood_parameters(blood_parameters):
                    await _sync_unsubmitted_blood_to_metsights(
                        db,
                        metsights_service=metsights_service,
                        assessments_service=assessments_service,
                        sync_service=sync_service,
                        user_id=user_id,
                        engagement_id=engagement_id,
                        instance_id=instance_id,
                        record_id=record_id,
                        assessment_type_code=assessment_type_code,
                        package_id=package_id,
                        package_code=package_code,
                        blood_loaded_this_run=blood_loaded_this_run,
                        already_drafted=blood_drafted_this_run,
                        is_full_report=is_full_report,
                        details=details,
                    )

                if send_notifications:
                    if not is_full_report:
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped",
                            "reason": "full_report is 0; notification not sent",
                        })
                        continue

                    if not _blood_report_data_complete(blood_parameters, diagnostic_report_url):
                        skipped += 1
                        incomplete_reason = (
                            "blood report data incomplete "
                            "(missing blood_parameters or diagnostic_report_url)"
                        )
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped",
                            "reason": incomplete_reason,
                        })
                        continue

                    service_configs = normalize_notification_services(blood_report_services)
                    if not service_configs:
                        skipped += 1
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped", "reason": "blood reports ready, no notification keys configured",
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
                    "load_blood_reports failed: user=%s engagement=%s: %s",
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
