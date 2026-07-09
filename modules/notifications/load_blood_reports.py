"""Load blood reports from Healthians and send notifications.

For participants in running engagements with MetSights Pro/Basic assessments
where today >= engagement_date:
1. Always refresh blood_parameters from Healthians and upsert individual_health_report.
2. After a successful blood load, draft blood-parameter questionnaire responses.
3. Push blood parameters to Metsights when BioAI report is not yet generated.
4. Always refresh individual_health_report.diagnostic_report_url from Healthians.
5. When both fields are present, send notifications using
   engagement.blood_report_notification (skipping services already sent).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.assessments.service import AssessmentsService, _PACKAGE_BLOOD_CATEGORY_KEYS
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.diagnostics.repository import DiagnosticsRepository
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.diagnostics.healthians import client as healthians_client
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.blood_parameters_normalizer import build_grouped_from_healthians
from modules.reports.blood_parameters_schemas import (
    has_usable_provider_blood_parameters,
    provider_code_from_field,
)
from modules.reports.healthians_booking_resolver import (
    HealthiansBookingSource,
    try_participant_booking_id,
)
from modules.reports.models import IndividualHealthReport
from modules.users.models import User

logger = logging.getLogger(__name__)

_METSIGHTS_PRO_BASIC_TYPE_CODES = {"1", "2"}


def _healthians_url(path: str) -> str:
    return f"{settings.HEALTHIANS_BASE_URL.rstrip('/')}/{path.lstrip('/')}"


def _metsights_fetch_collections_url(*, record_id: str) -> str:
    return f"{settings.METSIGHTS_BASE_URL.rstrip('/')}/records/{record_id}/fetch-collections/"


def _metsights_report_url(*, record_id: str, assessment_type_code: str) -> str:
    return f"{settings.METSIGHTS_BASE_URL.rstrip('/')}/reports/{record_id}/"


def _blood_report_data_complete(blood_parameters: Any, diagnostic_report_url: Any) -> bool:
    return (
        has_usable_provider_blood_parameters(blood_parameters)
        and diagnostic_report_url is not None
    )


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


def _match_customer_by_name(
    data_list: list[Any],
    first_name: str,
    last_name: str,
) -> dict[str, Any] | None:
    """Find the customer entry whose name matches the user (case-insensitive, tokenised)."""
    target_full = f"{first_name} {last_name}".strip().lower()
    target_tokens = set(target_full.split())

    best: dict[str, Any] | None = None
    best_score = 0

    for entry in data_list:
        if not isinstance(entry, dict):
            continue
        customer_name = str(entry.get("customer_name") or "").strip().lower()
        if not customer_name:
            continue
        if customer_name == target_full:
            return entry
        entry_tokens = set(customer_name.split())
        overlap = len(target_tokens & entry_tokens)
        if overlap > best_score:
            best_score = overlap
            best = entry

    if best is not None and best_score >= 1:
        return best
    return data_list[0] if data_list else None


async def _get_eligible_participants(
    db: AsyncSession,
    today: date,
) -> list[tuple]:
    """Return participants needing blood report loading.

    Returns tuples of:
    (user_id, engagement_id, record_id, first_name, last_name,
     blood_parameters, diagnostic_report_url, blood_report_notification, ihr_id, instance_id,
     diagnostic_package_id, participant_booking_id, diagnostic_provider,
     package_code, assessment_type_code)
    """
    query = (
        select(
            EngagementParticipant.user_id,
            Engagement.engagement_id,
            AssessmentInstance.metsights_record_id,
            User.first_name,
            User.last_name,
            IndividualHealthReport.blood_parameters,
            IndividualHealthReport.diagnostic_report_url,
            Engagement.blood_report_notification,
            IndividualHealthReport.report_id,
            AssessmentInstance.assessment_instance_id,
            Engagement.diagnostic_package_id,
            EngagementParticipant.booking_id,
            DiagnosticPackage.diagnostic_provider,
            AssessmentPackage.package_code,
            AssessmentPackage.assessment_type_code,
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
        .where(Engagement.status.ilike("running"))
        .where(EngagementParticipant.engagement_date <= today)
        .where(AssessmentPackage.assessment_type_code.in_(_METSIGHTS_PRO_BASIC_TYPE_CODES))
        .where(AssessmentInstance.metsights_record_id.isnot(None))
        .where(AssessmentInstance.metsights_record_id != "")
    )
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


async def load_blood_reports(
    db: AsyncSession,
    *,
    metsights_service: MetsightsService,
    notifications_service: NotificationsService,
    assessments_service: AssessmentsService,
    sync_service: MetsightsSyncService,
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
            user_id, engagement_id, record_id,
            first_name, last_name,
            blood_params, diag_url,
            blood_report_notification, ihr_id, instance_id,
            diagnostic_package_id, participant_booking_id, diagnostic_provider,
            package_code, assessment_type_code,
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
                "would_fetch_blood_parameters",
                "would_refresh_diagnostic_report_url",
            ]
            if package_code in _PACKAGE_BLOOD_CATEGORY_KEYS:
                dry_run_reasons.append("would_draft_blood_questionnaires")
                try:
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

                reference_id = str(collection_data.get("reference_id") or "").strip()
                provider_code = provider_code_from_field(collection_data.get("provider"))

                if not reference_id:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "no reference_id from MetSights collections",
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

            fetched_blood = None
            blood_loaded_this_run = False

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
                    loaded += 1
                    source_label = (
                        "participant booking_id"
                        if booking_source == HealthiansBookingSource.PARTICIPANT
                        else "Metsights reference_id"
                    )
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

                    if blood_loaded_this_run and package_code in _PACKAGE_BLOOD_CATEGORY_KEYS:
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
                            report_exists = True

                        if report_exists:
                            details.append({
                                "user_id": user_id, "engagement_id": engagement_id,
                                "action": "skipped",
                                "reason": "skipped metsights push: BioAI report already generated",
                            })
                        else:
                            category_keys = _PACKAGE_BLOOD_CATEGORY_KEYS[package_code]
                            for category_key in category_keys:
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
                                    await db.rollback()
                                    logger.warning(
                                        "Metsights blood push failed for user=%s category=%s: %s",
                                        user_id, category_key, exc,
                                    )
                                    details.append({
                                        "user_id": user_id, "engagement_id": engagement_id,
                                        "action": "failed",
                                        "reason": (
                                            f"metsights push failed for {category_key}: "
                                            f"{str(exc)[:100]}"
                                        ),
                                    })

            fetched_diag_url = None
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
            if report_data is not None:
                report_list = report_data.get("data")
                if isinstance(report_list, list) and report_list:
                    matched_report = _match_customer_by_name(
                        report_list, first_name or "", last_name or ""
                    )
                    if matched_report:
                        fetched_diag_url = (
                            matched_report.get("report_url") or matched_report.get("url")
                        )
            else:
                logger.warning(
                    "Healthians getBookingReport failed for user=%s booking=%s",
                    user_id, reference_id,
                )

            if fetched_diag_url is not None:
                ihr = await _get_or_create_ihr(
                    db,
                    ihr_id=ihr_id,
                    user_id=user_id,
                    engagement_id=engagement_id,
                    instance_id=instance_id,
                )
                ihr.diagnostic_report_url = fetched_diag_url
                diagnostic_report_url = fetched_diag_url
                await db.flush()
                await db.commit()
                if not blood_loaded_this_run:
                    loaded += 1
                source_label = (
                    "participant booking_id"
                    if booking_source == HealthiansBookingSource.PARTICIPANT
                    else "Metsights reference_id"
                )
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "loaded",
                    "reason": f"diagnostic_report_url refreshed from Healthians via {source_label}",
                })

            if not _blood_report_data_complete(blood_parameters, diagnostic_report_url):
                skipped += 1
                if fetched_blood is None and fetched_diag_url is None:
                    incomplete_reason = "no data returned from Healthians"
                else:
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

            service_keys = [
                k.strip() for k in (blood_report_notification or "").split(",") if k.strip()
            ]
            if not service_keys:
                skipped += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "skipped", "reason": "blood reports ready, no notification keys configured",
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
                "load_blood_reports failed: user=%s engagement=%s: %s",
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
