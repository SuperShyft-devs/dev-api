"""Load blood reports from Healthians and send notifications.

For participants in running engagements with MetSights Pro/Basic assessments
where today >= engagement_date:
1. Always refresh individual_health_report.diagnostic_report_url from Healthians.
2. Fetch blood_parameters from Healthians only when that field is null.
3. When both fields are present, send notifications using
   engagement.blood_report_notification (skipping services already sent).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.diagnostics.repository import DiagnosticsRepository
from modules.metsights.service import MetsightsService
from modules.diagnostics.healthians import client as healthians_client
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.blood_parameters_normalizer import build_grouped_from_healthians
from modules.reports.blood_parameters_schemas import (
    has_usable_provider_blood_parameters,
    provider_code_from_field,
)
from modules.reports.models import IndividualHealthReport
from modules.users.models import User

logger = logging.getLogger(__name__)

_METSIGHTS_PRO_BASIC_TYPE_CODES = {"1", "2"}


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


def _provider_code(provider_field: Any) -> str:
    return provider_code_from_field(provider_field)


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
     blood_parameters, diagnostic_report_url, blood_report_notification, ihr_id, instance_id)
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
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .join(User, User.user_id == EngagementParticipant.user_id)
        .join(
            AssessmentInstance,
            (AssessmentInstance.engagement_id == EngagementParticipant.engagement_id)
            & (AssessmentInstance.user_id == EngagementParticipant.user_id),
        )
        .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
        .outerjoin(
            IndividualHealthReport,
            (IndividualHealthReport.engagement_id == EngagementParticipant.engagement_id)
            & (IndividualHealthReport.user_id == EngagementParticipant.user_id),
        )
        .where(Engagement.status.ilike("running"))
        .where(EngagementParticipant.engagement_date <= today)
        .where(AssessmentPackage.assessment_type_code.in_(_METSIGHTS_PRO_BASIC_TYPE_CODES))
        .where(AssessmentInstance.metsights_record_id.isnot(None))
        .where(AssessmentInstance.metsights_record_id != "")
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
    record_id: str,
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
                record_id=record_id,
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
            diagnostic_package_id,
        ) = row

        record_id = (record_id or "").strip()
        if not record_id:
            skipped += 1
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "skipped", "reason": "no metsights_record_id",
            })
            continue

        if dry_run:
            complete = _blood_report_data_complete(blood_params, diag_url)
            dry_run_reasons = ["would_refresh_diagnostic_report_url"]
            if not has_usable_provider_blood_parameters(blood_params):
                dry_run_reasons.append("would_fetch_blood_parameters")
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

            try:
                collection_data = await metsights_service.get_fetch_collections(record_id=record_id)
            except Exception:
                skipped += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "skipped", "reason": "fetch-collections not available for this record",
                })
                continue

            reference_id = str(collection_data.get("reference_id") or "").strip()
            provider_code = _provider_code(collection_data.get("provider"))

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

            access_token = await healthians_client.get_access_token()

            fetched_blood = None
            fetched_diag_url = None

            if not has_usable_provider_blood_parameters(blood_parameters):
                try:
                    digital_value = await healthians_client.get_booking_digital_value(
                        access_token, reference_id
                    )
                    data_list = digital_value.get("data")
                    if isinstance(data_list, list) and data_list:
                        matched_entry = _match_customer_by_name(
                            data_list, first_name or "", last_name or ""
                        )
                        if matched_entry:
                            fetched_blood = matched_entry
                except Exception as exc:
                    logger.warning(
                        "Healthians getBookingDigitalValue failed for user=%s booking=%s: %s",
                        user_id, reference_id, exc,
                    )

            try:
                report_data = await healthians_client.get_booking_report(
                    access_token, reference_id
                )
                report_list = report_data.get("data")
                if isinstance(report_list, list) and report_list:
                    matched_report = _match_customer_by_name(
                        report_list, first_name or "", last_name or ""
                    )
                    if matched_report:
                        fetched_diag_url = (
                            matched_report.get("report_url") or matched_report.get("url")
                        )
            except Exception as exc:
                logger.warning(
                    "Healthians getBookingReport failed for user=%s booking=%s: %s",
                    user_id, reference_id, exc,
                )

            if fetched_blood is not None or fetched_diag_url is not None:
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

                if fetched_blood is not None:
                    if diagnostic_package_id is None:
                        details.append({
                            "user_id": user_id, "engagement_id": engagement_id,
                            "action": "skipped",
                            "reason": "engagement has no diagnostic package for blood parameters",
                        })
                    else:
                        grouped, raw = await _group_provider_blood(
                            db,
                            fetched_blood,
                            diagnostic_package_id=int(diagnostic_package_id),
                        )
                        ihr.blood_parameters = grouped
                        ihr.blood_report_raw = raw
                        blood_parameters = grouped
                if fetched_diag_url is not None:
                    ihr.diagnostic_report_url = fetched_diag_url
                    diagnostic_report_url = fetched_diag_url
                await db.flush()
                await db.commit()

                loaded += 1
                if fetched_blood is not None and fetched_diag_url is not None:
                    load_reason = "blood data and diagnostic_report_url fetched from Healthians"
                elif fetched_blood is not None:
                    load_reason = "blood data fetched from Healthians"
                else:
                    load_reason = "diagnostic_report_url refreshed from Healthians"
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "loaded", "reason": load_reason,
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
                record_id=record_id,
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
