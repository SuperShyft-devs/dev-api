"""Load blood reports from Healthians and send notifications.

For participants in running engagements with MetSights Pro/Basic assessments
where today >= engagement_date:
1. If individual_health_report.blood_parameters or diagnostic_report_url is null,
   fetch data from Healthians.
2. When both fields are present, send notifications using
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
from modules.metsights.service import MetsightsService
from modules.diagnostics.healthians import client as healthians_client
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport
from modules.users.models import User

logger = logging.getLogger(__name__)

_METSIGHTS_PRO_BASIC_TYPE_CODES = {"1", "2"}


def _blood_report_data_complete(blood_parameters: Any, diagnostic_report_url: Any) -> bool:
    return blood_parameters is not None and diagnostic_report_url is not None


def _provider_name(provider_field: Any) -> str:
    if isinstance(provider_field, dict):
        return str(provider_field.get("name") or "").strip()
    return str(provider_field or "").strip()


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
            (IndividualHealthReport.assessment_instance_id == AssessmentInstance.assessment_instance_id),
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
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "dry_run",
                "reason": "blood_report_complete" if complete else "blood_report_incomplete",
            })
            continue

        try:
            blood_parameters = blood_params
            diagnostic_report_url = diag_url

            if not _blood_report_data_complete(blood_parameters, diagnostic_report_url):
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
                provider_name = _provider_name(collection_data.get("provider"))

                if not reference_id:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "no reference_id from MetSights collections",
                    })
                    continue

                if "healthians" not in provider_name.lower():
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": f"provider is '{provider_name or 'unknown'}', not Healthians",
                    })
                    continue

                access_token = await healthians_client.get_access_token()

                fetched_blood = None
                fetched_diag_url = None

                if blood_parameters is None:
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

                if diagnostic_report_url is None:
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

                if fetched_blood is None and fetched_diag_url is None:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "no data returned from Healthians",
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

                if fetched_blood is not None:
                    ihr.blood_parameters = fetched_blood
                    blood_parameters = fetched_blood
                if fetched_diag_url is not None:
                    ihr.diagnostic_report_url = fetched_diag_url
                    diagnostic_report_url = fetched_diag_url
                await db.flush()

                if not _blood_report_data_complete(blood_parameters, diagnostic_report_url):
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "blood report data incomplete (missing blood_parameters or diagnostic_report_url)",
                    })
                    continue

                loaded += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "loaded", "reason": "blood data fetched from Healthians",
                })

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
