"""Load blood reports from Healthians and send notifications.

For participants in running engagements with MetSights Pro/Basic assessments
where today >= engagement_date:
1. If individual_health_report.blood_parameters or diagnostic_report_url is null,
   fetch data from Healthians.
2. After loading, send notifications using engagement.blood_report_notification.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.service import MetsightsService
from modules.diagnostics.healthians import client as healthians_client
from modules.notifications.dedup import has_notification_been_sent
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport
from modules.users.models import User

logger = logging.getLogger(__name__)

_METSIGHTS_PRO_BASIC_TYPE_CODES = {"1", "2"}


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
            IndividualHealthReport.individual_health_report_id,
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
            (IndividualHealthReport.user_id == EngagementParticipant.user_id)
            & (IndividualHealthReport.engagement_id == EngagementParticipant.engagement_id),
        )
        .where(Engagement.status.ilike("running"))
        .where(EngagementParticipant.engagement_date <= today)
        .where(AssessmentPackage.assessment_type_code.in_(_METSIGHTS_PRO_BASIC_TYPE_CODES))
        .where(AssessmentInstance.metsights_record_id.isnot(None))
        .where(AssessmentInstance.metsights_record_id != "")
    )
    result = await db.execute(query)
    return result.all()


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
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "dry_run",
                "reason": "blood_params_null" if not blood_params else "blood_params_exists",
            })
            continue

        try:
            needs_blood = blood_params is None
            needs_diag = diag_url is None

            if needs_blood or needs_diag:
                collection_data = await metsights_service.get_fetch_collections(record_id=record_id)
                reference_id = str(collection_data.get("reference_id") or "").strip()
                provider = str(collection_data.get("provider") or "").strip().lower()

                if not reference_id:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "no reference_id from MetSights collections",
                    })
                    continue

                if "healthians" not in provider:
                    skipped += 1
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": f"provider is '{provider}', not Healthians",
                    })
                    continue

                access_token = await healthians_client.get_access_token()

                ihr = None
                if ihr_id:
                    ihr_result = await db.execute(
                        select(IndividualHealthReport).where(
                            IndividualHealthReport.individual_health_report_id == ihr_id
                        )
                    )
                    ihr = ihr_result.scalar_one_or_none()

                if ihr is None:
                    ihr = IndividualHealthReport(
                        user_id=user_id,
                        engagement_id=engagement_id,
                    )
                    db.add(ihr)
                    await db.flush()

                if needs_blood:
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
                                ihr.blood_parameters = matched_entry
                    except Exception as exc:
                        logger.warning(
                            "Healthians getBookingDigitalValue failed for user=%s booking=%s: %s",
                            user_id, reference_id, exc,
                        )

                if needs_diag:
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
                                ihr.diagnostic_report_url = matched_report.get("report_url") or matched_report.get("url")
                    except Exception as exc:
                        logger.warning(
                            "Healthians getBookingReport failed for user=%s booking=%s: %s",
                            user_id, reference_id, exc,
                        )

                loaded += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "action": "loaded", "reason": "blood data fetched from Healthians",
                })

            # Send notifications (regardless of whether we just loaded or it was already there)
            service_keys = [
                k.strip() for k in (blood_report_notification or "").split(",") if k.strip()
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
            elif not service_keys and (blood_params is not None and diag_url is not None):
                skipped += 1
                if not any(d.get("user_id") == user_id and d.get("engagement_id") == engagement_id for d in details):
                    details.append({
                        "user_id": user_id, "engagement_id": engagement_id,
                        "action": "skipped", "reason": "already loaded, no notification keys configured",
                    })

        except Exception as exc:
            failed += 1
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "action": "failed", "reason": str(exc),
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
