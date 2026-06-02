"""Dispatch questionnaire reminder notifications for participants with engagement_date tomorrow or yesterday."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.assessments.package_questions_service import AssessmentPackageCategoriesService
from modules.engagements.repository import EngagementsRepository
from modules.metsights.service import MetsightsService
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

_METSIGHTS_PRO_BASIC_TYPE_CODES = {"1", "2"}
_FITPRINT_TYPE_CODES = {"7"}

_PRIMARY_RESOURCES = ("diet-lifestyle-parameters", "physical-measurement", "vitals")
_FITNESS_RESOURCE = "fitness-parameters"


def _resolve_today(*, as_of: date | None = None) -> date:
    if as_of is not None:
        return as_of
    return date.today()


async def _get_assessment_instances(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
) -> tuple[AssessmentInstance | None, AssessmentInstance | None]:
    """Return (primary_instance, fitprint_instance) for the user+engagement.

    *primary_instance*: package with assessment_type_code in ('1','2') — MetSights Basic/Pro.
    *fitprint_instance*: package with assessment_type_code '7' — FitPrint.
    """
    query = (
        select(AssessmentInstance, AssessmentPackage)
        .outerjoin(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
        .where(AssessmentInstance.user_id == user_id)
        .where(AssessmentInstance.engagement_id == engagement_id)
        .order_by(AssessmentInstance.assessment_instance_id.asc())
    )
    result = await db.execute(query)
    rows = result.all()

    primary: AssessmentInstance | None = None
    fitprint: AssessmentInstance | None = None

    for instance, package in rows:
        type_code = (package.assessment_type_code or "").strip() if package else ""
        if type_code in _METSIGHTS_PRO_BASIC_TYPE_CODES and primary is None:
            primary = instance
        elif type_code in _FITPRINT_TYPE_CODES and fitprint is None:
            fitprint = instance

    return primary, fitprint


async def _check_metsights_complete(
    metsights_service: MetsightsService,
    *,
    primary_record_id: str | None,
    fitprint_record_id: str | None,
) -> bool:
    """Return True if ALL metsights sub-resources report is_complete=True."""
    if not primary_record_id:
        return False

    for resource in _PRIMARY_RESOURCES:
        data = await metsights_service.get_record_subresource_or_none(
            record_id=primary_record_id, resource=resource
        )
        if data is None or not data.get("is_complete", False):
            return False

    if fitprint_record_id:
        data = await metsights_service.get_record_subresource_or_none(
            record_id=fitprint_record_id, resource=_FITNESS_RESOURCE
        )
        if data is None or not data.get("is_complete", False):
            return False

    return True


async def _check_internal_complete(
    db: AsyncSession,
    categories_service: AssessmentPackageCategoriesService,
    *,
    user_id: int,
    primary_instance: AssessmentInstance | None,
) -> bool:
    """Return True if all internal questionnaire categories are complete for the primary assessment."""
    if primary_instance is None:
        return False

    try:
        categories = await categories_service.list_category_completion_for_assessment_instance(
            db,
            user_id=user_id,
            assessment_instance_id=int(primary_instance.assessment_instance_id),
        )
    except Exception:
        logger.warning(
            "Internal completion check failed for user_id=%s instance=%s",
            user_id,
            primary_instance.assessment_instance_id,
            exc_info=True,
        )
        return False

    if not categories:
        return False

    return all(cat.get("status") == "complete" for cat in categories)


async def dispatch_questionnaire_reminders(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    engagements_repository: EngagementsRepository,
    metsights_service: MetsightsService,
    categories_service: AssessmentPackageCategoriesService,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Find participants with engagement_date = tomorrow or yesterday and dispatch reminders for incomplete questionnaires."""
    today = _resolve_today(as_of=as_of)
    tomorrow = today + timedelta(days=1)
    yesterday = today - timedelta(days=1)

    participants = await engagements_repository.list_participants_for_questionnaire_reminder(
        db, target_dates=[tomorrow, yesterday]
    )

    matched = len(participants)
    sent = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []

    if dry_run:
        for user_id, engagement_id, engagement_date, qr1, qr2 in participants:
            reminder_type = "reminder_1" if engagement_date == tomorrow else "reminder_2"
            service_key = qr1 if engagement_date == tomorrow else qr2
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "engagement_date": engagement_date.isoformat(),
                "reminder_type": reminder_type,
                "service_key": service_key,
                "action": "dry_run",
                "reason": "no service key configured" if not service_key else "would check and dispatch",
            })
        return {
            "as_of": today.isoformat(),
            "tomorrow": tomorrow.isoformat(),
            "yesterday": yesterday.isoformat(),
            "matched": matched,
            "sent": 0,
            "skipped": matched,
            "failed": 0,
            "dry_run": True,
            "details": details,
        }

    for user_id, engagement_id, engagement_date, qr1, qr2 in participants:
        reminder_type = "reminder_1" if engagement_date == tomorrow else "reminder_2"

        if engagement_date == tomorrow:
            service_key = qr1
        elif engagement_date == yesterday:
            service_key = qr2
        else:
            skipped += 1
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "engagement_date": engagement_date.isoformat(),
                "reminder_type": reminder_type, "action": "skipped",
                "reason": f"engagement_date {engagement_date} is neither tomorrow nor yesterday",
            })
            continue

        if not service_key:
            skipped += 1
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "engagement_date": engagement_date.isoformat(),
                "reminder_type": reminder_type, "action": "skipped",
                "reason": f"no service key configured for {reminder_type}",
            })
            continue

        try:
            primary_instance, fitprint_instance = await _get_assessment_instances(
                db, user_id=user_id, engagement_id=engagement_id
            )

            primary_record_id = (
                (primary_instance.metsights_record_id or "").strip()
                if primary_instance else None
            ) or None
            fitprint_record_id = (
                (fitprint_instance.metsights_record_id or "").strip()
                if fitprint_instance else None
            ) or None

            metsights_complete = await _check_metsights_complete(
                metsights_service,
                primary_record_id=primary_record_id,
                fitprint_record_id=fitprint_record_id,
            )

            if metsights_complete:
                skipped += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "engagement_date": engagement_date.isoformat(),
                    "reminder_type": reminder_type, "service_key": service_key,
                    "action": "skipped",
                    "reason": "questionnaire complete on Metsights",
                })
                continue

            internal_complete = await _check_internal_complete(
                db,
                categories_service,
                user_id=user_id,
                primary_instance=primary_instance,
            )

            if internal_complete:
                skipped += 1
                details.append({
                    "user_id": user_id, "engagement_id": engagement_id,
                    "engagement_date": engagement_date.isoformat(),
                    "reminder_type": reminder_type, "service_key": service_key,
                    "action": "skipped",
                    "reason": "questionnaire complete in internal DB",
                })
                continue

            await notifications_service.dispatch(
                db,
                payload=DispatchRequest(
                    service_key=service_key,
                    user_ids=[user_id],
                    engagement_id=engagement_id,
                ),
                triggered_by_user_id=None,
            )
            sent += 1
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "engagement_date": engagement_date.isoformat(),
                "reminder_type": reminder_type, "service_key": service_key,
                "action": "sent",
                "reason": "questionnaire incomplete in both Metsights and internal DB",
            })

        except Exception as exc:
            failed += 1
            details.append({
                "user_id": user_id, "engagement_id": engagement_id,
                "engagement_date": engagement_date.isoformat(),
                "reminder_type": reminder_type, "service_key": service_key,
                "action": "failed",
                "reason": str(exc),
            })
            logger.warning(
                "Questionnaire reminder dispatch failed: user_id=%s engagement_id=%s: %s",
                user_id, engagement_id, str(exc),
                exc_info=True,
            )

    return {
        "as_of": today.isoformat(),
        "tomorrow": tomorrow.isoformat(),
        "yesterday": yesterday.isoformat(),
        "matched": matched,
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "dry_run": False,
        "details": details,
    }
