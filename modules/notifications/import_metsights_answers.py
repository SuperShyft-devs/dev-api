"""Auto-import questionnaire answers from MetSights for incomplete assessment instances.

For participants in running engagements whose assessment_instance.status != 'complete':
1. Check MetSights record sub-resources for is_complete flags.
2. If any sub-resource is complete, import answers via MetsightsSyncService.
3. After import, mark the assessment instance as complete if the API didn't already.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService

logger = logging.getLogger(__name__)

_METSIGHTS_PRO_BASIC_TYPE_CODES = {"1", "2"}
_FITPRINT_TYPE_CODES = {"7"}

_PRIMARY_RESOURCES = ("physical-measurement", "vitals", "diet-lifestyle-parameters")
_FITNESS_RESOURCE = "fitness-parameters"


async def _get_running_engagement_instances(
    db: AsyncSession,
) -> list[tuple[AssessmentInstance, AssessmentPackage, int]]:
    """Return all incomplete assessment instances for participants in running engagements."""
    query = (
        select(AssessmentInstance, AssessmentPackage, EngagementParticipant.user_id)
        .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
        .join(
            EngagementParticipant,
            (EngagementParticipant.engagement_id == AssessmentInstance.engagement_id)
            & (EngagementParticipant.user_id == AssessmentInstance.user_id),
        )
        .join(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
        .where(Engagement.status.ilike("running"))
        .where(AssessmentInstance.status != "complete")
        .where(AssessmentInstance.metsights_record_id.isnot(None))
        .where(AssessmentInstance.metsights_record_id != "")
        .order_by(AssessmentInstance.engagement_id.asc(), AssessmentInstance.user_id.asc())
    )
    result = await db.execute(query)
    return [(row[0], row[1], int(row[2])) for row in result.all()]


async def _check_any_subresource_complete(
    metsights_service: MetsightsService,
    *,
    record_id: str,
    type_code: str,
) -> bool:
    """Return True if any relevant sub-resource on MetSights has is_complete=True."""
    if type_code in _METSIGHTS_PRO_BASIC_TYPE_CODES:
        resources = _PRIMARY_RESOURCES
    elif type_code in _FITPRINT_TYPE_CODES:
        resources = (_FITNESS_RESOURCE,)
    else:
        return False

    for resource in resources:
        try:
            data = await metsights_service.get_record_subresource_or_none(
                record_id=record_id, resource=resource
            )
            if data is not None and data.get("is_complete", False):
                return True
        except Exception:
            continue
    return False


async def import_metsights_answers(
    db: AsyncSession,
    *,
    metsights_service: MetsightsService,
    sync_service: MetsightsSyncService,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Check all incomplete assessment instances and import answers from MetSights where sub-resources are complete."""
    today = as_of or date.today()

    instances = await _get_running_engagement_instances(db)
    matched = len(instances)
    imported = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []

    for instance, package, user_id in instances:
        record_id = (instance.metsights_record_id or "").strip()
        type_code = (package.assessment_type_code or "").strip()
        ai_id = int(instance.assessment_instance_id)

        if not record_id:
            skipped += 1
            details.append({
                "assessment_instance_id": ai_id, "user_id": user_id,
                "action": "skipped", "reason": "no metsights_record_id",
            })
            continue

        try:
            has_complete = await _check_any_subresource_complete(
                metsights_service, record_id=record_id, type_code=type_code,
            )

            if not has_complete:
                skipped += 1
                details.append({
                    "assessment_instance_id": ai_id, "user_id": user_id,
                    "action": "skipped", "reason": "no sub-resource complete on MetSights",
                })
                continue

            if dry_run:
                skipped += 1
                details.append({
                    "assessment_instance_id": ai_id, "user_id": user_id,
                    "action": "dry_run", "reason": "would import answers",
                })
                continue

            result = await sync_service.import_questionnaire_answers_for_instance(
                db,
                assessment_instance_id=ai_id,
                current_user_id=user_id,
                employee_ok=True,
            )

            await db.flush()
            await db.refresh(instance)
            if (instance.status or "").strip().lower() != "complete":
                instance.status = "complete"
                instance.completed_at = datetime.now(timezone.utc)

            imported += 1
            details.append({
                "assessment_instance_id": ai_id, "user_id": user_id,
                "action": "imported",
                "reason": f"upserted {result.get('responses_upserted', 0)} responses",
            })

        except Exception as exc:
            failed += 1
            details.append({
                "assessment_instance_id": ai_id, "user_id": user_id,
                "action": "failed", "reason": str(exc),
            })
            logger.warning(
                "import_metsights_answers failed: instance=%s user=%s: %s",
                ai_id, user_id, exc, exc_info=True,
            )

    return {
        "as_of": today.isoformat(),
        "matched": matched,
        "imported": imported,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry_run,
        "details": details,
    }
