"""Push local questionnaire answers to MetSights for running engagements.

For every participant in a running engagement with Metsights Basic/Pro or FitPrint
assessment instances:
1. Merge answers from all of the user's assessment packages in the engagement.
2. PATCH Metsights sub-resources (physical, vitals, diet-lifestyle, fitness).
3. Exclude blood-parameters and advanced-blood-parameters (handled by load_blood_reports).

Re-pushes whenever local answers exist; does not gate on is_submitted.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement
from modules.engagements.repository import EngagementsRepository
from modules.metsights.sync_service import MetsightsSyncService
from modules.questionnaire.repository import QuestionnaireRepository

logger = logging.getLogger(__name__)

_PUSHABLE_TYPE_CODES = frozenset({"1", "2", "7"})

METSIGHTS_PUSH_CATEGORIES: list[str] = [
    "physical-measurement",
    "vitals",
    "diet-lifestyle-parameters",
]

FITPRINT_PUSH_CATEGORIES: list[str] = [
    "fitness-parameters",
]


def categories_for_type_code(type_code: str) -> list[str]:
    """Return Metsights sub-resources to push for an assessment type code."""
    tc = (type_code or "").strip()
    if tc in ("1", "2"):
        return list(METSIGHTS_PUSH_CATEGORIES)
    if tc == "7":
        return list(FITPRINT_PUSH_CATEGORIES)
    return []


async def _load_running_engagement_instances(
    db: AsyncSession,
) -> list[tuple[AssessmentInstance, AssessmentPackage]]:
    """All assessment instances in running engagements (any package type)."""
    query = (
        select(AssessmentInstance, AssessmentPackage)
        .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
        .join(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
        .where(EngagementsRepository._running_engagement_status_filter())
        .order_by(
            AssessmentInstance.engagement_id.asc(),
            AssessmentInstance.user_id.asc(),
            AssessmentInstance.assessment_instance_id.asc(),
        )
    )
    result = await db.execute(query)
    return [(row[0], row[1]) for row in result.all()]


def _group_instances_by_user(
    rows: list[tuple[AssessmentInstance, AssessmentPackage]],
) -> dict[tuple[int, int], list[tuple[AssessmentInstance, AssessmentPackage]]]:
    grouped: dict[tuple[int, int], list[tuple[AssessmentInstance, AssessmentPackage]]] = defaultdict(list)
    for instance, package in rows:
        key = (int(instance.engagement_id), int(instance.user_id))
        grouped[key].append((instance, package))
    return grouped


def _is_push_target(instance: AssessmentInstance, package: AssessmentPackage) -> bool:
    type_code = (package.assessment_type_code or "").strip()
    if type_code not in _PUSHABLE_TYPE_CODES:
        return False
    mrid = (instance.metsights_record_id or "").strip()
    return bool(mrid)


ProgressCallback = Callable[[int, int, int, int, int], None]


async def push_questionnaires_to_metsights(
    db: AsyncSession,
    *,
    sync_service: MetsightsSyncService,
    questionnaire_repository: QuestionnaireRepository | None = None,
    as_of: date | None = None,
    dry_run: bool = False,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Push questionnaire answers to Metsights for all running-engagement participants.

    ``on_progress(done, total, pushed, skipped, failed)`` is called after each
    target instance is processed.
    """
    today = as_of or date.today()
    q_repo = questionnaire_repository or QuestionnaireRepository()

    all_rows = await _load_running_engagement_instances(db)
    grouped = _group_instances_by_user(all_rows)

    targets: list[tuple[AssessmentInstance, AssessmentPackage, list[int]]] = []
    for (_engagement_id, _user_id), user_rows in grouped.items():
        source_ids = [int(inst.assessment_instance_id) for inst, _pkg in user_rows]
        for instance, package in user_rows:
            if _is_push_target(instance, package):
                targets.append((instance, package, source_ids))

    matched = len(targets)
    pushed = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []
    options_cache: dict[str, dict] = {}

    def _report(done: int) -> None:
        if on_progress is not None:
            on_progress(done, matched, pushed, skipped, failed)

    _report(0)

    for index, (instance, package, source_ids) in enumerate(targets, start=1):
        ai_id = int(instance.assessment_instance_id)
        user_id = int(instance.user_id)
        engagement_id = int(instance.engagement_id)
        type_code = (package.assessment_type_code or "").strip()
        categories = categories_for_type_code(type_code)

        if not categories:
            skipped += 1
            details.append({
                "assessment_instance_id": ai_id,
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "skipped",
                "reason": f"unsupported assessment type {type_code!r}",
            })
            _report(index)
            continue

        try:
            if dry_run:
                responses = await q_repo.list_responses_for_instances(
                    db,
                    assessment_instance_ids=source_ids,
                )
                if not responses:
                    skipped += 1
                    details.append({
                        "assessment_instance_id": ai_id,
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "action": "dry_run",
                        "reason": "would skip: no questionnaire responses",
                    })
                else:
                    pushed += 1
                    details.append({
                        "assessment_instance_id": ai_id,
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "action": "dry_run",
                        "reason": f"would push categories: {', '.join(categories)}",
                    })
                _report(index)
                continue

            result = await sync_service.push_questionnaire_for_instance(
                db,
                assessment_instance_id=ai_id,
                source_assessment_instance_ids=source_ids,
                options_cache=options_cache,
                categories=categories,
            )

            if result.get("pushed"):
                pushed += 1
                patched = result.get("resources_patched") or []
                details.append({
                    "assessment_instance_id": ai_id,
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "action": "pushed",
                    "reason": f"patched: {', '.join(patched) if patched else 'none'}",
                })
            else:
                skipped += 1
                reason = result.get("reason") or "not pushed"
                section_errors = result.get("section_errors") or []
                if section_errors:
                    reason = f"{reason}; section_errors={', '.join(section_errors)}"
                details.append({
                    "assessment_instance_id": ai_id,
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "action": "skipped",
                    "reason": reason,
                })

        except Exception as exc:
            failed += 1
            details.append({
                "assessment_instance_id": ai_id,
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "failed",
                "reason": str(exc),
            })
            logger.warning(
                "push_questionnaires_to_metsights failed: instance=%s user=%s engagement=%s: %s",
                ai_id,
                user_id,
                engagement_id,
                exc,
                exc_info=True,
            )

        _report(index)

    return {
        "as_of": today.isoformat(),
        "matched": matched,
        "pushed": pushed,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry_run,
        "details": details,
    }
