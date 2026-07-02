"""Idempotent upsert operations for Metsights sync categories, links, and configs."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db.seed.metsights_sync_registry import (
    CATEGORY_QUESTION_ORDER,
    METSIGHTS_CATEGORY_OF,
    METSIGHTS_SYNC_BY_QUESTION_KEY,
    METSIGHTS_SYNC_CATEGORIES,
    PACKAGE_METSIGHTS_CATEGORY_LINKS,
    QUESTION_CATEGORY_ASSIGNMENTS,
)
from modules.assessments.models import AssessmentPackageCategory
from modules.assessments.repository import AssessmentsRepository
from modules.questionnaire.models import QuestionnaireCategory
from modules.questionnaire.repository import QuestionnaireRepository


async def _ensure_category_key_unique_constraint(db: AsyncSession) -> bool:
    """Ensure global category_key uniqueness (rename supershyft vitals collision first).

    Returns True when the schema was upgraded in-place.
    """
    upgraded = False

    rename_result = await db.execute(
        text(
            """
            UPDATE questionnaire_categories
            SET category_key = 'health_vitals'
            WHERE category_key = 'vitals' AND category_of = 'supershyft'
            """
        )
    )
    if rename_result.rowcount and rename_result.rowcount > 0:
        upgraded = True

    has_global = (
        await db.execute(
            text("SELECT 1 FROM pg_constraint WHERE conname = 'uq_questionnaire_categories_key'")
        )
    ).scalar_one_or_none()
    if has_global is not None:
        await db.flush()
        return upgraded

    await db.execute(
        text(
            "ALTER TABLE questionnaire_categories "
            "DROP CONSTRAINT IF EXISTS uq_questionnaire_categories_key_category_of"
        )
    )
    await db.execute(
        text("ALTER TABLE questionnaire_categories DROP CONSTRAINT IF EXISTS uq_questionnaire_categories_key")
    )
    await db.execute(
        text(
            "ALTER TABLE questionnaire_categories "
            "ADD CONSTRAINT uq_questionnaire_categories_key UNIQUE (category_key)"
        )
    )
    await db.flush()
    return True


async def reset_metsights_sync(db: AsyncSession) -> dict[str, Any]:
    """Create or update Metsights sync taxonomy from the codebase registry.

    Does not commit — caller is responsible for ``db.commit()``.
    """
    q_repo = QuestionnaireRepository()
    assessments_repo = AssessmentsRepository()

    stats: dict[str, Any] = {
        "schema_upgraded": False,
        "categories_total": 0,
        "categories_created": 0,
        "categories_updated": 0,
        "categories_unchanged": 0,
        "question_links_total": 0,
        "links_added": 0,
        "questions_sync_updated": 0,
        "package_links_total": 0,
        "package_links_added": 0,
        "missing_question_keys": [],
        "missing_package_codes": [],
    }

    stats["schema_upgraded"] = await _ensure_category_key_unique_constraint(db)

    category_id_by_key: dict[str, int] = {}

    for category_key, display_name in METSIGHTS_SYNC_CATEGORIES:
        existing = await q_repo.get_category_by_key_and_category_of(
            db,
            category_key=category_key,
            category_of=METSIGHTS_CATEGORY_OF,
        )
        if existing is None:
            row = QuestionnaireCategory(
                category_key=category_key,
                display_name=display_name,
                category_of=METSIGHTS_CATEGORY_OF,
                status="active",
            )
            db.add(row)
            await db.flush()
            category_id_by_key[category_key] = int(row.category_id)
            stats["categories_created"] += 1
        else:
            changed = False
            if existing.display_name != display_name:
                existing.display_name = display_name
                changed = True
            if (existing.status or "").lower() != "active":
                existing.status = "active"
                changed = True
            if changed:
                await q_repo.update_category(db, existing)
                stats["categories_updated"] += 1
            else:
                stats["categories_unchanged"] += 1
            category_id_by_key[category_key] = int(existing.category_id)

    question_id_by_key: dict[str, int] = {}
    for question_key in QUESTION_CATEGORY_ASSIGNMENTS:
        definition = await q_repo.get_definition_by_key(db, question_key=question_key)
        if definition is None:
            stats["missing_question_keys"].append(question_key)
            continue
        question_id_by_key[question_key] = int(definition.question_id)

    links_before: dict[str, set[int]] = {}
    for category_key, category_id in category_id_by_key.items():
        ordered_ids = await q_repo.get_assigned_question_ids_for_category_ordered(
            db,
            category_id=category_id,
        )
        links_before[category_key] = set(ordered_ids)

    for category_key, ordered_keys in CATEGORY_QUESTION_ORDER.items():
        category_id = category_id_by_key.get(category_key)
        if category_id is None:
            continue
        question_ids = [
            question_id_by_key[key]
            for key in ordered_keys
            if key in question_id_by_key
        ]
        if not question_ids:
            continue
        await q_repo.assign_questions_to_category(
            db,
            category_id=category_id,
            question_ids=question_ids,
        )
        await q_repo.reorder_category_questions(
            db,
            category_id=category_id,
            question_ids=question_ids,
        )

    for category_key, category_id in category_id_by_key.items():
        ordered_ids = await q_repo.get_assigned_question_ids_for_category_ordered(
            db,
            category_id=category_id,
        )
        before = links_before.get(category_key, set())
        after = set(ordered_ids)
        stats["question_links_total"] += len(after)
        stats["links_added"] += len(after - before)

    for question_key, sync_cfg in METSIGHTS_SYNC_BY_QUESTION_KEY.items():
        question_id = question_id_by_key.get(question_key)
        if question_id is None:
            continue
        definition = await q_repo.get_definition_by_id(db, question_id)
        if definition is None:
            continue
        definition.metsights_sync = sync_cfg
        await q_repo.update_definition(db, definition)
        stats["questions_sync_updated"] += 1

    for package_code, category_keys in PACKAGE_METSIGHTS_CATEGORY_LINKS.items():
        package = await assessments_repo.get_package_by_code(db, package_code=package_code)
        if package is None:
            stats["missing_package_codes"].append(package_code)
            continue
        for index, category_key in enumerate(category_keys, start=1):
            category_id = category_id_by_key.get(category_key)
            if category_id is None:
                continue
            existing = await assessments_repo.get_package_category_link(
                db,
                package_id=int(package.package_id),
                category_id=category_id,
            )
            if existing is not None:
                stats["package_links_total"] += 1
                continue
            link = AssessmentPackageCategory(
                package_id=int(package.package_id),
                category_id=category_id,
                display_order=index,
            )
            await assessments_repo.create_package_category_link(db, link)
            stats["package_links_added"] += 1
            stats["package_links_total"] += 1

    stats["categories_total"] = (
        stats["categories_created"] + stats["categories_updated"] + stats["categories_unchanged"]
    )

    return stats
