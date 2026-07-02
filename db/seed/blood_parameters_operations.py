"""Reload blood parameter questions from the Metsights OPTIONS registry."""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_CATEGORY_DISPLAY,
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    ADVANCED_BLOOD_PARAMETER_QUESTION_ORDER,
    ALL_BLOOD_PARAMETER_KEYS,
    BLOOD_PARAMETER_CATEGORY_DISPLAY,
    BLOOD_PARAMETER_CATEGORY_KEY,
    BLOOD_PARAMETER_FIELDS,
    BLOOD_PARAMETER_QUESTION_ORDER,
    build_blood_parameter_metsights_sync,
)
from db.seed.metsights_sync_registry import METSIGHTS_CATEGORY_OF, PACKAGE_METSIGHTS_CATEGORY_LINKS
from modules.assessments.models import AssessmentPackageCategory
from modules.assessments.repository import AssessmentsRepository
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
)
from modules.questionnaire.repository import QuestionnaireRepository

_BLOOD_PACKAGE_CODES = ("METSIGHTS_BASIC", "METSIGHTS_PRO")

_PACKAGE_BLOOD_CATEGORY_KEYS: dict[str, tuple[str, ...]] = {
    "METSIGHTS_BASIC": (BLOOD_PARAMETER_CATEGORY_KEY,),
    "METSIGHTS_PRO": (BLOOD_PARAMETER_CATEGORY_KEY, ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY),
}


async def reload_blood_parameters_questions(db: AsyncSession) -> dict[str, Any]:
    """Delete and recreate blood parameter question definitions and Metsights wiring.

    Does not commit — caller is responsible for ``db.commit()``.
    """
    q_repo = QuestionnaireRepository()
    assessments_repo = AssessmentsRepository()

    stats: dict[str, Any] = {
        "questions_deleted": 0,
        "responses_deleted": 0,
        "questions_created": 0,
        "categories_created": 0,
        "categories_updated": 0,
        "question_links_total": 0,
        "links_added": 0,
        "package_links_added": 0,
        "package_links_total": 0,
        "missing_package_codes": [],
    }

    existing_ids: list[int] = []
    for question_key in ALL_BLOOD_PARAMETER_KEYS:
        definition = await q_repo.get_definition_by_key(db, question_key=question_key)
        if definition is not None:
            existing_ids.append(int(definition.question_id))

    if existing_ids:
        stats["responses_deleted"] = await q_repo.delete_responses_for_question_ids(db, existing_ids)
        for question_id in existing_ids:
            await q_repo.delete_definition_cascade(db, question_id)
        stats["questions_deleted"] = len(existing_ids)

    question_id_by_key: dict[str, int] = {}
    for field in BLOOD_PARAMETER_FIELDS:
        row = QuestionnaireDefinition(
            question_key=field.question_key,
            question_text=field.label,
            question_type="scale",
            is_required=field.required,
            is_read_only=False,
            help_text=field.help_text,
            metsights_sync=build_blood_parameter_metsights_sync(field.question_key),
            status="active",
        )
        row = await q_repo.create_definition(db, row)
        options = [
            {
                "option_value": code,
                "display_name": label,
                "tooltip_text": None,
            }
            for code, label in field.units
        ]
        await q_repo.replace_options_for_question(db, question_id=row.question_id, options=options)
        question_id_by_key[field.question_key] = int(row.question_id)
        stats["questions_created"] += 1

    category_specs = (
        (BLOOD_PARAMETER_CATEGORY_KEY, BLOOD_PARAMETER_CATEGORY_DISPLAY, BLOOD_PARAMETER_QUESTION_ORDER),
        (
            ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
            ADVANCED_BLOOD_PARAMETER_CATEGORY_DISPLAY,
            ADVANCED_BLOOD_PARAMETER_QUESTION_ORDER,
        ),
    )
    category_id_by_key: dict[str, int] = {}

    for category_key, display_name, ordered_keys in category_specs:
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
            category_id_by_key[category_key] = int(existing.category_id)

        category_id = category_id_by_key[category_key]
        await db.execute(
            delete(QuestionnaireCategoryQuestion).where(
                QuestionnaireCategoryQuestion.category_id == category_id
            )
        )
        await db.flush()

        links_before = set(
            await q_repo.get_assigned_question_ids_for_category_ordered(db, category_id=category_id)
        )

        question_ids = [
            question_id_by_key[key] for key in ordered_keys if key in question_id_by_key
        ]
        for index, question_id in enumerate(question_ids, start=1):
            db.add(
                QuestionnaireCategoryQuestion(
                    category_id=category_id,
                    question_id=question_id,
                    display_order=index,
                )
            )
        await db.flush()

        links_after = set(question_ids)
        stats["question_links_total"] += len(links_after)
        stats["links_added"] += len(links_after - links_before)

    for package_code in _BLOOD_PACKAGE_CODES:
        package = await assessments_repo.get_package_by_code(db, package_code=package_code)
        if package is None:
            stats["missing_package_codes"].append(package_code)
            continue

        package_blood_keys = _PACKAGE_BLOOD_CATEGORY_KEYS.get(package_code, ())
        base_categories = list(PACKAGE_METSIGHTS_CATEGORY_LINKS.get(package_code, []))
        for category_key in package_blood_keys:
            if category_key not in base_categories:
                base_categories.append(category_key)

        if package_code == "METSIGHTS_BASIC":
            advanced_category_id = category_id_by_key.get(ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY)
            if advanced_category_id is not None:
                await assessments_repo.delete_package_category_link(
                    db,
                    package_id=int(package.package_id),
                    category_id=advanced_category_id,
                )

        for index, category_key in enumerate(base_categories, start=1):
            category_id = category_id_by_key.get(category_key)
            if category_id is None:
                existing_cat = await q_repo.get_category_by_key_and_category_of(
                    db,
                    category_key=category_key,
                    category_of=METSIGHTS_CATEGORY_OF,
                )
                if existing_cat is None:
                    continue
                category_id = int(existing_cat.category_id)

            existing_link = await assessments_repo.get_package_category_link(
                db,
                package_id=int(package.package_id),
                category_id=category_id,
            )
            if existing_link is not None:
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

    return stats
