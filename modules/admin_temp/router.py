"""Temporary admin endpoints for one-off data fixes.

These endpoints are NOT meant for production use. Remove this module once
the server data is consistent with the seed definitions.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from db.session import get_db
from modules.assessments.models import AssessmentPackageCategory
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireOption,
)
from db.seed.metsights_questionnaire_data import (
    METSIGHTS_CATEGORIES,
    METSIGHTS_CATEGORY_QUESTIONS,
    METSIGHTS_OPTIONS,
    METSIGHTS_PACKAGE_CATEGORIES,
    METSIGHTS_QUESTIONS,
)

router = APIRouter(prefix="/admin-temp", tags=["admin-temp"])


@router.post("/sync-questionnaire-seed")
async def sync_questionnaire_seed(
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_current_user),
):
    """Upsert all questionnaire seed data (definitions, categories, options,
    category-question links, package-category links) so the server DB matches
    the codebase seed exactly."""

    stats: dict[str, int] = {
        "categories_upserted": 0,
        "questions_upserted": 0,
        "options_replaced": 0,
        "category_question_links_upserted": 0,
        "package_category_links_upserted": 0,
    }

    # --- Categories ---
    for sc in METSIGHTS_CATEGORIES:
        existing = await db.execute(
            select(QuestionnaireCategory).where(
                QuestionnaireCategory.category_id == sc.category_id
            )
        )
        row = existing.scalar_one_or_none()
        if row is None:
            db.add(
                QuestionnaireCategory(
                    category_id=sc.category_id,
                    category_key=sc.category_key,
                    display_name=sc.display_name,
                    status=sc.status,
                )
            )
        else:
            row.category_key = sc.category_key
            row.display_name = sc.display_name
            row.status = sc.status
        stats["categories_upserted"] += 1
    await db.flush()

    # --- Questions ---
    for sq in METSIGHTS_QUESTIONS:
        existing = await db.execute(
            select(QuestionnaireDefinition).where(
                QuestionnaireDefinition.question_id == sq.question_id
            )
        )
        row = existing.scalar_one_or_none()
        if row is None:
            db.add(
                QuestionnaireDefinition(
                    question_id=sq.question_id,
                    question_key=sq.question_key,
                    question_text=sq.question_text,
                    question_type=sq.question_type,
                    is_required=sq.is_required,
                    is_read_only=sq.is_read_only,
                    help_text=sq.help_text,
                    status=sq.status,
                )
            )
        else:
            row.question_key = sq.question_key
            row.question_text = sq.question_text
            row.question_type = sq.question_type
            row.is_required = sq.is_required
            row.is_read_only = sq.is_read_only
            row.help_text = sq.help_text
            row.status = sq.status
        stats["questions_upserted"] += 1
    await db.flush()

    # --- Options (delete + re-insert per question) ---
    seen_question_ids: set[int] = set()
    for so in METSIGHTS_OPTIONS:
        if so.question_id not in seen_question_ids:
            await db.execute(
                delete(QuestionnaireOption).where(
                    QuestionnaireOption.question_id == so.question_id
                )
            )
            seen_question_ids.add(so.question_id)
        db.add(
            QuestionnaireOption(
                question_id=so.question_id,
                option_value=so.option_value,
                display_name=so.display_name,
                tooltip_text=so.tooltip_text,
            )
        )
        stats["options_replaced"] += 1
    await db.flush()

    # --- Category ↔ Question links ---
    for scq in METSIGHTS_CATEGORY_QUESTIONS:
        existing = await db.execute(
            select(QuestionnaireCategoryQuestion).where(
                QuestionnaireCategoryQuestion.category_id == scq.category_id,
                QuestionnaireCategoryQuestion.question_id == scq.question_id,
            )
        )
        if existing.scalar_one_or_none() is None:
            db.add(
                QuestionnaireCategoryQuestion(
                    category_id=scq.category_id,
                    question_id=scq.question_id,
                )
            )
        stats["category_question_links_upserted"] += 1
    await db.flush()

    # --- Package ↔ Category links ---
    for spc in METSIGHTS_PACKAGE_CATEGORIES:
        existing = await db.execute(
            select(AssessmentPackageCategory).where(
                AssessmentPackageCategory.package_id == spc.package_id,
                AssessmentPackageCategory.category_id == spc.category_id,
            )
        )
        if existing.scalar_one_or_none() is None:
            db.add(
                AssessmentPackageCategory(
                    package_id=spc.package_id,
                    category_id=spc.category_id,
                )
            )
        stats["package_category_links_upserted"] += 1
    await db.flush()

    await db.commit()
    return success_response(stats)
