"""Questionnaire repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import delete, func, select, update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentPackageCategory
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireHealthyHabitRule,
    QuestionnaireOption,
    QuestionnaireResponse,
)


class QuestionnaireRepository:
    """Questionnaire database queries."""

    async def create_definition(self, db: AsyncSession, row: QuestionnaireDefinition) -> QuestionnaireDefinition:
        db.add(row)
        await db.flush()
        return row

    async def get_definition_by_id(self, db: AsyncSession, question_id: int) -> QuestionnaireDefinition | None:
        result = await db.execute(
            select(QuestionnaireDefinition).where(QuestionnaireDefinition.question_id == question_id)
        )
        return result.scalar_one_or_none()

    async def get_definitions_by_ids(
        self,
        db: AsyncSession,
        *,
        question_ids: list[int],
    ) -> dict[int, QuestionnaireDefinition]:
        if not question_ids:
            return {}
        result = await db.execute(
            select(QuestionnaireDefinition).where(QuestionnaireDefinition.question_id.in_(question_ids))
        )
        rows = list(result.scalars().all())
        return {int(r.question_id): r for r in rows}

    async def get_definition_by_key(self, db: AsyncSession, *, question_key: str) -> QuestionnaireDefinition | None:
        result = await db.execute(
            select(QuestionnaireDefinition).where(QuestionnaireDefinition.question_key == question_key)
        )
        return result.scalar_one_or_none()

    async def list_definitions(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        status: str | None = None,
        question_type: str | None = None,
    ) -> list[QuestionnaireDefinition]:
        offset = (page - 1) * limit

        query = select(QuestionnaireDefinition)
        if status is not None:
            query = query.where(QuestionnaireDefinition.status == status)
        if question_type is not None:
            query = query.where(QuestionnaireDefinition.question_type == question_type)
        query = query.order_by(QuestionnaireDefinition.question_id.desc()).offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_definitions(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        question_type: str | None = None,
    ) -> int:
        from sqlalchemy import func

        query = select(func.count()).select_from(QuestionnaireDefinition)
        if status is not None:
            query = query.where(QuestionnaireDefinition.status == status)
        if question_type is not None:
            query = query.where(QuestionnaireDefinition.question_type == question_type)
        result = await db.execute(query)
        return int(result.scalar_one())

    async def update_definition(self, db: AsyncSession, row: QuestionnaireDefinition) -> QuestionnaireDefinition:
        db.add(row)
        await db.flush()
        return row

    async def list_options_for_question(self, db: AsyncSession, *, question_id: int) -> list[QuestionnaireOption]:
        result = await db.execute(
            select(QuestionnaireOption)
            .where(QuestionnaireOption.question_id == question_id)
            .order_by(QuestionnaireOption.option_id.asc())
        )
        return list(result.scalars().all())

    async def list_options_for_question_ids(
        self,
        db: AsyncSession,
        *,
        question_ids: list[int],
    ) -> list[QuestionnaireOption]:
        if not question_ids:
            return []
        result = await db.execute(
            select(QuestionnaireOption)
            .where(QuestionnaireOption.question_id.in_(question_ids))
            .order_by(QuestionnaireOption.question_id.asc(), QuestionnaireOption.option_id.asc())
        )
        return list(result.scalars().all())

    async def replace_options_for_question(
        self,
        db: AsyncSession,
        *,
        question_id: int,
        options: list[dict],
    ) -> None:
        await db.execute(delete(QuestionnaireOption).where(QuestionnaireOption.question_id == question_id))
        for option in options:
            row = QuestionnaireOption(
                question_id=question_id,
                option_value=option["option_value"],
                display_name=option["display_name"],
                tooltip_text=option.get("tooltip_text"),
            )
            db.add(row)
        await db.flush()

    async def create_category(self, db: AsyncSession, row: QuestionnaireCategory) -> QuestionnaireCategory:
        db.add(row)
        await db.flush()
        return row

    async def get_category_by_id(self, db: AsyncSession, category_id: int) -> QuestionnaireCategory | None:
        result = await db.execute(
            select(QuestionnaireCategory).where(QuestionnaireCategory.category_id == category_id)
        )
        return result.scalar_one_or_none()

    async def get_category_by_key(self, db: AsyncSession, *, category_key: str) -> QuestionnaireCategory | None:
        result = await db.execute(
            select(QuestionnaireCategory).where(QuestionnaireCategory.category_key == category_key)
        )
        return result.scalar_one_or_none()

    async def list_categories(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
    ) -> list[QuestionnaireCategory]:
        offset = (page - 1) * limit
        result = await db.execute(
            select(QuestionnaireCategory)
            .order_by(QuestionnaireCategory.category_id.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(result.scalars().all())

    async def count_categories(self, db: AsyncSession) -> int:
        from sqlalchemy import func

        result = await db.execute(select(func.count()).select_from(QuestionnaireCategory))
        return int(result.scalar_one())

    async def update_category(self, db: AsyncSession, row: QuestionnaireCategory) -> QuestionnaireCategory:
        db.add(row)
        await db.flush()
        return row

    async def list_questions_by_category(
        self,
        db: AsyncSession,
        *,
        category_id: int,
    ) -> list[QuestionnaireDefinition]:
        result = await db.execute(
            select(QuestionnaireDefinition)
            .join(
                QuestionnaireCategoryQuestion,
                QuestionnaireCategoryQuestion.question_id == QuestionnaireDefinition.question_id,
            )
            .where(QuestionnaireCategoryQuestion.category_id == category_id)
            .order_by(
                QuestionnaireCategoryQuestion.display_order.asc().nulls_last(),
                QuestionnaireDefinition.question_id.asc(),
            )
        )
        return list(result.scalars().all())

    async def assign_questions_to_category(
        self,
        db: AsyncSession,
        *,
        category_id: int,
        question_ids: list[int],
    ) -> None:
        if not question_ids:
            return
        existing_result = await db.execute(
            select(QuestionnaireCategoryQuestion.question_id)
            .where(QuestionnaireCategoryQuestion.category_id == category_id)
            .where(QuestionnaireCategoryQuestion.question_id.in_(question_ids))
        )
        existing_question_ids = {int(qid) for qid in existing_result.scalars().all()}

        result = await db.execute(
            select(QuestionnaireDefinition.question_id).where(QuestionnaireDefinition.question_id.in_(question_ids))
        )
        valid_question_ids = {int(qid) for qid in result.scalars().all()}
        max_order_result = await db.execute(
            select(func.max(QuestionnaireCategoryQuestion.display_order)).where(
                QuestionnaireCategoryQuestion.category_id == category_id
            )
        )
        max_order = max_order_result.scalar_one_or_none()
        next_order = (int(max_order) if max_order is not None else 0) + 1

        for question_id in question_ids:
            if question_id not in valid_question_ids or question_id in existing_question_ids:
                continue
            db.add(
                QuestionnaireCategoryQuestion(
                    category_id=category_id,
                    question_id=question_id,
                    display_order=next_order,
                )
            )
            next_order += 1
        await db.flush()

    async def get_assigned_question_ids_for_category_ordered(
        self,
        db: AsyncSession,
        *,
        category_id: int,
    ) -> list[int]:
        result = await db.execute(
            select(QuestionnaireCategoryQuestion.question_id)
            .where(QuestionnaireCategoryQuestion.category_id == category_id)
            .order_by(QuestionnaireCategoryQuestion.display_order.asc().nulls_last(), QuestionnaireCategoryQuestion.id.asc())
        )
        return list(result.scalars().all())

    async def reorder_category_questions(
        self,
        db: AsyncSession,
        *,
        category_id: int,
        question_ids: list[int],
    ) -> None:
        for index, question_id in enumerate(question_ids, start=1):
            await db.execute(
                sql_update(QuestionnaireCategoryQuestion)
                .where(
                    QuestionnaireCategoryQuestion.category_id == category_id,
                    QuestionnaireCategoryQuestion.question_id == question_id,
                )
                .values(display_order=index)
            )
        await db.flush()

    async def remove_question_from_category(
        self,
        db: AsyncSession,
        *,
        category_id: int,
        question_id: int,
    ) -> bool:
        result = await db.execute(
            delete(QuestionnaireCategoryQuestion)
            .where(QuestionnaireCategoryQuestion.category_id == category_id)
            .where(QuestionnaireCategoryQuestion.question_id == question_id)
        )
        if int(result.rowcount or 0) == 0:
            return False
        await db.flush()
        return True

    async def get_response_by_instance_and_question(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        category_id: int,
        question_id: int,
    ) -> QuestionnaireResponse | None:
        result = await db.execute(
            select(QuestionnaireResponse)
            .where(QuestionnaireResponse.assessment_instance_id == assessment_instance_id)
            .where(QuestionnaireResponse.category_id == category_id)
            .where(QuestionnaireResponse.question_id == question_id)
        )
        return result.scalar_one_or_none()

    async def get_response_by_instance_and_question_id(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        question_id: int,
    ) -> QuestionnaireResponse | None:
        result = await db.execute(
            select(QuestionnaireResponse)
            .where(QuestionnaireResponse.assessment_instance_id == assessment_instance_id)
            .where(QuestionnaireResponse.question_id == question_id)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_responses_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        category_id: int | None = None,
    ) -> list[QuestionnaireResponse]:
        query = (
            select(QuestionnaireResponse)
            .where(QuestionnaireResponse.assessment_instance_id == assessment_instance_id)
            .order_by(QuestionnaireResponse.question_id.asc())
        )
        if category_id is not None:
            query = query.where(QuestionnaireResponse.category_id == category_id)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def list_responses_for_instances(
        self,
        db: AsyncSession,
        *,
        assessment_instance_ids: list[int],
    ) -> list[QuestionnaireResponse]:
        if not assessment_instance_ids:
            return []
        result = await db.execute(
            select(QuestionnaireResponse)
            .where(QuestionnaireResponse.assessment_instance_id.in_(assessment_instance_ids))
            .order_by(
                QuestionnaireResponse.assessment_instance_id.asc(),
                QuestionnaireResponse.question_id.asc(),
            )
        )
        return list(result.scalars().all())

    async def create_response(
        self,
        db: AsyncSession,
        row: QuestionnaireResponse,
    ) -> QuestionnaireResponse:
        db.add(row)
        await db.flush()
        return row

    async def update_response(
        self,
        db: AsyncSession,
        row: QuestionnaireResponse,
    ) -> QuestionnaireResponse:
        db.add(row)
        await db.flush()
        return row

    async def delete_responses_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> int:
        result = await db.execute(
            delete(QuestionnaireResponse).where(
                QuestionnaireResponse.assessment_instance_id == assessment_instance_id
            )
        )
        return int(result.rowcount or 0)

    async def list_question_ids_for_package(self, db: AsyncSession, *, package_id: int) -> list[int]:
        result = await db.execute(
            select(QuestionnaireCategoryQuestion.question_id)
            .join(
                AssessmentPackageCategory,
                AssessmentPackageCategory.category_id == QuestionnaireCategoryQuestion.category_id,
            )
            .where(AssessmentPackageCategory.package_id == package_id)
            .distinct()
        )
        return [int(qid) for qid in result.scalars().all()]

    async def list_healthy_habit_rules_for_question(
        self,
        db: AsyncSession,
        *,
        question_id: int,
    ) -> list[QuestionnaireHealthyHabitRule]:
        result = await db.execute(
            select(QuestionnaireHealthyHabitRule)
            .where(QuestionnaireHealthyHabitRule.question_id == question_id)
            .order_by(
                QuestionnaireHealthyHabitRule.display_order.asc().nulls_last(),
                QuestionnaireHealthyHabitRule.rule_id.asc(),
            )
        )
        return list(result.scalars().all())

    async def list_active_healthy_habit_rules_for_questions(
        self,
        db: AsyncSession,
        *,
        question_ids: list[int],
    ) -> list[QuestionnaireHealthyHabitRule]:
        if not question_ids:
            return []
        result = await db.execute(
            select(QuestionnaireHealthyHabitRule)
            .where(QuestionnaireHealthyHabitRule.question_id.in_(question_ids))
            .where(QuestionnaireHealthyHabitRule.status == "active")
            .order_by(
                QuestionnaireHealthyHabitRule.display_order.asc().nulls_last(),
                QuestionnaireHealthyHabitRule.rule_id.asc(),
            )
        )
        return list(result.scalars().all())

    async def get_healthy_habit_rule(
        self,
        db: AsyncSession,
        *,
        rule_id: int,
        question_id: int,
    ) -> QuestionnaireHealthyHabitRule | None:
        result = await db.execute(
            select(QuestionnaireHealthyHabitRule).where(
                QuestionnaireHealthyHabitRule.rule_id == rule_id,
                QuestionnaireHealthyHabitRule.question_id == question_id,
            )
        )
        return result.scalar_one_or_none()

    async def create_healthy_habit_rule(
        self,
        db: AsyncSession,
        row: QuestionnaireHealthyHabitRule,
    ) -> QuestionnaireHealthyHabitRule:
        db.add(row)
        await db.flush()
        return row

    async def update_healthy_habit_rule(
        self,
        db: AsyncSession,
        row: QuestionnaireHealthyHabitRule,
    ) -> QuestionnaireHealthyHabitRule:
        db.add(row)
        await db.flush()
        return row

    async def delete_healthy_habit_rule(self, db: AsyncSession, *, rule_id: int, question_id: int) -> bool:
        result = await db.execute(
            delete(QuestionnaireHealthyHabitRule).where(
                QuestionnaireHealthyHabitRule.rule_id == rule_id,
                QuestionnaireHealthyHabitRule.question_id == question_id,
            )
        )
        await db.flush()
        return int(result.rowcount or 0) > 0
