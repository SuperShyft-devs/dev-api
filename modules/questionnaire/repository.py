"""Questionnaire repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireDefinition,
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
        category_id: int | None = None,
    ) -> list[QuestionnaireDefinition]:
        offset = (page - 1) * limit

        query = select(QuestionnaireDefinition)
        if status is not None:
            query = query.where(QuestionnaireDefinition.status == status)
        if question_type is not None:
            query = query.where(QuestionnaireDefinition.question_type == question_type)
        if category_id is not None:
            query = query.where(QuestionnaireDefinition.category_id == category_id)

        query = query.order_by(QuestionnaireDefinition.question_id.desc()).offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_definitions(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        question_type: str | None = None,
        category_id: int | None = None,
    ) -> int:
        from sqlalchemy import func

        query = select(func.count()).select_from(QuestionnaireDefinition)
        if status is not None:
            query = query.where(QuestionnaireDefinition.status == status)
        if question_type is not None:
            query = query.where(QuestionnaireDefinition.question_type == question_type)
        if category_id is not None:
            query = query.where(QuestionnaireDefinition.category_id == category_id)

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
            .where(QuestionnaireDefinition.category_id == category_id)
            .order_by(QuestionnaireDefinition.question_id.asc())
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
        result = await db.execute(
            select(QuestionnaireDefinition).where(QuestionnaireDefinition.question_id.in_(question_ids))
        )
        rows = list(result.scalars().all())
        rows_by_id = {row.question_id: row for row in rows}
        for question_id in question_ids:
            row = rows_by_id.get(question_id)
            if row is not None:
                row.category_id = category_id
                db.add(row)
        await db.flush()

    async def remove_question_from_category(
        self,
        db: AsyncSession,
        *,
        category_id: int,
        question_id: int,
    ) -> bool:
        row = await self.get_definition_by_id(db, question_id)
        if row is None or row.category_id != category_id:
            return False
        row.category_id = None
        db.add(row)
        await db.flush()
        return True

    async def get_response_by_instance_and_question(
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
        )
        return result.scalar_one_or_none()

    async def list_responses_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> list[QuestionnaireResponse]:
        result = await db.execute(
            select(QuestionnaireResponse)
            .where(QuestionnaireResponse.assessment_instance_id == assessment_instance_id)
            .order_by(QuestionnaireResponse.question_id.asc())
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
