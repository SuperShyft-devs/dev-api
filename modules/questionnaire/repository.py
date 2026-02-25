"""Questionnaire repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.questionnaire.models import QuestionnaireDefinition, QuestionnaireResponse


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

    async def get_response_by_instance_and_question(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        question_id: int,
    ) -> QuestionnaireResponse | None:
        """Get a specific response for an assessment instance and question."""
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
        """Get all responses for an assessment instance."""
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
        """Create a new questionnaire response."""
        db.add(row)
        await db.flush()
        return row

    async def update_response(
        self,
        db: AsyncSession,
        row: QuestionnaireResponse,
    ) -> QuestionnaireResponse:
        """Update an existing questionnaire response."""
        db.add(row)
        await db.flush()
        return row

    async def delete_responses_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> int:
        """Delete all responses for an assessment instance. Used for cleanup only."""
        result = await db.execute(
            delete(QuestionnaireResponse).where(
                QuestionnaireResponse.assessment_instance_id == assessment_instance_id
            )
        )
        return int(result.rowcount or 0)
