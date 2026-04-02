"""Load and compute top healthy habits for an assessment (reports)."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from modules.questionnaire.healthy_habits_eval import HealthyHabitComputed, compute_top_healthy_habits
from modules.questionnaire.repository import QuestionnaireRepository


class HealthyHabitsService:
    def __init__(self, repository: QuestionnaireRepository):
        self._repository = repository

    async def top_habits_for_assessment(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        package_id: int,
        limit: int = 3,
    ) -> list[HealthyHabitComputed]:
        question_ids = await self._repository.list_question_ids_for_package(db, package_id=package_id)
        if not question_ids:
            return []
        rules = await self._repository.list_active_healthy_habit_rules_for_questions(
            db,
            question_ids=question_ids,
        )
        if not rules:
            return []
        definitions = await self._repository.get_definitions_by_ids(db, question_ids=question_ids)
        responses = await self._repository.list_responses_for_instance(
            db,
            assessment_instance_id=assessment_instance_id,
        )
        question_id_set = set(question_ids)
        answers_by_question_id: dict[int, object] = {}
        for r in responses:
            qid = int(r.question_id)
            if qid in question_id_set:
                answers_by_question_id[qid] = r.answer
        return compute_top_healthy_habits(
            rules=rules,
            definitions_by_id=definitions,
            answers_by_question_id=answers_by_question_id,
            limit=limit,
        )
