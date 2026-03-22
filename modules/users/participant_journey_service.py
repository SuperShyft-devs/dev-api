"""Employee read-only views of a participant's assessments and questionnaire state."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.questionnaire.models import QuestionnaireResponse
from modules.questionnaire.repository import QuestionnaireRepository
from modules.questionnaire.service import QuestionnaireService
from modules.users.repository import UsersRepository


def _dt_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


class ParticipantJourneyService:
    def __init__(
        self,
        users_repository: UsersRepository,
        assessments_repository: AssessmentsRepository,
        questionnaire_repository: QuestionnaireRepository,
        questionnaire_service: QuestionnaireService,
    ) -> None:
        self._users_repository = users_repository
        self._assessments_repository = assessments_repository
        self._questionnaire_repository = questionnaire_repository
        self._questionnaire_service = questionnaire_service

    def _ensure_employee_access(self, employee) -> None:
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    async def _ensure_user_exists(self, db: AsyncSession, user_id: int) -> None:
        user = await self._users_repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

    async def get_summary(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
        page: int,
        limit: int,
    ) -> tuple[dict, dict]:
        self._ensure_employee_access(employee)
        await self._ensure_user_exists(db, user_id)

        total = await self._assessments_repository.count_instances_for_user(db, user_id=user_id)
        rows = await self._assessments_repository.list_instances_for_user_with_engagement(
            db, user_id=user_id, page=page, limit=limit
        )

        instances_out: list[dict] = []
        for instance, package, engagement in rows:
            progress_rows = await self._assessments_repository.list_category_progress_for_instance(
                db, assessment_instance_id=instance.assessment_instance_id
            )
            responses = await self._questionnaire_repository.list_responses_for_instance(
                db,
                assessment_instance_id=instance.assessment_instance_id,
                category_id=None,
            )
            draft_count = sum(1 for r in responses if r.submitted_at is None)
            submitted_count = sum(1 for r in responses if r.submitted_at is not None)
            categories_touched = len({int(r.category_id) for r in responses})

            category_progress: list[dict] = []
            for pr in progress_rows:
                cat = await self._questionnaire_repository.get_category_by_id(db, int(pr.category_id))
                category_progress.append(
                    {
                        "category_id": int(pr.category_id),
                        "display_name": getattr(cat, "display_name", None) if cat else None,
                        "category_key": getattr(cat, "category_key", None) if cat else None,
                        "status": pr.status,
                        "completed_at": _dt_iso(pr.completed_at),
                    }
                )

            instances_out.append(
                {
                    "assessment_instance_id": instance.assessment_instance_id,
                    "status": instance.status,
                    "assigned_at": _dt_iso(instance.assigned_at),
                    "completed_at": _dt_iso(instance.completed_at),
                    "package_id": instance.package_id,
                    "package_code": getattr(package, "package_code", None) if package else None,
                    "package_display_name": getattr(package, "display_name", None) if package else None,
                    "engagement_id": instance.engagement_id,
                    "engagement_name": getattr(engagement, "engagement_name", None) if engagement else None,
                    "engagement_code": getattr(engagement, "engagement_code", None) if engagement else None,
                    "category_progress": category_progress,
                    "questionnaire": {
                        "response_count": len(responses),
                        "draft_count": draft_count,
                        "submitted_count": submitted_count,
                        "categories_touched": categories_touched,
                    },
                }
            )

        data = {"instances": instances_out}
        meta = {"page": page, "limit": limit, "total": total}
        return data, meta

    async def get_instance_detail(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
        assessment_instance_id: int,
    ) -> dict:
        self._ensure_employee_access(employee)
        await self._ensure_user_exists(db, user_id)

        row = await self._assessments_repository.get_instance_for_user_with_engagement(
            db,
            assessment_instance_id=assessment_instance_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment instance does not exist for this user",
            )

        instance, package, engagement = row

        responses = await self._questionnaire_repository.list_responses_for_instance(
            db,
            assessment_instance_id=assessment_instance_id,
            category_id=None,
        )
        resp_by_cat_q: dict[tuple[int, int], QuestionnaireResponse] = {}
        for r in responses:
            resp_by_cat_q[(int(r.category_id), int(r.question_id))] = r

        progress_rows = await self._assessments_repository.list_category_progress_for_instance(
            db, assessment_instance_id=assessment_instance_id
        )
        category_progress: list[dict] = []
        for pr in progress_rows:
            cat = await self._questionnaire_repository.get_category_by_id(db, int(pr.category_id))
            category_progress.append(
                {
                    "category_id": int(pr.category_id),
                    "display_name": getattr(cat, "display_name", None) if cat else None,
                    "category_key": getattr(cat, "category_key", None) if cat else None,
                    "status": pr.status,
                    "completed_at": _dt_iso(pr.completed_at),
                }
            )

        ordered_category_ids = await self._assessments_repository.get_assigned_category_ids_for_package_ordered(
            db, package_id=instance.package_id
        )

        categories_out: list[dict] = []
        for category_id in ordered_category_ids:
            cat = await self._questionnaire_repository.get_category_by_id(db, category_id)
            questions = await self._questionnaire_service.list_category_questions_for_user(
                db, category_id=category_id
            )
            questions_out: list[dict] = []
            for q in questions:
                qid = int(q["question_id"])
                key = (category_id, qid)
                resp_obj = resp_by_cat_q.get(key)
                if resp_obj is None:
                    answer_state = "empty"
                    answer = None
                    submitted_at = None
                else:
                    submitted_at = _dt_iso(resp_obj.submitted_at)
                    answer = resp_obj.answer
                    answer_state = "submitted" if resp_obj.submitted_at is not None else "draft"

                payload = {**q}
                payload["answer"] = answer
                payload["submitted_at"] = submitted_at
                payload["answer_state"] = answer_state
                questions_out.append(payload)

            categories_out.append(
                {
                    "category_id": category_id,
                    "display_name": getattr(cat, "display_name", None) if cat else None,
                    "category_key": getattr(cat, "category_key", None) if cat else None,
                    "questions": questions_out,
                }
            )

        return {
            "assessment_instance_id": instance.assessment_instance_id,
            "user_id": instance.user_id,
            "status": instance.status,
            "assigned_at": _dt_iso(instance.assigned_at),
            "completed_at": _dt_iso(instance.completed_at),
            "package": {
                "package_id": instance.package_id,
                "package_code": getattr(package, "package_code", None) if package else None,
                "package_display_name": getattr(package, "display_name", None) if package else None,
            },
            "engagement": {
                "engagement_id": instance.engagement_id,
                "engagement_name": getattr(engagement, "engagement_name", None) if engagement else None,
                "engagement_code": getattr(engagement, "engagement_code", None) if engagement else None,
            },
            "category_progress": category_progress,
            "categories": categories_out,
        }
