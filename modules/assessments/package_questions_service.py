"""Assessment package question linking service.

These endpoints are employee-only.

Rules:
- Only employees can manage package questions.
- Package must exist.
- Questions must exist.
- Links must be unique.
- All mutations must be audit logged.

This module owns the `assessment_package_questions` table.
It does not own questionnaire questions.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.models import AssessmentPackageQuestion
from modules.assessments.repository import AssessmentsRepository
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.questionnaire.service import QuestionnaireService


def _normalize_int(value: int) -> int:
    if not isinstance(value, int) or value <= 0:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    return value


class AssessmentPackageQuestionsService:
    """Business logic for assessment package question linking."""

    def __init__(
        self,
        repository: AssessmentsRepository,
        questionnaire_service: QuestionnaireService,
        audit_service: AuditService | None = None,
    ):
        self._repository = repository
        self._questionnaire_service = questionnaire_service
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(status_code=403, error_code="FORBIDDEN", message="You do not have permission to perform this action")

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def list_questions_for_package(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        package_id: int,
    ) -> list[dict]:
        self._ensure_employee_access(employee)

        package_id = _normalize_int(package_id)

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        links = await self._repository.list_package_questions(db, package_id=package_id)
        question_ids = [link.question_id for link in links]

        # We do not directly query questionnaire tables here.
        # We call questionnaire service for each question ID.
        # This keeps module boundaries strict.
        questions: list[dict] = []
        for question_id in question_ids:
            try:
                row = await self._questionnaire_service.get_question_definition(
                    db,
                    employee=employee,
                    question_id=question_id,
                )
            except AppError as exc:
                # If a question was removed from questionnaire module, we do not leak it.
                # We simply skip it.
                if exc.status_code == 404:
                    continue
                raise

            questions.append(
                {
                    "question_id": row.question_id,
                    "question_text": row.question_text,
                    "question_type": row.question_type,
                    "options": row.options,
                    "status": row.status,
                    "created_at": row.created_at,
                }
            )

        return questions

    async def add_questions_to_package(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        package_id: int,
        question_ids: list[int],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)

        package_id = _normalize_int(package_id)

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        if not isinstance(question_ids, list) or len(question_ids) == 0:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        normalized_ids: list[int] = []
        seen: set[int] = set()
        for raw in question_ids:
            question_id = _normalize_int(raw)
            if question_id in seen:
                continue
            seen.add(question_id)
            normalized_ids.append(question_id)

        added: list[int] = []
        skipped: list[int] = []

        for question_id in normalized_ids:
            # Validate question exists via questionnaire service.
            await self._questionnaire_service.get_question_definition(db, employee=employee, question_id=question_id)

            existing = await self._repository.get_package_question_link(db, package_id=package_id, question_id=question_id)
            if existing is not None:
                skipped.append(question_id)
                continue

            link = AssessmentPackageQuestion(package_id=package_id, question_id=question_id)
            await self._repository.create_package_question_link(db, link)
            added.append(question_id)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_ADD_ASSESSMENT_PACKAGE_QUESTIONS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {"package_id": package_id, "added_question_ids": added, "skipped_question_ids": skipped}

    async def remove_question_from_package(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        package_id: int,
        question_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)

        package_id = _normalize_int(package_id)
        question_id = _normalize_int(question_id)

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        deleted = await self._repository.delete_package_question_link(db, package_id=package_id, question_id=question_id)
        if deleted == 0:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_QUESTION_NOT_FOUND", message="Question is not attached to this package")

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_REMOVE_ASSESSMENT_PACKAGE_QUESTION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {"package_id": package_id, "removed_question_id": question_id}
