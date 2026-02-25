"""Questionnaire service.

These endpoints are employee-only.

This module only manages question definitions.
It does not score answers.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.questionnaire.models import QuestionnaireDefinition
from modules.questionnaire.repository import QuestionnaireRepository
from modules.questionnaire.schemas import (
    QuestionnaireQuestionCreateRequest,
    QuestionnaireQuestionStatusUpdateRequest,
    QuestionnaireQuestionUpdateRequest,
)

_ALLOWED_STATUS = {"active", "inactive", "archived"}
_ALLOWED_STATUS_UPDATE = {"active", "inactive"}


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _clean_options(options: list[str] | None) -> list[str] | None:
    if options is None:
        return None

    cleaned: list[str] = []
    for option in options:
        value = (option or "").strip()
        if not value:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if len(value) > 200:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        cleaned.append(value)

    if len(cleaned) > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    return cleaned


class QuestionnaireService:
    def __init__(self, repository: QuestionnaireRepository, audit_service: AuditService | None = None):
        self._repository = repository
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(status_code=403, error_code="FORBIDDEN", message="You do not have permission to perform this action")

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def create_question_definition(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: QuestionnaireQuestionCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> QuestionnaireDefinition:
        self._ensure_employee_access(employee)

        question_text = payload.normalized_question_text()
        if not question_text:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        question_type = payload.normalized_question_type()
        if not question_type:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        status_value = payload.normalized_status()
        if status_value not in _ALLOWED_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        options = _clean_options(payload.options)

        row = QuestionnaireDefinition(
            question_text=question_text,
            question_type=question_type,
            options=options,
            status=status_value,
        )
        row = await self._repository.create_definition(db, row)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_QUESTIONNAIRE_QUESTION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row

    async def list_question_definitions(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        status: str | None,
        question_type: str | None,
    ) -> tuple[list[QuestionnaireDefinition], int]:
        self._ensure_employee_access(employee)

        status_value = None
        if status is not None:
            normalized = _normalize(status)
            if normalized not in _ALLOWED_STATUS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            status_value = normalized

        type_value = None
        if question_type is not None:
            normalized = _normalize(question_type)
            if not normalized:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            type_value = normalized

        rows = await self._repository.list_definitions(
            db,
            page=page,
            limit=limit,
            status=status_value,
            question_type=type_value,
        )
        total = await self._repository.count_definitions(db, status=status_value, question_type=type_value)
        return rows, total

    async def get_question_definition(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        question_id: int,
    ) -> QuestionnaireDefinition:
        self._ensure_employee_access(employee)

        row = await self._repository.get_definition_by_id(db, question_id)
        if row is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_QUESTION_NOT_FOUND", message="Question does not exist")

        return row

    async def update_question_definition(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        question_id: int,
        payload: QuestionnaireQuestionUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> QuestionnaireDefinition:
        self._ensure_employee_access(employee)

        row = await self._repository.get_definition_by_id(db, question_id)
        if row is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_QUESTION_NOT_FOUND", message="Question does not exist")

        question_text = payload.normalized_question_text()
        if not question_text:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        question_type = payload.normalized_question_type()
        if not question_type:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        row.question_text = question_text
        row.question_type = question_type
        row.options = _clean_options(payload.options)

        row = await self._repository.update_definition(db, row)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_QUESTIONNAIRE_QUESTION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row

    async def change_question_status(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        question_id: int,
        payload: QuestionnaireQuestionStatusUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> QuestionnaireDefinition:
        self._ensure_employee_access(employee)

        row = await self._repository.get_definition_by_id(db, question_id)
        if row is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_QUESTION_NOT_FOUND", message="Question does not exist")

        normalized = payload.normalized_status()
        if normalized not in _ALLOWED_STATUS_UPDATE:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        row.status = normalized
        row = await self._repository.update_definition(db, row)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_QUESTIONNAIRE_QUESTION_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row

    # User-facing methods for questionnaire responses

    async def get_questionnaire_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        assessment_instance_id: int,
    ) -> dict:
        """Get questionnaire questions and existing draft answers for a user.
        
        Security: Validates that the assessment instance belongs to the user.
        Returns questions linked to the assessment package with existing answers.
        """
        from modules.assessments.repository import AssessmentsRepository
        from modules.questionnaire.models import QuestionnaireResponse

        assessments_repo = AssessmentsRepository()
        
        # Validate ownership and get instance
        instance = await assessments_repo.get_instance_by_id(db, assessment_instance_id)
        if instance is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist"
            )
        
        if instance.user_id != user_id:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action"
            )
        
        # Get package questions
        package_questions = await assessments_repo.list_package_questions(
            db,
            package_id=instance.package_id
        )
        
        question_ids = [pq.question_id for pq in package_questions]
        
        if not question_ids:
            return {
                "assessment_instance_id": assessment_instance_id,
                "status": instance.status or "active",
                "questions": []
            }
        
        # Get question definitions (only active ones)
        questions_map = {}
        for question_id in question_ids:
            question = await self._repository.get_definition_by_id(db, question_id)
            if question is not None and (question.status or "").lower() == "active":
                questions_map[question_id] = question
        
        # Get existing responses
        responses = await self._repository.list_responses_for_instance(
            db,
            assessment_instance_id=assessment_instance_id
        )
        
        responses_map = {r.question_id: r.answer for r in responses}
        
        # Build response
        questions_with_answers = []
        for question_id in question_ids:
            question = questions_map.get(question_id)
            if question is None:
                continue
            
            questions_with_answers.append({
                "question_id": question.question_id,
                "question_text": question.question_text,
                "question_type": question.question_type,
                "options": question.options,
                "answer": responses_map.get(question_id)
            })
        
        return {
            "assessment_instance_id": assessment_instance_id,
            "status": instance.status or "active",
            "questions": questions_with_answers
        }

    async def upsert_responses_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        assessment_instance_id: int,
        responses: list[dict],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        """Create or update draft answers for a user.
        
        Security: Validates ownership and that assessment is not completed.
        Business rules:
        - Assessment must be active (not completed)
        - Questions must belong to the assessment package
        - Questions must be active
        - Responses are stored as JSON (no interpretation)
        """
        from modules.assessments.repository import AssessmentsRepository
        from modules.questionnaire.models import QuestionnaireResponse

        assessments_repo = AssessmentsRepository()
        
        # Validate ownership and get instance
        instance = await assessments_repo.get_instance_by_id(db, assessment_instance_id)
        if instance is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist"
            )
        
        if instance.user_id != user_id:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action"
            )
        
        # Check if already completed
        current_status = (instance.status or "").lower()
        if current_status == "completed":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment is already completed"
            )
        
        # Get valid question IDs for this package
        package_questions = await assessments_repo.list_package_questions(
            db,
            package_id=instance.package_id
        )
        valid_question_ids = {pq.question_id for pq in package_questions}
        
        # Validate all question IDs and ensure they're active
        for response_item in responses:
            question_id = response_item["question_id"]
            
            if question_id not in valid_question_ids:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Question does not belong to this assessment"
                )
            
            # Verify question is active
            question = await self._repository.get_definition_by_id(db, question_id)
            if question is None or (question.status or "").lower() != "active":
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Question is not available"
                )
        
        # Upsert responses
        for response_item in responses:
            question_id = response_item["question_id"]
            answer = response_item["answer"]
            
            existing = await self._repository.get_response_by_instance_and_question(
                db,
                assessment_instance_id=assessment_instance_id,
                question_id=question_id
            )
            
            if existing is not None:
                # Update existing response (draft mode)
                existing.answer = answer
                existing.submitted_at = None  # Draft responses don't have submission time
                await self._repository.update_response(db, existing)
            else:
                # Create new response
                new_response = QuestionnaireResponse(
                    assessment_instance_id=assessment_instance_id,
                    question_id=question_id,
                    answer=answer,
                    submitted_at=None
                )
                await self._repository.create_response(db, new_response)
        
        # Audit log
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="USER_UPDATE_QUESTIONNAIRE_RESPONSES",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

    async def submit_questionnaire_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        assessment_instance_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        """Submit questionnaire and mark assessment as completed.
        
        Security: Validates ownership.
        Business rules:
        - Assessment must be active
        - Marks all responses with submission timestamp
        - Changes assessment status to completed
        - Triggers Metsights (placeholder for now)
        """
        from datetime import datetime, timezone
        from modules.assessments.repository import AssessmentsRepository

        assessments_repo = AssessmentsRepository()
        
        # Validate ownership and get instance
        instance = await assessments_repo.get_instance_by_id(db, assessment_instance_id)
        if instance is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist"
            )
        
        if instance.user_id != user_id:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action"
            )
        
        # Check if already completed
        current_status = (instance.status or "").lower()
        if current_status == "completed":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment is already completed"
            )
        
        if current_status != "active":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment is not active"
            )
        
        # Mark all responses as submitted
        now = datetime.now(timezone.utc)
        responses = await self._repository.list_responses_for_instance(
            db,
            assessment_instance_id=assessment_instance_id
        )
        
        for response in responses:
            response.submitted_at = now
            await self._repository.update_response(db, response)
        
        # Mark assessment as completed
        instance.status = "completed"
        instance.completed_at = now
        await assessments_repo.update_instance(db, instance)
        
        # Audit log
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="USER_SUBMIT_QUESTIONNAIRE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )
        
        # TODO: Trigger Metsights integration
        # This will be implemented later - placeholder for now
        # await self._trigger_metsights(db, assessment_instance_id)
