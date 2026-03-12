"""Questionnaire service."""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireDefinition
from modules.questionnaire.repository import QuestionnaireRepository
from modules.questionnaire.schemas import (
    QuestionnaireCategoryCreateRequest,
    QuestionnaireCategoryStatusUpdateRequest,
    QuestionnaireCategoryUpdateRequest,
    QuestionnaireQuestionCreateRequest,
    QuestionnaireQuestionStatusUpdateRequest,
    QuestionnaireQuestionUpdateRequest,
)

_ALLOWED_STATUS = {"active", "inactive", "archived"}
_ALLOWED_STATUS_UPDATE = {"active", "inactive"}
_CHOICE_TYPES = {"single_choice", "multiple_choice"}


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _clean_options(options: list[dict[str, str | None]] | None) -> list[dict[str, str | None]]:
    if not options:
        return []

    cleaned: list[dict[str, str | None]] = []
    seen_values: set[str] = set()
    for option in options:
        option_value = (option.get("option_value") or "").strip()
        display_name = (option.get("display_name") or "").strip()
        tooltip_text = (option.get("tooltip_text") or "").strip() or None
        if not option_value or len(option_value) > 200 or len(display_name) > 200:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        normalized = option_value.lower()
        if normalized in seen_values:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        seen_values.add(normalized)
        cleaned.append(
            {
                "option_value": option_value,
                "display_name": display_name,
                "tooltip_text": tooltip_text,
            }
        )

    if len(cleaned) > 200:
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

    async def _ensure_category_exists(self, db: AsyncSession, *, category_id: int) -> None:
        row = await self._repository.get_category_by_id(db, category_id)
        if row is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_CATEGORY_NOT_FOUND", message="Category does not exist")

    async def _serialize_question(self, db: AsyncSession, row: QuestionnaireDefinition) -> dict:
        options = await self._repository.list_options_for_question(db, question_id=row.question_id)
        serialized_options = [
            {
                "option_value": opt.option_value,
                "display_name": opt.display_name,
                "tooltip_text": opt.tooltip_text,
            }
            for opt in options
        ]
        return {
            "question_id": row.question_id,
            "question_key": row.question_key,
            "question_text": row.question_text,
            "question_type": row.question_type,
            "is_required": bool(row.is_required),
            "is_read_only": bool(row.is_read_only),
            "help_text": row.help_text,
            "options": serialized_options if serialized_options else None,
            "status": row.status,
            "created_at": row.created_at,
        }

    async def _resolve_instance_for_user_category(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        category_id: int,
    ):
        from modules.assessments.repository import AssessmentsRepository

        assessments_repo = AssessmentsRepository()
        instances = await assessments_repo.list_instances_for_user_category(
            db,
            user_id=user_id,
            category_id=category_id,
        )
        if not instances:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist",
            )

        active = [row for row in instances if (row.status or "").lower() == "active"]
        if active:
            return active[0]
        return instances[0]

    async def serialize_question_definition(self, db: AsyncSession, row: QuestionnaireDefinition) -> dict:
        return await self._serialize_question(db, row)

    def _validate_options_by_type(self, *, question_type: str, options: list[dict[str, str | None]]) -> None:
        if question_type in _CHOICE_TYPES and len(options) == 0:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if question_type not in _CHOICE_TYPES and len(options) > 0:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

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

        question_key = payload.normalized_question_key()
        if not question_key:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        status_value = payload.normalized_status()
        if status_value not in _ALLOWED_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        options = _clean_options(payload.options)
        self._validate_options_by_type(question_type=question_type, options=options)
        existing = await self._repository.get_definition_by_key(db, question_key=question_key)
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="QUESTIONNAIRE_QUESTION_KEY_EXISTS",
                message="Question key already exists",
            )

        row = QuestionnaireDefinition(
            question_key=question_key,
            question_text=question_text,
            question_type=question_type,
            is_required=payload.is_required,
            is_read_only=payload.is_read_only,
            help_text=(payload.help_text or "").strip() or None,
            status=status_value,
        )
        try:
            row = await self._repository.create_definition(db, row)
            await self._repository.replace_options_for_question(
                db,
                question_id=row.question_id,
                options=options,
            )
        except IntegrityError as exc:
            raise AppError(
                status_code=409,
                error_code="QUESTIONNAIRE_QUESTION_KEY_EXISTS",
                message="Question key already exists",
            ) from exc

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

        question_key = payload.normalized_question_key()
        if not question_key:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        existing = await self._repository.get_definition_by_key(db, question_key=question_key)
        if existing is not None and existing.question_id != row.question_id:
            raise AppError(
                status_code=409,
                error_code="QUESTIONNAIRE_QUESTION_KEY_EXISTS",
                message="Question key already exists",
            )

        row.question_text = question_text
        row.question_key = question_key
        row.question_type = question_type
        row.is_required = payload.is_required
        row.is_read_only = payload.is_read_only
        row.help_text = (payload.help_text or "").strip() or None

        cleaned_options = _clean_options(payload.options)
        self._validate_options_by_type(question_type=question_type, options=cleaned_options)
        try:
            row = await self._repository.update_definition(db, row)
            await self._repository.replace_options_for_question(
                db,
                question_id=row.question_id,
                options=cleaned_options,
            )
        except IntegrityError as exc:
            raise AppError(
                status_code=409,
                error_code="QUESTIONNAIRE_QUESTION_KEY_EXISTS",
                message="Question key already exists",
            ) from exc

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

    async def create_category(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: QuestionnaireCategoryCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> QuestionnaireCategory:
        self._ensure_employee_access(employee)
        category_key = payload.normalized_category_key()
        display_name = payload.normalized_display_name()
        if not category_key or not display_name:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        existing = await self._repository.get_category_by_key(db, category_key=category_key)
        if existing is not None:
            raise AppError(status_code=409, error_code="QUESTIONNAIRE_CATEGORY_EXISTS", message="Category already exists")
        row = QuestionnaireCategory(category_key=category_key, display_name=display_name, status="active")
        row = await self._repository.create_category(db, row)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_QUESTIONNAIRE_CATEGORY",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return row

    async def list_categories(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
    ) -> tuple[list[QuestionnaireCategory], int]:
        self._ensure_employee_access(employee)
        rows = await self._repository.list_categories(db, page=page, limit=limit)
        total = await self._repository.count_categories(db)
        return rows, total

    async def get_category(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        category_id: int,
    ) -> QuestionnaireCategory:
        self._ensure_employee_access(employee)
        row = await self._repository.get_category_by_id(db, category_id)
        if row is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_CATEGORY_NOT_FOUND", message="Category does not exist")
        return row

    async def update_category(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        category_id: int,
        payload: QuestionnaireCategoryUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> QuestionnaireCategory:
        self._ensure_employee_access(employee)
        row = await self.get_category(db, employee=employee, category_id=category_id)
        category_key = payload.normalized_category_key()
        display_name = payload.normalized_display_name()
        existing = await self._repository.get_category_by_key(db, category_key=category_key)
        if existing is not None and existing.category_id != row.category_id:
            raise AppError(status_code=409, error_code="QUESTIONNAIRE_CATEGORY_EXISTS", message="Category already exists")
        row.category_key = category_key
        row.display_name = display_name
        row = await self._repository.update_category(db, row)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_QUESTIONNAIRE_CATEGORY",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return row

    async def change_category_status(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        category_id: int,
        payload: QuestionnaireCategoryStatusUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> QuestionnaireCategory:
        self._ensure_employee_access(employee)
        row = await self.get_category(db, employee=employee, category_id=category_id)
        normalized = payload.normalized_status()
        if normalized not in _ALLOWED_STATUS_UPDATE:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        row.status = normalized
        row = await self._repository.update_category(db, row)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_QUESTIONNAIRE_CATEGORY_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return row

    async def list_category_questions(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        category_id: int,
    ) -> list[dict]:
        self._ensure_employee_access(employee)
        await self._ensure_category_exists(db, category_id=category_id)
        rows = await self._repository.list_questions_by_category(db, category_id=category_id)
        data: list[dict] = []
        for row in rows:
            payload = await self._serialize_question(db, row)
            payload["category_id"] = category_id
            data.append(payload)
        return data

    async def list_category_questions_for_user(
        self,
        db: AsyncSession,
        *,
        category_id: int,
    ) -> list[dict]:
        await self._ensure_category_exists(db, category_id=category_id)
        rows = await self._repository.list_questions_by_category(db, category_id=category_id)
        active_rows = [row for row in rows if (row.status or "").lower() == "active"]
        data: list[dict] = []
        for row in active_rows:
            payload = await self._serialize_question(db, row)
            payload["category_id"] = category_id
            data.append(payload)
        return data

    async def assign_category_questions(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        category_id: int,
        question_ids: list[int],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        await self._ensure_category_exists(db, category_id=category_id)
        if not question_ids:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        normalized = [qid for qid in question_ids if isinstance(qid, int) and qid > 0]
        if len(normalized) != len(question_ids):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        await self._repository.assign_questions_to_category(db, category_id=category_id, question_ids=normalized)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_ASSIGN_QUESTIONNAIRE_CATEGORY_QUESTIONS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return {"category_id": category_id, "question_ids": normalized}

    async def remove_category_question(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        category_id: int,
        question_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        await self._ensure_category_exists(db, category_id=category_id)
        ok = await self._repository.remove_question_from_category(
            db,
            category_id=category_id,
            question_id=question_id,
        )
        if not ok:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_CATEGORY_QUESTION_NOT_FOUND", message="Question not mapped")
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REMOVE_QUESTIONNAIRE_CATEGORY_QUESTION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return {"category_id": category_id, "question_id": question_id}

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
        category_id: int,
    ) -> dict:
        """Get category questionnaire questions and existing draft answers for a user."""
        instance = await self._resolve_instance_for_user_category(
            db,
            user_id=user_id,
            category_id=category_id,
        )

        questions = await self._repository.list_questions_by_category(db, category_id=category_id)
        active_questions = [q for q in questions if (q.status or "").lower() == "active"]
        if not active_questions:
            return {
                "assessment_instance_id": instance.assessment_instance_id,
                "status": instance.status or "active",
                "questions": [],
            }

        # Get existing responses
        responses = await self._repository.list_responses_for_instance(
            db,
            assessment_instance_id=instance.assessment_instance_id,
        )
        responses_map = {r.question_id: r.answer for r in responses}

        # Build response
        questions_with_answers = []
        for question in active_questions:
            serialized_options = [
                {
                    "option_value": opt.option_value,
                    "display_name": opt.display_name,
                    "tooltip_text": opt.tooltip_text,
                }
                for opt in await self._repository.list_options_for_question(db, question_id=question.question_id)
            ]
            questions_with_answers.append(
                {
                    "question_id": question.question_id,
                    "question_text": question.question_text,
                    "question_type": question.question_type,
                    "question_key": question.question_key,
                    "category_id": category_id,
                    "is_required": bool(question.is_required),
                    "is_read_only": bool(question.is_read_only),
                    "help_text": question.help_text,
                    "options": serialized_options if serialized_options else None,
                    "answer": responses_map.get(question.question_id),
                }
            )

        return {
            "assessment_instance_id": instance.assessment_instance_id,
            "status": instance.status or "active",
            "questions": questions_with_answers,
        }

    async def upsert_responses_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        category_id: int,
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
        from modules.questionnaire.models import QuestionnaireResponse

        instance = await self._resolve_instance_for_user_category(
            db,
            user_id=user_id,
            category_id=category_id,
        )

        # Check if already completed
        current_status = (instance.status or "").lower()
        if current_status == "completed":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment is already completed"
            )
        
        category_questions = await self._repository.list_questions_by_category(db, category_id=category_id)
        valid_question_ids = {int(row.question_id) for row in category_questions}

        # Validate all question IDs and ensure they're active
        for response_item in responses:
            question_id = response_item["question_id"]

            if question_id not in valid_question_ids:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Question does not belong to this category",
                )

            # Verify question is active
            question = await self._repository.get_definition_by_id(db, question_id)
            if question is None or (question.status or "").lower() != "active":
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Question is not available",
                )

        # Upsert responses
        for response_item in responses:
            question_id = response_item["question_id"]
            answer = response_item["answer"]

            existing = await self._repository.get_response_by_instance_and_question(
                db,
                assessment_instance_id=instance.assessment_instance_id,
                category_id=category_id,
                question_id=question_id,
            )

            if existing is not None:
                # Update existing response (draft mode)
                existing.answer = answer
                existing.submitted_at = None  # Draft responses don't have submission time
                await self._repository.update_response(db, existing)
            else:
                # Create new response
                new_response = QuestionnaireResponse(
                    assessment_instance_id=instance.assessment_instance_id,
                    question_id=question_id,
                    category_id=category_id,
                    answer=answer,
                    submitted_at=None,
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
        from modules.assessments.models import AssessmentCategoryProgress
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

        package_categories = await assessments_repo.list_package_categories(db, package_id=instance.package_id)
        for link in package_categories:
            progress = await assessments_repo.get_category_progress(
                db,
                assessment_instance_id=assessment_instance_id,
                category_id=link.category_id,
            )
            if progress is None:
                progress = AssessmentCategoryProgress(
                    assessment_instance_id=assessment_instance_id,
                    category_id=link.category_id,
                    status="complete",
                    completed_at=now,
                )
                await assessments_repo.create_category_progress(db, progress)
            else:
                progress.status = "complete"
                progress.completed_at = now
                await assessments_repo.update_category_progress(db, progress)
        
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
