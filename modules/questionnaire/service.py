"""Questionnaire service."""

from __future__ import annotations

import math
from decimal import Decimal

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireDefinition, QuestionnaireHealthyHabitRule
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository
from modules.questionnaire.schemas import (
    HealthyHabitRuleCreateRequest,
    HealthyHabitRuleUpdateRequest,
    QuestionnaireCategoryCreateRequest,
    QuestionnaireCategoryQuestionsReorderRequest,
    QuestionnaireCategoryStatusUpdateRequest,
    QuestionnaireCategoryUpdateRequest,
    QuestionnaireQuestionCreateRequest,
    QuestionnaireQuestionStatusUpdateRequest,
    QuestionnaireQuestionUpdateRequest,
)

_ALLOWED_STATUS = {"active", "inactive", "archived"}
_ALLOWED_STATUS_UPDATE = {"active", "inactive"}
_CHOICE_TYPES = {"single_choice", "multiple_choice"}
_QUESTION_TYPE_ALIASES = {"multi_choice": "multiple_choice"}
_SCALE_TYPE = "scale"
_RULE_MATCH_MODES = {"all", "any"}
_RULE_TYPES = {"question_answer", "user_preference"}
_RULE_OPERATORS = {"equals", "not_equals", "contains", "not_contains", "in", "not_in"}
_PREFERENCE_KEYS = {"diet_preference", "allergies"}
_HABIT_CONDITION_OPTION = "option_match"
_HABIT_CONDITION_SCALE = "scale_range"
_ALLOWED_HABIT_RULE_STATUS = {"active", "inactive"}


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


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _normalize_question_type(value: str | None) -> str:
    normalized = _normalize(value)
    return _QUESTION_TYPE_ALIASES.get(normalized, normalized)


class QuestionnaireService:
    def __init__(
        self,
        repository: QuestionnaireRepository,
        users_repository: UsersRepository,
        audit_service: AuditService | None = None,
    ):
        self._repository = repository
        self._users_repository = users_repository
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(status_code=403, error_code="FORBIDDEN", message="You do not have permission to perform this action")

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    def _validate_exact_assignment_ids(self, *, requested_ids: list[int], assigned_ids: list[int], field_name: str) -> list[int]:
        requested_unique = list(dict.fromkeys(requested_ids))
        if len(requested_unique) != len(requested_ids):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"{field_name} contains duplicate ids",
            )
        if set(requested_unique) != set(assigned_ids):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"{field_name} must contain exactly the currently assigned ids",
            )
        return requested_unique

    def _normalize_visibility_rules(self, value: dict | None) -> dict | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        raw_match = _normalize_text(value.get("match") or "all")
        match_mode = raw_match if raw_match in _RULE_MATCH_MODES else None
        if match_mode is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        conditions = value.get("conditions")
        if not isinstance(conditions, list):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        normalized_conditions: list[dict] = []
        for raw_condition in conditions:
            if not isinstance(raw_condition, dict):
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            rule_type = _normalize_text(raw_condition.get("type"))
            operator = _normalize_text(raw_condition.get("operator") or "equals")
            if rule_type not in _RULE_TYPES or operator not in _RULE_OPERATORS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

            condition: dict = {"type": rule_type, "operator": operator}
            if rule_type == "question_answer":
                question_key = _normalize_text(raw_condition.get("question_key"))
                if not question_key:
                    raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
                condition["question_key"] = question_key
                condition["value"] = raw_condition.get("value")
            else:
                preference_key = _normalize_text(raw_condition.get("preference_key"))
                if preference_key not in _PREFERENCE_KEYS:
                    raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
                condition["preference_key"] = preference_key
                condition["value"] = raw_condition.get("value")
            normalized_conditions.append(condition)

        if len(normalized_conditions) == 0:
            return None

        return {"match": match_mode, "conditions": normalized_conditions}

    def _normalize_prefill_from(self, value: dict | None) -> dict | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        source = _normalize_text(value.get("source"))
        preference_key = _normalize_text(value.get("preference_key"))
        if source != "user_preference" or preference_key not in _PREFERENCE_KEYS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return {"source": "user_preference", "preference_key": preference_key}

    def _matches_operator(self, *, actual: object, expected: object, operator: str) -> bool:
        if operator == "equals":
            return _normalize_text(actual) == _normalize_text(expected)
        if operator == "not_equals":
            return _normalize_text(actual) != _normalize_text(expected)
        if operator == "contains":
            if isinstance(actual, list):
                return _normalize_text(expected) in {_normalize_text(item) for item in actual}
            return _normalize_text(expected) in _normalize_text(actual)
        if operator == "not_contains":
            if isinstance(actual, list):
                return _normalize_text(expected) not in {_normalize_text(item) for item in actual}
            return _normalize_text(expected) not in _normalize_text(actual)
        if operator == "in":
            if not isinstance(expected, list):
                return False
            return _normalize_text(actual) in {_normalize_text(item) for item in expected}
        if operator == "not_in":
            if not isinstance(expected, list):
                return False
            return _normalize_text(actual) not in {_normalize_text(item) for item in expected}
        return False

    def _evaluate_visibility_rules(
        self,
        *,
        visibility_rules: dict | None,
        answers_by_question_key: dict[str, object],
        preferences: dict[str, object],
    ) -> tuple[bool, str | None]:
        if not visibility_rules:
            return True, None

        conditions = visibility_rules.get("conditions") or []
        if not isinstance(conditions, list) or len(conditions) == 0:
            return True, None
        match_mode = _normalize_text(visibility_rules.get("match") or "all")
        if match_mode not in _RULE_MATCH_MODES:
            return False, "invalid_rule"

        results: list[bool] = []
        for condition in conditions:
            if not isinstance(condition, dict):
                results.append(False)
                continue
            condition_type = _normalize_text(condition.get("type"))
            operator = _normalize_text(condition.get("operator") or "equals")
            expected = condition.get("value")
            if condition_type == "question_answer":
                question_key = _normalize_text(condition.get("question_key"))
                actual = answers_by_question_key.get(question_key)
                results.append(self._matches_operator(actual=actual, expected=expected, operator=operator))
            elif condition_type == "user_preference":
                preference_key = _normalize_text(condition.get("preference_key"))
                actual = preferences.get(preference_key)
                results.append(self._matches_operator(actual=actual, expected=expected, operator=operator))
            else:
                results.append(False)

        is_visible = all(results) if match_mode == "all" else any(results)
        return (is_visible, None if is_visible else "visibility_rules_not_matched")

    def _resolve_prefill_answer(self, *, prefill_from: dict | None, preferences: dict[str, object]) -> object | None:
        if not prefill_from:
            return None
        if _normalize_text(prefill_from.get("source")) != "user_preference":
            return None
        preference_key = _normalize_text(prefill_from.get("preference_key"))
        if preference_key not in _PREFERENCE_KEYS:
            return None
        return preferences.get(preference_key)

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
            "visibility_rules": row.visibility_rules,
            "prefill_from": row.prefill_from,
            "status": row.status,
            "created_at": row.created_at,
        }

    async def _list_active_questions_for_category(
        self,
        db: AsyncSession,
        *,
        category_id: int,
    ) -> list[dict]:
        rows = await self._repository.list_questions_by_category(db, category_id=category_id)
        active_rows = [row for row in rows if (row.status or "").lower() == "active"]
        payloads: list[dict] = []
        for row in active_rows:
            question = await self._serialize_question(db, row)
            question["category_id"] = category_id
            payloads.append(question)
        return payloads

    def _build_preferences_map(self, preference_row) -> dict[str, object]:
        if preference_row is None:
            return {"diet_preference": None, "allergies": []}
        allergies = preference_row.allergies if isinstance(preference_row.allergies, list) else []
        return {
            "diet_preference": preference_row.diet_preference,
            "allergies": allergies,
        }

    def _compute_visibility_state(
        self,
        *,
        questions: list[dict],
        answers_by_question_id: dict[int, object],
        preferences: dict[str, object],
    ) -> dict[int, bool]:
        visibility: dict[int, bool] = {}
        answers_by_key: dict[str, object] = {}
        for question in questions:
            question_id = int(question["question_id"])
            question_key = _normalize_text(question.get("question_key"))
            visible, _ = self._evaluate_visibility_rules(
                visibility_rules=question.get("visibility_rules"),
                answers_by_question_key=answers_by_key,
                preferences=preferences,
            )
            visibility[question_id] = visible
            answer = answers_by_question_id.get(question_id)
            if answer is None:
                answer = self._resolve_prefill_answer(
                    prefill_from=question.get("prefill_from"),
                    preferences=preferences,
                )
            if question_key and answer is not None:
                answers_by_key[question_key] = answer
        return visibility

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
        if question_type == _SCALE_TYPE and len(options) == 0:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if question_type not in _CHOICE_TYPES and question_type != _SCALE_TYPE and len(options) > 0:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    def _validate_answer_by_type(self, *, question: dict, answer: object) -> None:
        question_type = _normalize_question_type(question.get("question_type"))
        if question_type != _SCALE_TYPE:
            return
        if not isinstance(answer, dict):
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Scale answer must be an object")
        value = answer.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Scale answer must include a valid number")
        unit = _normalize_text(answer.get("unit"))
        if not unit:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Scale answer must include a unit")
        allowed_units = {
            _normalize_text(option.get("option_value"))
            for option in (question.get("options") or [])
            if isinstance(option, dict)
        }
        if unit not in allowed_units:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Scale answer unit is not allowed")

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

        question_type = _normalize_question_type(payload.normalized_question_type())
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
        visibility_rules = self._normalize_visibility_rules(payload.visibility_rules)
        prefill_from = self._normalize_prefill_from(payload.prefill_from)
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
            visibility_rules=visibility_rules,
            prefill_from=prefill_from,
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
            normalized = _normalize_question_type(question_type)
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

        question_type = _normalize_question_type(payload.normalized_question_type())
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
        row.visibility_rules = self._normalize_visibility_rules(payload.visibility_rules)
        row.prefill_from = self._normalize_prefill_from(payload.prefill_from)

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
        return await self._list_active_questions_for_category(db, category_id=category_id)

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

    async def reorder_category_questions(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        category_id: int,
        payload: QuestionnaireCategoryQuestionsReorderRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        await self._ensure_category_exists(db, category_id=category_id)
        assigned_ids = await self._repository.get_assigned_question_ids_for_category_ordered(
            db,
            category_id=category_id,
        )
        ordered_ids = self._validate_exact_assignment_ids(
            requested_ids=payload.question_ids,
            assigned_ids=assigned_ids,
            field_name="question_ids",
        )
        await self._repository.reorder_category_questions(
            db,
            category_id=category_id,
            question_ids=ordered_ids,
        )
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REORDER_QUESTIONNAIRE_CATEGORY_QUESTIONS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return {"category_id": category_id, "question_ids": ordered_ids}

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

        questions = await self._list_active_questions_for_category(db, category_id=category_id)
        if not questions:
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
        preferences = self._build_preferences_map(
            await self._users_repository.get_preferences(db, user_id=user_id)
        )

        # Build response
        questions_with_answers = []
        visibility = self._compute_visibility_state(
            questions=questions,
            answers_by_question_id=responses_map,
            preferences=preferences,
        )
        answers_by_key: dict[str, object] = {}
        for question in questions:
            question_id = int(question["question_id"])
            question_key = _normalize_text(question.get("question_key"))
            is_visible, visibility_reason = self._evaluate_visibility_rules(
                visibility_rules=question.get("visibility_rules"),
                answers_by_question_key=answers_by_key,
                preferences=preferences,
            )
            # Keep deterministic behavior from computed visibility map for consistency.
            is_visible = visibility.get(question_id, is_visible)
            answer = responses_map.get(question_id)
            answer_source = "none"
            if answer is not None:
                answer_source = "draft"
            elif is_visible:
                prefill_answer = self._resolve_prefill_answer(
                    prefill_from=question.get("prefill_from"),
                    preferences=preferences,
                )
                if prefill_answer is not None:
                    answer = prefill_answer
                    answer_source = "prefill"

            if question_key and answer is not None:
                answers_by_key[question_key] = answer

            questions_with_answers.append(
                {
                    "question_id": question_id,
                    "question_text": question.get("question_text"),
                    "question_type": question.get("question_type"),
                    "question_key": question.get("question_key"),
                    "category_id": category_id,
                    "is_required": bool(question.get("is_required")),
                    "is_read_only": bool(question.get("is_read_only")),
                    "help_text": question.get("help_text"),
                    "options": question.get("options"),
                    "visibility_rules": question.get("visibility_rules"),
                    "prefill_from": question.get("prefill_from"),
                    "is_visible": is_visible,
                    "visibility_reason": visibility_reason,
                    "answer_source": answer_source,
                    "answer": answer,
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
        
        category_question_rows = await self._repository.list_questions_by_category(db, category_id=category_id)
        valid_question_ids = {int(row.question_id) for row in category_question_rows}
        category_questions = await self._list_active_questions_for_category(db, category_id=category_id)
        active_question_ids = {int(row["question_id"]) for row in category_questions}

        existing_responses = await self._repository.list_responses_for_instance(
            db,
            assessment_instance_id=instance.assessment_instance_id,
            category_id=category_id,
        )
        answers_for_visibility: dict[int, object] = {int(row.question_id): row.answer for row in existing_responses}
        incoming_answers: dict[int, object] = {}
        for response_item in responses:
            incoming_answers[int(response_item["question_id"])] = response_item["answer"]
        answers_for_visibility.update(incoming_answers)

        preferences = self._build_preferences_map(
            await self._users_repository.get_preferences(db, user_id=user_id)
        )
        visible_questions = self._compute_visibility_state(
            questions=category_questions,
            answers_by_question_id=answers_for_visibility,
            preferences=preferences,
        )
        questions_by_id = {int(row["question_id"]): row for row in category_questions}

        # Validate all question IDs and ensure they're active
        for response_item in responses:
            question_id = response_item["question_id"]

            if question_id not in valid_question_ids:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Question does not belong to this category",
                )
            if question_id not in active_question_ids:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Question is not available",
                )
            if not visible_questions.get(int(question_id), False):
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Question is not currently visible",
                )
            self._validate_answer_by_type(
                question=questions_by_id[int(question_id)],
                answer=response_item["answer"],
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

    def _serialize_healthy_habit_rule(self, row: QuestionnaireHealthyHabitRule) -> dict:
        scale_min = row.scale_min
        scale_max = row.scale_max
        return {
            "rule_id": int(row.rule_id),
            "question_id": int(row.question_id),
            "habit_key": (row.habit_key or "").strip() or None,
            "habit_label": row.habit_label or "",
            "display_order": int(row.display_order) if row.display_order is not None else None,
            "condition_type": row.condition_type or "",
            "matched_option_values": row.matched_option_values
            if isinstance(row.matched_option_values, list)
            else None,
            "scale_min": float(scale_min) if scale_min is not None else None,
            "scale_max": float(scale_max) if scale_max is not None else None,
            "scale_unit": (row.scale_unit or "").strip() or None,
            "status": row.status or "active",
            "created_at": row.created_at,
            "updated_employee_id": int(row.updated_employee_id) if row.updated_employee_id is not None else None,
        }

    def _prepare_healthy_habit_rule_fields(
        self,
        *,
        definition: QuestionnaireDefinition,
        payload: HealthyHabitRuleCreateRequest | HealthyHabitRuleUpdateRequest,
        option_rows: list,
    ) -> dict:
        qtype = _normalize_question_type(definition.question_type)
        if qtype == "text":
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Healthy habit rules are not supported for text questions",
            )
        ctype = payload.normalized_condition_type()
        if ctype not in {_HABIT_CONDITION_OPTION, _HABIT_CONDITION_SCALE}:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid condition_type")
        status = payload.normalized_status()
        if status not in _ALLOWED_HABIT_RULE_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid status")

        if ctype == _HABIT_CONDITION_OPTION:
            if qtype not in _CHOICE_TYPES:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="option_match requires a single_choice or multiple_choice question",
                )
            vals = payload.matched_option_values or []
            cleaned_vals = [str(v).strip() for v in vals if str(v).strip()]
            if not cleaned_vals:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="matched_option_values is required for option_match",
                )
            allowed = {(o.option_value or "").strip().lower() for o in option_rows}
            for v in cleaned_vals:
                if v.lower() not in allowed:
                    raise AppError(
                        status_code=400,
                        error_code="INVALID_INPUT",
                        message="matched_option_values must reference question option values",
                    )
            return {
                "condition_type": ctype,
                "matched_option_values": cleaned_vals,
                "scale_min": None,
                "scale_max": None,
                "scale_unit": None,
                "status": status,
            }

        if qtype != _SCALE_TYPE:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="scale_range requires a scale question",
            )
        if payload.scale_min is None or payload.scale_max is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="scale_min and scale_max are required for scale_range",
            )
        lo = float(payload.scale_min)
        hi = float(payload.scale_max)
        if not math.isfinite(lo) or not math.isfinite(hi):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid scale range")
        if lo > hi:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="scale_min cannot exceed scale_max")
        su = (payload.scale_unit or "").strip()
        if not su:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="scale_unit is required for scale_range",
            )
        allowed_units = {(o.option_value or "").strip().lower() for o in option_rows}
        if su.lower() not in allowed_units:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="scale_unit must be an allowed unit for this question",
            )
        return {
            "condition_type": ctype,
            "matched_option_values": None,
            "scale_min": Decimal(str(lo)),
            "scale_max": Decimal(str(hi)),
            "scale_unit": su,
            "status": status,
        }

    async def list_healthy_habit_rules(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        question_id: int,
    ) -> list[dict]:
        self._ensure_employee_access(employee)
        definition = await self._repository.get_definition_by_id(db, question_id)
        if definition is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_QUESTION_NOT_FOUND", message="Question not found")
        rows = await self._repository.list_healthy_habit_rules_for_question(db, question_id=question_id)
        return [self._serialize_healthy_habit_rule(r) for r in rows]

    async def create_healthy_habit_rule(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        question_id: int,
        payload: HealthyHabitRuleCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        definition = await self._repository.get_definition_by_id(db, question_id)
        if definition is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_QUESTION_NOT_FOUND", message="Question not found")
        option_rows = await self._repository.list_options_for_question(db, question_id=question_id)
        fields = self._prepare_healthy_habit_rule_fields(
            definition=definition,
            payload=payload,
            option_rows=option_rows,
        )
        habit_key = (payload.habit_key or "").strip() or None
        if habit_key and len(habit_key) > 200:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        row = QuestionnaireHealthyHabitRule(
            question_id=question_id,
            habit_key=habit_key,
            habit_label=(payload.habit_label or "").strip(),
            display_order=payload.display_order,
            updated_employee_id=employee.employee_id,
            **fields,
        )
        row = await self._repository.create_healthy_habit_rule(db, row)
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_QUESTIONNAIRE_HEALTHY_HABIT_RULE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._serialize_healthy_habit_rule(row)

    async def update_healthy_habit_rule(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        question_id: int,
        rule_id: int,
        payload: HealthyHabitRuleUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        definition = await self._repository.get_definition_by_id(db, question_id)
        if definition is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_QUESTION_NOT_FOUND", message="Question not found")
        existing = await self._repository.get_healthy_habit_rule(db, rule_id=rule_id, question_id=question_id)
        if existing is None:
            raise AppError(status_code=404, error_code="HEALTHY_HABIT_RULE_NOT_FOUND", message="Rule not found")
        option_rows = await self._repository.list_options_for_question(db, question_id=question_id)
        fields = self._prepare_healthy_habit_rule_fields(
            definition=definition,
            payload=payload,
            option_rows=option_rows,
        )
        habit_key = (payload.habit_key or "").strip() or None
        if habit_key and len(habit_key) > 200:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        existing.habit_key = habit_key
        existing.habit_label = (payload.habit_label or "").strip()
        existing.display_order = payload.display_order
        existing.condition_type = fields["condition_type"]
        existing.matched_option_values = fields["matched_option_values"]
        existing.scale_min = fields["scale_min"]
        existing.scale_max = fields["scale_max"]
        existing.scale_unit = fields["scale_unit"]
        existing.status = fields["status"]
        existing.updated_employee_id = employee.employee_id
        row = await self._repository.update_healthy_habit_rule(db, existing)
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_QUESTIONNAIRE_HEALTHY_HABIT_RULE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._serialize_healthy_habit_rule(row)

    async def delete_healthy_habit_rule(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        question_id: int,
        rule_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        definition = await self._repository.get_definition_by_id(db, question_id)
        if definition is None:
            raise AppError(status_code=404, error_code="QUESTIONNAIRE_QUESTION_NOT_FOUND", message="Question not found")
        deleted = await self._repository.delete_healthy_habit_rule(db, rule_id=rule_id, question_id=question_id)
        if not deleted:
            raise AppError(status_code=404, error_code="HEALTHY_HABIT_RULE_NOT_FOUND", message="Rule not found")
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_DELETE_QUESTIONNAIRE_HEALTHY_HABIT_RULE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
