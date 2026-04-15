"""Experts business logic."""

from __future__ import annotations

from decimal import Decimal

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.experts.models import Expert, ExpertExpertiseTag, ExpertReview
from modules.experts.repository import ExpertsRepository
from modules.experts.schemas import (
    ExpertCreateRequest,
    ExpertReviewCreateRequest,
    ExpertTagCreateRequest,
    ExpertUpdateRequest,
)
from modules.users.repository import UsersRepository


_ALLOWED_STATUS = {"active", "inactive"}
_ALLOWED_MODES = {"video", "voice", "chat"}


class ExpertsService:
    def __init__(self, repository: ExpertsRepository, audit_service: AuditService | None = None):
        self._repository = repository
        self._audit_service = audit_service
        self._users_repository = UsersRepository()

    def _require_audit(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    def _normalize_status(self, value: str | None) -> str:
        return (value or "").strip().lower()

    def _validate_modes(self, modes: list[str] | None) -> None:
        if modes is None:
            return
        for m in modes:
            if m not in _ALLOWED_MODES:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    async def _ensure_user_exists(self, db, user_id: int) -> None:
        user = await self._users_repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    def _expert_visible_to_public(self, expert: Expert, employee: EmployeeContext | None) -> bool:
        if employee is not None:
            return True
        return (expert.status or "").lower() == "active"

    async def list_experts(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        page: int,
        limit: int,
        expert_type: str | None,
        status_query: str | None,
    ) -> tuple[list[Expert], int]:
        expert_type_filter = None
        if expert_type is not None:
            normalized = (expert_type or "").strip().lower()
            if normalized not in {"doctor", "nutritionist"}:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            expert_type_filter = normalized

        status_filter: str | None = "active"
        if employee is not None and status_query is not None:
            normalized = self._normalize_status(status_query)
            if normalized not in _ALLOWED_STATUS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            status_filter = normalized
        elif employee is not None and status_query is None:
            status_filter = None

        experts = await self._repository.list_experts(
            db,
            page=page,
            limit=limit,
            expert_type=expert_type_filter,
            status=status_filter,
        )
        total = await self._repository.count_experts(
            db,
            expert_type=expert_type_filter,
            status=status_filter,
        )
        return experts, total

    async def get_expert_detail(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        expert_id: int,
    ) -> tuple[Expert, list[ExpertExpertiseTag]]:
        expert = await self._repository.get_by_id(db, expert_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")
        if not self._expert_visible_to_public(expert, employee):
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")
        tags = await self._repository.list_tags(db, expert_id)
        return expert, tags

    async def create_expert_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        payload: ExpertCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Expert:
        await self._ensure_user_exists(db, payload.user_id)
        self._validate_modes(list(payload.consultation_modes) if payload.consultation_modes else None)

        expert = Expert(
            user_id=payload.user_id,
            expert_type=payload.expert_type,
            display_name=payload.display_name.strip(),
            profile_photo=payload.profile_photo,
            experience_years=payload.experience_years,
            qualifications=payload.qualifications,
            about_text=payload.about_text,
            consultation_modes=payload.consultation_modes,
            languages=payload.languages,
            session_duration_mins=payload.session_duration_mins,
            appointment_fee_paise=payload.appointment_fee_paise,
            original_fee_paise=payload.original_fee_paise,
            patient_count=payload.patient_count or 0,
            status="active",
        )
        expert = await self._repository.create(db, expert)
        audit = self._require_audit()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_EXPERT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
        )
        return expert

    async def update_expert_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        expert_id: int,
        payload: ExpertUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Expert:
        expert = await self._repository.get_by_id(db, expert_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")

        await self._ensure_user_exists(db, payload.user_id)
        self._validate_modes(list(payload.consultation_modes) if payload.consultation_modes else None)

        expert.user_id = payload.user_id
        expert.expert_type = payload.expert_type
        expert.display_name = payload.display_name.strip()
        expert.profile_photo = payload.profile_photo
        expert.experience_years = payload.experience_years
        expert.qualifications = payload.qualifications
        expert.about_text = payload.about_text
        expert.consultation_modes = payload.consultation_modes
        expert.languages = payload.languages
        expert.session_duration_mins = payload.session_duration_mins
        expert.appointment_fee_paise = payload.appointment_fee_paise
        expert.original_fee_paise = payload.original_fee_paise
        if payload.patient_count is not None:
            expert.patient_count = payload.patient_count

        expert = await self._repository.update(db, expert)
        audit = self._require_audit()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_EXPERT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
        )
        return expert

    async def patch_expert_status_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        expert_id: int,
        status: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Expert:
        normalized = self._normalize_status(status)
        if normalized not in _ALLOWED_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        expert = await self._repository.get_by_id(db, expert_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")

        expert.status = normalized
        expert = await self._repository.update(db, expert)
        audit = self._require_audit()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_EXPERT_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
        )
        return expert

    async def add_tag_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        expert_id: int,
        payload: ExpertTagCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> ExpertExpertiseTag:
        expert = await self._repository.get_by_id(db, expert_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")

        tag = ExpertExpertiseTag(
            expert_id=expert_id,
            tag_name=payload.tag_name.strip(),
            display_order=payload.display_order,
        )
        tag = await self._repository.add_tag(db, tag)
        audit = self._require_audit()
        await audit.log_event(
            db,
            action="EMPLOYEE_ADD_EXPERT_TAG",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
        )
        return tag

    async def delete_tag_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        expert_id: int,
        tag_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        tag = await self._repository.get_tag(db, tag_id, expert_id)
        if tag is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Tag does not exist")

        await self._repository.delete_tag(db, tag)
        audit = self._require_audit()
        await audit.log_event(
            db,
            action="EMPLOYEE_DELETE_EXPERT_TAG",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
        )

    async def list_reviews(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        expert_id: int,
        page: int,
        limit: int,
    ) -> tuple[list[ExpertReview], int]:
        expert = await self._repository.get_by_id(db, expert_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")
        if not self._expert_visible_to_public(expert, employee):
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")

        reviews = await self._repository.list_reviews(db, expert_id=expert_id, page=page, limit=limit)
        total = await self._repository.count_reviews(db, expert_id)
        return reviews, total

    async def create_review_for_user(
        self,
        db,
        *,
        user_id: int,
        expert_id: int,
        payload: ExpertReviewCreateRequest,
    ) -> ExpertReview:
        expert = await self._repository.get_by_id(db, expert_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")
        if (expert.status or "").lower() != "active":
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        existing = await self._repository.get_review_by_expert_and_user(db, expert_id=expert_id, user_id=user_id)
        if existing is not None:
            raise AppError(status_code=409, error_code="CONFLICT", message="You have already reviewed this expert")

        review = ExpertReview(
            expert_id=expert_id,
            user_id=user_id,
            rating=payload.rating,
            review_text=payload.review_text,
        )
        review = await self._repository.create_review(db, review)
        await self._repository.refresh_expert_rating_from_reviews(db, expert_id)
        return review
