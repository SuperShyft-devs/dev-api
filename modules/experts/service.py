"""Experts business logic."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import ensure_expert_portal_access, ensure_not_expert_employee
from modules.employee.models import Employee, EmployeeRole
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext
from modules.engagements.models import Engagement, EngagementParticipant
from modules.experts.consultation_bookings_repository import ConsultationBookingsRepository
from modules.experts.consultations import (
    booking_to_api_preference,
    is_upcoming_slot,
    normalize_hhmm,
    normalize_preference,
)
from modules.experts.models import (
    ConsultationBooking,
    Expert,
    ExpertAvailabilityModel,
    ExpertAvailabilityOverrideModel,
    ExpertExpertiseTag,
    ExpertReview,
    ExpertTypeModel,
)
from modules.experts.repository import (
    ExpertAvailabilityRepository,
    ExpertAvailabilityOverrideRepository,
    ExpertsRepository,
    ExpertTypesRepository,
)
from modules.experts.schemas import (
    AvailabilityBlockCreate,
    AvailabilityBulkSave,
    ConsultationBookRequest,
    ConsultationConfirmRequest,
    ConsultationDoneRequest,
    ExpertCreateRequest,
    ExpertReviewCreateRequest,
    ExpertTagCreateRequest,
    ExpertTypeCreateRequest,
    ExpertTypeUpdateRequest,
    ExpertUpdateRequest,
    OverrideCreate,
)
from modules.experts.slot_engine import (
    aggregate_slots,
    compute_expert_day_slots,
    expert_effective_on,
    is_slot_available_for_expert,
    next_n_days,
    parse_slot_time,
)
from modules.users.models import User
from modules.users.repository import UsersRepository
from sqlalchemy import select


_ALLOWED_STATUS = {"active", "inactive"}
_ALLOWED_MODES = {"video", "voice", "chat"}


class ExpertTypesService:
    def __init__(self, repository: ExpertTypesRepository):
        self._repository = repository

    async def list_expert_types(self, db) -> list[ExpertTypeModel]:
        return await self._repository.list_all(db)

    async def create_expert_type(self, db, *, payload: ExpertTypeCreateRequest) -> ExpertTypeModel:
        existing = await self._repository.get_by_key(db, payload.type_key)
        if existing is not None:
            raise AppError(status_code=409, error_code="CONFLICT", message="Expert type with this key already exists")
        expert_type = ExpertTypeModel(type_key=payload.type_key, type=payload.type.strip())
        return await self._repository.create(db, expert_type)

    async def update_expert_type(self, db, *, expert_type_id: int, payload: ExpertTypeUpdateRequest) -> ExpertTypeModel:
        expert_type = await self._repository.get_by_id(db, expert_type_id)
        if expert_type is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert type not found")
        if payload.type_key is not None and payload.type_key != expert_type.type_key:
            existing = await self._repository.get_by_key(db, payload.type_key)
            if existing is not None:
                raise AppError(status_code=409, error_code="CONFLICT", message="Expert type with this key already exists")
            expert_type.type_key = payload.type_key
        if payload.type is not None:
            expert_type.type = payload.type.strip()
        return await self._repository.update(db, expert_type)

    async def delete_expert_type(self, db, *, expert_type_id: int) -> None:
        expert_type = await self._repository.get_by_id(db, expert_type_id)
        if expert_type is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert type not found")
        await self._repository.delete(db, expert_type)

    async def validate_type_key(self, db, type_key: str) -> None:
        existing = await self._repository.get_by_key(db, type_key)
        if existing is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message=f"Unknown expert type: {type_key}")


class ExpertsService:
    def __init__(
        self,
        repository: ExpertsRepository,
        audit_service: AuditService | None = None,
        expert_types_service: ExpertTypesService | None = None,
        employee_repository: EmployeeRepository | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._users_repository = UsersRepository()
        self._expert_types_service = expert_types_service
        self._employee_repository = employee_repository or EmployeeRepository()

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

    async def _ensure_expert_employee(self, db, user_id: int) -> None:
        existing = await self._employee_repository.get_by_user_id(db, user_id)
        if existing is not None:
            return
        row = Employee(
            user_id=user_id,
            role=EmployeeRole.expert,
            status="active",
        )
        await self._employee_repository.create(db, row)

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
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[Expert], int]:
        expert_type_filter = None
        if expert_type is not None:
            expert_type_filter = (expert_type or "").strip().lower()

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
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        total = await self._repository.count_experts(
            db,
            expert_type=expert_type_filter,
            status=status_filter,
            search=search,
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
        ensure_not_expert_employee(employee)
        await self._ensure_user_exists(db, payload.user_id)
        self._validate_modes(list(payload.consultation_modes) if payload.consultation_modes else None)
        if self._expert_types_service:
            await self._expert_types_service.validate_type_key(db, payload.expert_type)

        expert = Expert(
            user_id=payload.user_id,
            expert_type=payload.expert_type,
            specialization=payload.specialization.strip(),
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
            effective_from=payload.effective_from,
            effective_until=payload.effective_until,
            status="active",
        )
        expert = await self._repository.create(db, expert)
        await self._ensure_expert_employee(db, payload.user_id)
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
        ensure_not_expert_employee(employee)
        expert = await self._repository.get_by_id(db, expert_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")

        await self._ensure_user_exists(db, payload.user_id)
        self._validate_modes(list(payload.consultation_modes) if payload.consultation_modes else None)
        if self._expert_types_service:
            await self._expert_types_service.validate_type_key(db, payload.expert_type)

        expert.user_id = payload.user_id
        expert.expert_type = payload.expert_type
        expert.specialization = payload.specialization.strip()
        expert.profile_photo = payload.profile_photo
        expert.experience_years = payload.experience_years
        expert.qualifications = payload.qualifications
        expert.about_text = payload.about_text
        expert.consultation_modes = payload.consultation_modes
        expert.languages = payload.languages
        expert.session_duration_mins = payload.session_duration_mins
        expert.appointment_fee_paise = payload.appointment_fee_paise
        expert.original_fee_paise = payload.original_fee_paise
        expert.effective_from = payload.effective_from
        expert.effective_until = payload.effective_until
        if payload.patient_count is not None:
            expert.patient_count = payload.patient_count

        expert = await self._repository.update(db, expert)
        await self._ensure_expert_employee(db, payload.user_id)
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
        ensure_not_expert_employee(employee)
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
        ensure_not_expert_employee(employee)
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
        ensure_not_expert_employee(employee)
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

    async def get_portal_me(
        self,
        db,
        *,
        employee: EmployeeContext,
    ) -> tuple[Expert, list[ExpertExpertiseTag]]:
        ensure_expert_portal_access(employee)
        expert = await self._repository.get_by_user_id(db, employee.user_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")
        tags = await self._repository.list_tags(db, expert.expert_id)
        return expert, tags

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


class ExpertAvailabilityService:
    def __init__(
        self,
        experts_repository: ExpertsRepository,
        availability_repository: ExpertAvailabilityRepository,
        override_repository: ExpertAvailabilityOverrideRepository,
        consultation_bookings_repository: ConsultationBookingsRepository | None = None,
    ):
        self._experts = experts_repository
        self._availability = availability_repository
        self._overrides = override_repository
        self._consultation_bookings = consultation_bookings_repository or ConsultationBookingsRepository()

    async def _get_expert_or_404(self, db, expert_id: int) -> Expert:
        expert = await self._experts.get_by_id(db, expert_id)
        if expert is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Expert does not exist")
        return expert

    # ─── Weekly blocks ─────────────────────────────────────────────────────

    async def list_blocks(self, db, *, expert_id: int) -> list[ExpertAvailabilityModel]:
        await self._get_expert_or_404(db, expert_id)
        return await self._availability.list_by_expert(db, expert_id)

    async def create_block(
        self, db, *, expert_id: int, payload: AvailabilityBlockCreate
    ) -> ExpertAvailabilityModel:
        await self._get_expert_or_404(db, expert_id)
        block = ExpertAvailabilityModel(
            expert_id=expert_id,
            day_of_week=payload.day_of_week,
            start_time=payload.start_time,
            end_time=payload.end_time,
            slot_duration=payload.slot_duration,
            buffer_time=payload.buffer_time,
        )
        return await self._availability.create(db, block)

    async def update_block(
        self, db, *, expert_id: int, block_id: int, payload: AvailabilityBlockCreate
    ) -> ExpertAvailabilityModel:
        block = await self._availability.get_by_id(db, block_id)
        if block is None or block.expert_id != expert_id:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Availability block not found")
        block.day_of_week = payload.day_of_week
        block.start_time = payload.start_time
        block.end_time = payload.end_time
        block.slot_duration = payload.slot_duration
        block.buffer_time = payload.buffer_time
        return await self._availability.update(db, block)

    async def delete_block(self, db, *, expert_id: int, block_id: int) -> None:
        block = await self._availability.get_by_id(db, block_id)
        if block is None or block.expert_id != expert_id:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Availability block not found")
        await self._availability.delete(db, block)

    async def bulk_save_blocks(
        self, db, *, expert_id: int, payload: AvailabilityBulkSave
    ) -> list[ExpertAvailabilityModel]:
        await self._get_expert_or_404(db, expert_id)
        models = [
            ExpertAvailabilityModel(
                expert_id=expert_id,
                day_of_week=b.day_of_week,
                start_time=b.start_time,
                end_time=b.end_time,
                slot_duration=b.slot_duration,
                buffer_time=b.buffer_time,
            )
            for b in payload.blocks
        ]
        return await self._availability.bulk_replace(db, expert_id, models)

    # ─── Overrides ─────────────────────────────────────────────────────────

    async def list_overrides(self, db, *, expert_id: int) -> list[ExpertAvailabilityOverrideModel]:
        await self._get_expert_or_404(db, expert_id)
        return await self._overrides.list_by_expert(db, expert_id)

    async def create_override(
        self, db, *, expert_id: int, payload: OverrideCreate
    ) -> ExpertAvailabilityOverrideModel:
        await self._get_expert_or_404(db, expert_id)
        override = ExpertAvailabilityOverrideModel(
            expert_id=expert_id,
            override_date=payload.override_date,
            status=payload.status,
            start_time=payload.start_time,
            end_time=payload.end_time,
            buffer_time=payload.buffer_time,
        )
        return await self._overrides.create(db, override)

    async def delete_override(self, db, *, expert_id: int, override_id: int) -> None:
        override = await self._overrides.get_by_id(db, override_id)
        if override is None or override.expert_id != expert_id:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Override not found")
        await self._overrides.delete(db, override)

    # ─── Consultation slots / booking ──────────────────────────────────────

    async def get_consultation_slots(
        self,
        db,
        *,
        expert_type: str | None = None,
        expert_id: int | None = None,
        days: int = 7,
    ) -> dict[str, dict[str, list[dict[str, Any]]]]:
        if expert_id is not None:
            expert = await self._get_expert_or_404(db, expert_id)
            if expert_type and expert.expert_type != expert_type:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="expert_id does not match expert_type",
                )
            experts = [expert] if (expert.status or "").lower() == "active" else []
        else:
            experts = await self._experts.list_for_slots(db, expert_type=expert_type)

        dates = next_n_days(days)
        if not experts:
            if expert_type:
                return {expert_type: {d.isoformat(): [] for d in dates}}
            types = await ExpertTypesRepository().list_all(db)
            return {t.type_key: {d.isoformat(): [] for d in dates} for t in types}

        expert_ids = [e.expert_id for e in experts]
        blocks = await self._availability.list_by_expert_ids(db, expert_ids)
        overrides = await self._overrides.list_by_expert_ids_and_dates(
            db, expert_ids, start_date=dates[0], end_date=dates[-1]
        )

        blocks_by_expert: dict[int, list[ExpertAvailabilityModel]] = defaultdict(list)
        for b in blocks:
            blocks_by_expert[b.expert_id].append(b)
        overrides_by_expert: dict[int, list[ExpertAvailabilityOverrideModel]] = defaultdict(list)
        for o in overrides:
            overrides_by_expert[o.expert_id].append(o)

        by_type_raw: dict[str, list[tuple[str, str, int]]] = defaultdict(list)
        for expert in experts:
            default_duration = expert.session_duration_mins or 30
            e_blocks = blocks_by_expert.get(expert.expert_id, [])
            e_overrides = overrides_by_expert.get(expert.expert_id, [])
            for day in dates:
                if not expert_effective_on(expert, day):
                    continue
                day_slots = compute_expert_day_slots(
                    day=day,
                    blocks=e_blocks,
                    overrides=e_overrides,
                    default_duration=default_duration,
                )
                date_iso = day.isoformat()
                for start, duration in day_slots:
                    by_type_raw[expert.expert_type].append((date_iso, start, duration))

        type_keys: list[str]
        if expert_type:
            type_keys = [expert_type]
        elif expert_id is not None and experts:
            type_keys = [experts[0].expert_type]
        else:
            types = await ExpertTypesRepository().list_all(db)
            type_keys = [t.type_key for t in types]
            for key in by_type_raw:
                if key not in type_keys:
                    type_keys.append(key)

        result: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for key in type_keys:
            aggregated = aggregate_slots(by_type_raw.get(key, []))
            # Ensure all dates present
            day_map: dict[str, list[dict[str, Any]]] = {
                d.isoformat(): aggregated.get(d.isoformat(), []) for d in dates
            }
            for d_iso, slots in aggregated.items():
                day_map[d_iso] = slots
            result[key] = day_map
        return result

    async def _slot_is_available(
        self,
        db,
        *,
        expert_type: str,
        day: date,
        slot_hhmm: str,
        expert_id: int | None,
    ) -> bool:
        if expert_id is not None:
            experts = await self._experts.list_for_slots(db, expert_id=expert_id)
        else:
            experts = await self._experts.list_for_slots(db, expert_type=expert_type)

        if not experts:
            return False

        expert_ids = [e.expert_id for e in experts]
        blocks = await self._availability.list_by_expert_ids(db, expert_ids)
        overrides = await self._overrides.list_by_expert_ids_and_dates(
            db, expert_ids, start_date=day, end_date=day
        )
        blocks_by_expert: dict[int, list[ExpertAvailabilityModel]] = defaultdict(list)
        for b in blocks:
            blocks_by_expert[b.expert_id].append(b)
        overrides_by_expert: dict[int, list[ExpertAvailabilityOverrideModel]] = defaultdict(list)
        for o in overrides:
            overrides_by_expert[o.expert_id].append(o)

        for expert in experts:
            if expert.expert_type != expert_type:
                continue
            if not expert_effective_on(expert, day):
                continue
            if is_slot_available_for_expert(
                day=day,
                slot_hhmm=slot_hhmm,
                blocks=blocks_by_expert.get(expert.expert_id, []),
                overrides=overrides_by_expert.get(expert.expert_id, []),
                default_duration=expert.session_duration_mins or 30,
            ):
                return True
        return False

    async def book_consultation_slot(
        self,
        db,
        *,
        user_id: int,
        payload: ConsultationBookRequest,
    ) -> dict[str, Any]:
        slot_hhmm = normalize_hhmm(payload.slot)
        engagement = await db.get(Engagement, payload.engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Engagement not found")

        allowed = engagement.consultations if isinstance(engagement.consultations, dict) else {}
        if allowed.get(payload.expert_type) is not True:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Consultation not available for this engagement: {payload.expert_type}",
            )

        result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.user_id == user_id)
            .where(EngagementParticipant.engagement_id == payload.engagement_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = result.scalar_one_or_none()
        if participant is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Participant not found")

        booking = await self._consultation_bookings.get_by_participant_and_type(
            db,
            participant.engagement_participant_id,
            payload.expert_type,
        )
        if booking is not None and not booking.want:
            booking = None
        if booking is None or not booking.want:
            booking = await self._consultation_bookings.create_or_update_for_type(
                db,
                participant,
                payload.expert_type,
                want=True,
            )
        if booking.expert_id is not None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Consultation already confirmed by an expert",
            )

        available = await self._slot_is_available(
            db,
            expert_type=payload.expert_type,
            day=payload.date,
            slot_hhmm=slot_hhmm,
            expert_id=payload.expert_id,
        )
        if not available:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Selected slot is not available",
            )

        booking.consultation_date = payload.date
        booking.consultation_slot = slot_hhmm
        db.add(booking)
        db.add(participant)
        await db.flush()

        return {
            "message": "We have received your slot. We will let you know when expert confirms",
            "engagement_id": payload.engagement_id,
            "expert_type": payload.expert_type,
            "date": payload.date.isoformat(),
            "slot": slot_hhmm,
        }

    async def list_consultation_requests(self, db, *, employee: EmployeeContext) -> list[dict[str, Any]]:
        ensure_expert_portal_access(employee)
        result = await db.execute(
            select(ConsultationBooking, EngagementParticipant, Engagement, User)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id == ConsultationBooking.engagement_participant_id,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(Engagement.status.in_(("running", "scheduled")))
            .where(Engagement.organization_id.is_(None))
            .where(ConsultationBooking.want.is_(True))
            .where(ConsultationBooking.expert_id.is_(None))
            .order_by(ConsultationBooking.consultation_id.desc())
        )
        rows = result.all()
        items: list[dict[str, Any]] = []
        for booking, participant, engagement, user in rows:
            pref = booking_to_api_preference(booking)
            items.append(
                {
                    "consultation_id": booking.consultation_id,
                    "user_id": user.user_id,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "email": user.email,
                    "phone": user.phone,
                    "engagement_id": engagement.engagement_id,
                    "engagement_code": engagement.engagement_code,
                    "expert_type": booking.expert_type,
                    "date": pref.get("date"),
                    "slot": pref.get("slot"),
                    "engagement_participant_id": participant.engagement_participant_id,
                }
            )
        return items

    async def confirm_consultation_request(
        self,
        db,
        *,
        employee: EmployeeContext,
        payload: ConsultationConfirmRequest,
    ) -> dict[str, Any]:
        ensure_expert_portal_access(employee)
        slot_hhmm = normalize_hhmm(payload.slot)

        confirming_expert_id = payload.expert_id
        if confirming_expert_id is None:
            expert = await self._experts.get_by_user_id(db, employee.user_id)
            if expert is None:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="expert_id is required when the current user is not linked to an expert",
                )
            confirming_expert_id = expert.expert_id

        expert = await self._get_expert_or_404(db, confirming_expert_id)
        if expert.expert_type != payload.expert_type:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Expert type does not match the request",
            )

        # Experts may only confirm as themselves
        if employee.role == EmployeeRole.expert:
            own = await self._experts.get_by_user_id(db, employee.user_id)
            if own is None or own.expert_id != confirming_expert_id:
                raise AppError(status_code=403, error_code="FORBIDDEN", message="Cannot confirm for another expert")

        engagement = await db.get(Engagement, payload.engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Engagement not found")

        result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.user_id == payload.user_id)
            .where(EngagementParticipant.engagement_id == payload.engagement_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = result.scalar_one_or_none()
        if participant is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Participant not found")

        booking = await self._consultation_bookings.get_by_participant_and_type(
            db,
            participant.engagement_participant_id,
            payload.expert_type,
        )
        if booking is None or not booking.want:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Participant did not request this consultation",
            )
        if booking.expert_id is not None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Consultation already confirmed",
            )

        pref = booking_to_api_preference(booking)
        if pref.get("date") and pref["date"] != payload.date.isoformat():
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Date does not match requested slot")
        if pref.get("slot") and normalize_hhmm(pref["slot"]) != slot_hhmm:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Slot does not match requested slot")

        available = await self._slot_is_available(
            db,
            expert_type=payload.expert_type,
            day=payload.date,
            slot_hhmm=slot_hhmm,
            expert_id=confirming_expert_id,
        )
        if not available:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Selected slot is not available for this expert",
            )

        booking.consultation_date = payload.date
        booking.consultation_slot = slot_hhmm
        booking.expert_id = confirming_expert_id
        booking.done = False
        db.add(booking)
        db.add(participant)

        override = ExpertAvailabilityOverrideModel(
            expert_id=confirming_expert_id,
            override_date=payload.date,
            status="booked",
            start_time=parse_slot_time(slot_hhmm),
            end_time=None,
            buffer_time=None,
        )
        await self._overrides.create(db, override)
        await db.flush()

        return {
            "message": "Consultation confirmed",
            "user_id": payload.user_id,
            "engagement_id": payload.engagement_id,
            "expert_type": payload.expert_type,
            "expert_id": confirming_expert_id,
            "date": payload.date.isoformat(),
            "slot": slot_hhmm,
        }

    async def list_upcoming_consultations(self, db, *, employee: EmployeeContext) -> list[dict[str, Any]]:
        ensure_expert_portal_access(employee)
        expert = await self._experts.get_by_user_id(db, employee.user_id)
        if expert is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Current user is not linked to an expert profile",
            )

        result = await db.execute(
            select(ConsultationBooking, EngagementParticipant, Engagement, User)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id == ConsultationBooking.engagement_participant_id,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(ConsultationBooking.want.is_(True))
            .where(ConsultationBooking.done.is_(False))
            .where(ConsultationBooking.expert_id == expert.expert_id)
            .order_by(ConsultationBooking.consultation_date.asc(), ConsultationBooking.consultation_slot.asc())
        )
        rows = result.all()
        items: list[dict[str, Any]] = []
        for booking, participant, engagement, user in rows:
            pref = booking_to_api_preference(booking)
            if not is_upcoming_slot(pref.get("date"), pref.get("slot")):
                continue
            items.append(
                {
                    "consultation_id": booking.consultation_id,
                    "user_id": user.user_id,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "email": user.email,
                    "phone": user.phone,
                    "engagement_id": engagement.engagement_id,
                    "engagement_code": engagement.engagement_code,
                    "expert_type": booking.expert_type,
                    "date": pref.get("date"),
                    "slot": pref.get("slot"),
                    "expert_id": expert.expert_id,
                    "meet_link": booking.meet_link,
                    "engagement_participant_id": participant.engagement_participant_id,
                }
            )

        items.sort(key=lambda x: (x.get("date") or "", x.get("slot") or ""))
        return items

    async def mark_consultation_done(
        self,
        db,
        *,
        employee: EmployeeContext,
        payload: ConsultationDoneRequest,
    ) -> dict[str, Any]:
        ensure_expert_portal_access(employee)

        actor_expert = await self._experts.get_by_user_id(db, employee.user_id)
        acting_expert_id = payload.expert_id
        if acting_expert_id is None:
            if actor_expert is None:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="expert_id is required when the current user is not linked to an expert",
                )
            acting_expert_id = actor_expert.expert_id

        if employee.role == EmployeeRole.expert:
            if actor_expert is None or actor_expert.expert_id != acting_expert_id:
                raise AppError(
                    status_code=403,
                    error_code="FORBIDDEN",
                    message="Cannot mark done for another expert",
                )

        result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.user_id == payload.user_id)
            .where(EngagementParticipant.engagement_id == payload.engagement_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        participant = result.scalar_one_or_none()
        if participant is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Participant not found")

        meet_link = (payload.meet_link or "").strip()
        if not meet_link:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="meet_link is required",
            )

        booking = await self._consultation_bookings.get_by_participant_and_type(
            db,
            participant.engagement_participant_id,
            payload.expert_type,
        )
        if booking is None or not booking.want:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Participant did not request this consultation",
            )
        if booking.expert_id is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Consultation is not assigned to an expert yet",
            )
        if booking.expert_id != acting_expert_id:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Consultation is assigned to a different expert",
            )
        if booking.done:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Consultation is already marked done",
            )

        booking.meet_link = meet_link
        booking.done = True
        db.add(booking)
        await db.flush()

        return {
            "message": "Consultation marked as done",
            "user_id": payload.user_id,
            "engagement_id": payload.engagement_id,
            "expert_type": payload.expert_type,
            "expert_id": acting_expert_id,
            "done": True,
        }
