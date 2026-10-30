"""Business logic for user onboarding payload catalog and camp configuration."""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import ensure_internal_employee
from modules.employee.service import EmployeeContext
from modules.engagements.enums import UserOnboardingPayloadInputType
from modules.engagements.models import CampUserOnboardingPayloadField, UserOnboardingPayloadField
from modules.engagements.onboarding_payload_repository import (
    CampOnboardingPayloadRepository,
    UserOnboardingPayloadFieldsRepository,
)
from modules.engagements.onboarding_payload_schemas import (
    UserOnboardingPayloadFieldCreateRequest,
    UserOnboardingPayloadFieldUpdateRequest,
)
from modules.experts.repository import ExpertTypesRepository


def _field_to_dict(row: UserOnboardingPayloadField) -> dict[str, Any]:
    section = row.section.value if hasattr(row.section, "value") else str(row.section)
    input_type = row.input_type.value if hasattr(row.input_type, "value") else str(row.input_type)
    return {
        "id": row.id,
        "payload_key": row.payload_key,
        "display_name": row.display_name,
        "section": section,
        "input_type": input_type,
    }


class UserOnboardingPayloadFieldsService:
    """Employee CRUD for onboarding payload field definitions."""

    def __init__(
        self,
        *,
        repository: UserOnboardingPayloadFieldsRepository,
        audit_service: AuditService,
    ) -> None:
        self._repository = repository
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        ensure_internal_employee(employee)

    @staticmethod
    def _normalize_text(value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped if stripped else None

    async def list_fields(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
    ) -> tuple[list[UserOnboardingPayloadField], int]:
        self._ensure_employee_access(employee)
        rows = await self._repository.list_fields(db, page=page, limit=limit)
        total = await self._repository.count_fields(db)
        return rows, total

    async def create_field(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: UserOnboardingPayloadFieldCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserOnboardingPayloadField:
        self._ensure_employee_access(employee)

        payload_key = self._normalize_text(payload.payload_key)
        if not payload_key:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        existing = await self._repository.get_by_payload_key(db, payload_key=payload_key)
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="DUPLICATE_PAYLOAD_KEY",
                message="Payload key already exists",
            )

        row = UserOnboardingPayloadField(
            payload_key=payload_key,
            display_name=self._normalize_text(payload.display_name),
            section=payload.section,
            input_type=payload.input_type,
        )
        try:
            created = await self._repository.create(db, row)
        except IntegrityError:
            raise AppError(
                status_code=409,
                error_code="DUPLICATE_PAYLOAD_KEY",
                message="Payload key already exists",
            ) from None

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_CREATE_USER_ONBOARDING_PAYLOAD_FIELD",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return created

    async def update_field(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        field_id: int,
        payload: UserOnboardingPayloadFieldUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserOnboardingPayloadField:
        self._ensure_employee_access(employee)

        row = await self._repository.get_by_id(db, field_id=field_id)
        if row is None:
            raise AppError(
                status_code=404,
                error_code="USER_ONBOARDING_PAYLOAD_FIELD_NOT_FOUND",
                message="Onboarding payload field does not exist",
            )

        if payload.payload_key is not None:
            payload_key = self._normalize_text(payload.payload_key)
            if not payload_key:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            dup = await self._repository.get_by_payload_key(db, payload_key=payload_key)
            if dup is not None and dup.id != field_id:
                raise AppError(
                    status_code=409,
                    error_code="DUPLICATE_PAYLOAD_KEY",
                    message="Payload key already exists",
                )
            row.payload_key = payload_key

        if payload.display_name is not None:
            row.display_name = self._normalize_text(payload.display_name)
        if payload.section is not None:
            row.section = payload.section
        if payload.input_type is not None:
            row.input_type = payload.input_type

        try:
            await db.flush()
        except IntegrityError:
            raise AppError(
                status_code=409,
                error_code="DUPLICATE_PAYLOAD_KEY",
                message="Payload key already exists",
            ) from None

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_UPDATE_USER_ONBOARDING_PAYLOAD_FIELD",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return row

    async def delete_field(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        field_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)

        existing = await self._repository.get_by_id(db, field_id=field_id)
        if existing is None:
            raise AppError(
                status_code=404,
                error_code="USER_ONBOARDING_PAYLOAD_FIELD_NOT_FOUND",
                message="Onboarding payload field does not exist",
            )

        await self._repository.delete_by_id(db, field_id=field_id)
        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_DELETE_USER_ONBOARDING_PAYLOAD_FIELD",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

    row_to_dict = staticmethod(_field_to_dict)


class CampOnboardingPayloadService:
    """Camp-level onboarding requirement configuration."""

    def __init__(
        self,
        *,
        camp_repository: CampOnboardingPayloadRepository,
        fields_repository: UserOnboardingPayloadFieldsRepository,
        expert_types_repository: ExpertTypesRepository,
        audit_service: AuditService,
    ) -> None:
        self._camp_repository = camp_repository
        self._fields_repository = fields_repository
        self._expert_types_repository = expert_types_repository
        self._audit_service = audit_service

    async def ensure_row_for_camp_no(self, db: AsyncSession, *, camp_no: int) -> None:
        if camp_no is None:
            return
        await self._camp_repository.ensure_row(db, camp_no=int(camp_no))

    async def get_row_by_camp_code(
        self,
        db: AsyncSession,
        *,
        camp_code: str,
    ) -> CampUserOnboardingPayloadField | None:
        return await self._camp_repository.get_by_camp_code(db, camp_code=camp_code)

    async def remap_camp_no(
        self,
        db: AsyncSession,
        *,
        from_camp_no: int,
        to_camp_no: int,
    ) -> None:
        await self._camp_repository.remap_camp_no(
            db,
            from_camp_no=from_camp_no,
            to_camp_no=to_camp_no,
        )

    async def get_payload_state(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
    ) -> dict[str, Any]:
        row = await self._camp_repository.get_by_camp_no(db, camp_no=camp_no)
        if row is None:
            return {
                "camp_no": camp_no,
                "camp_code": None,
                "initialized": False,
                "onboarding_payload": None,
            }
        return {
            "camp_no": camp_no,
            "camp_code": row.camp_code,
            "initialized": True,
            "onboarding_payload": row.onboarding_payload or {},
        }

    async def initialize_payload(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        camp_exists: bool,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        ensure_internal_employee(employee)
        if not camp_exists:
            raise AppError(
                status_code=404,
                error_code="CAMP_NOT_FOUND",
                message="Camp does not exist",
            )

        existing = await self._camp_repository.get_by_camp_no(db, camp_no=camp_no)
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="CAMP_ONBOARDING_ALREADY_INITIALIZED",
                message="Camp onboarding requirements are already initialized",
            )

        row = await self._camp_repository.create_initial_row(db, camp_no=camp_no)
        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_INIT_CAMP_ONBOARDING_PAYLOAD",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return {
            "camp_no": camp_no,
            "camp_code": row.camp_code,
            "initialized": True,
            "onboarding_payload": {},
        }

    async def _validate_onboarding_payload(
        self,
        db: AsyncSession,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        catalog = await self._fields_repository.list_all(db)
        if not catalog:
            if payload:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            return {}

        catalog_by_key = {row.payload_key: row for row in catalog}
        expert_types = await self._expert_types_repository.list_all(db)
        expert_keys = {row.type_key for row in expert_types}

        if set(payload.keys()) != set(catalog_by_key.keys()):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        normalized: dict[str, Any] = {}
        for key, row in catalog_by_key.items():
            entry = payload.get(key)
            if not isinstance(entry, dict):
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

            input_type = entry.get("input_type")
            expected = row.input_type.value if hasattr(row.input_type, "value") else str(row.input_type)
            if input_type != expected:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

            required = entry.get("required")
            if not isinstance(required, bool):
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

            sub_payload = entry.get("sub_payload")
            if not isinstance(sub_payload, dict):
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

            if expected == UserOnboardingPayloadInputType.json.value:
                if set(sub_payload.keys()) != expert_keys:
                    raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
                for expert_key, flag in sub_payload.items():
                    if expert_key not in expert_keys or not isinstance(flag, bool):
                        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
                normalized[key] = {
                    "input_type": expected,
                    "required": required,
                    "sub_payload": sub_payload,
                }
            else:
                if sub_payload:
                    raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
                normalized[key] = {
                    "input_type": expected,
                    "required": required,
                    "sub_payload": {},
                }

        return normalized

    async def update_payload(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        payload: dict[str, Any],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        ensure_internal_employee(employee)

        row = await self._camp_repository.get_by_camp_no(db, camp_no=camp_no)
        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_ONBOARDING_NOT_INITIALIZED",
                message="Camp onboarding requirements are not initialized",
            )

        normalized = await self._validate_onboarding_payload(db, payload)
        await self._camp_repository.update_payload(db, row=row, onboarding_payload=normalized)
        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_UPDATE_CAMP_ONBOARDING_PAYLOAD",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return {
            "camp_no": camp_no,
            "camp_code": row.camp_code,
            "initialized": True,
            "onboarding_payload": normalized,
        }

    async def public_onboarding_for_camp(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        engagements: list[tuple[str, str | None]],
        organization_name: str = "",
        year: int = 0,
    ) -> dict[str, Any]:
        row = await self._camp_repository.get_by_camp_no(db, camp_no=camp_no)
        onboarding_payload = row.onboarding_payload if row is not None else {}
        if onboarding_payload is None:
            onboarding_payload = {}
        return {
            "organization_name": organization_name or "",
            "year": year,
            "engagements": [
                {"code": code, "city": city}
                for code, city in engagements
            ],
            "onboarding_payload": onboarding_payload,
        }
