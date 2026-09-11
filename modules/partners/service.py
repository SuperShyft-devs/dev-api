"""Partners service — CRUD business rules."""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import ensure_admin
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext
from modules.partners.models import Partner, PartnerRole
from modules.partners.repository import PartnersRepository
from modules.partners.schemas import PartnerCreateRequest, PartnerUpdateRequest


_ALLOWED_STATUS = {"active", "inactive", "archived"}
_ALLOWED_STATUS_UPDATE = {"active", "inactive"}
_ALLOWED_ROLES = frozenset(
    {
        PartnerRole.phlebo.value,
        PartnerRole.expert.value,
        PartnerRole.organization_manager.value,
    }
)


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_email(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized or None


def _normalize_phone(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


class PartnersService:
    def __init__(
        self,
        repository: PartnersRepository,
        audit_service: AuditService | None = None,
        employee_repository: EmployeeRepository | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._employee_repository = employee_repository or EmployeeRepository()

    def _require_audit(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def _reject_if_employee_identifier_exists(
        self,
        db: AsyncSession,
        *,
        phone: str | None,
        email: str | None,
    ) -> None:
        if phone:
            existing = await self._employee_repository.get_by_phone(db, phone)
            if existing is not None:
                raise AppError(
                    status_code=409,
                    error_code="IDENTIFIER_ALREADY_EXISTS",
                    message="Phone or email already exists on an employee",
                )
        if email:
            existing = await self._employee_repository.get_by_email(db, email)
            if existing is not None:
                raise AppError(
                    status_code=409,
                    error_code="IDENTIFIER_ALREADY_EXISTS",
                    message="Phone or email already exists on an employee",
                )

    async def create_partner(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: PartnerCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Partner:
        ensure_admin(employee)

        status_value = _normalize_status(payload.status)
        if status_value not in _ALLOWED_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        phone = _normalize_phone(payload.phone)
        email = _normalize_email(str(payload.email) if payload.email else None)
        role = payload.role.value if isinstance(payload.role, PartnerRole) else str(payload.role)

        await self._reject_if_employee_identifier_exists(db, phone=phone, email=email)

        if phone:
            existing = await self._repository.get_by_phone(db, phone)
            if existing is not None:
                raise AppError(status_code=409, error_code="PARTNER_ALREADY_EXISTS", message="Partner already exists")
        if email:
            existing = await self._repository.get_by_email(db, email)
            if existing is not None:
                raise AppError(status_code=409, error_code="PARTNER_ALREADY_EXISTS", message="Partner already exists")

        row = Partner(
            name=payload.name.strip(),
            phone=phone,
            email=email,
            role=role,
            status=status_value,
        )
        try:
            row = await self._repository.create(db, row)
        except IntegrityError as exc:
            raise AppError(
                status_code=409,
                error_code="PARTNER_ALREADY_EXISTS",
                message="Partner already exists",
            ) from exc

        await self._require_audit().log_event(
            db,
            action="PARTNER_CREATE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return row

    async def list_partners(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        status: str | None,
        role: str | None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[Partner], int]:
        ensure_admin(employee)

        status_value = None
        if status is not None:
            normalized = _normalize_status(status)
            if normalized not in _ALLOWED_STATUS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            status_value = normalized

        role_value = None
        if role is not None:
            normalized_role = role.strip().lower()
            if normalized_role not in _ALLOWED_ROLES:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            role_value = normalized_role

        partners = await self._repository.list_partners(
            db,
            page=page,
            limit=limit,
            status=status_value,
            role=role_value,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        total = await self._repository.count_partners(
            db, status=status_value, role=role_value, search=search
        )
        return partners, total

    async def get_partner_details(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        partner_id: int,
    ) -> Partner:
        ensure_admin(employee)
        row = await self._repository.get_by_id(db, partner_id)
        if row is None:
            raise AppError(status_code=404, error_code="PARTNER_NOT_FOUND", message="Partner does not exist")
        return row

    async def update_partner(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        partner_id: int,
        payload: PartnerUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Partner:
        ensure_admin(employee)
        row = await self._repository.get_by_id(db, partner_id)
        if row is None:
            raise AppError(status_code=404, error_code="PARTNER_NOT_FOUND", message="Partner does not exist")

        phone = _normalize_phone(payload.phone)
        email = _normalize_email(str(payload.email) if payload.email else None)
        role = payload.role.value if isinstance(payload.role, PartnerRole) else str(payload.role)

        await self._reject_if_employee_identifier_exists(db, phone=phone, email=email)

        if phone:
            existing = await self._repository.get_by_phone(db, phone)
            if existing is not None and existing.partner_id != row.partner_id:
                raise AppError(status_code=409, error_code="PARTNER_ALREADY_EXISTS", message="Partner already exists")
        if email:
            existing = await self._repository.get_by_email(db, email)
            if existing is not None and existing.partner_id != row.partner_id:
                raise AppError(status_code=409, error_code="PARTNER_ALREADY_EXISTS", message="Partner already exists")

        row.name = payload.name.strip()
        row.phone = phone
        row.email = email
        row.role = role
        try:
            row = await self._repository.update(db, row)
        except IntegrityError as exc:
            raise AppError(
                status_code=409,
                error_code="PARTNER_ALREADY_EXISTS",
                message="Partner already exists",
            ) from exc

        await self._require_audit().log_event(
            db,
            action="PARTNER_UPDATE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return row

    async def change_partner_status(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        partner_id: int,
        status: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Partner:
        ensure_admin(employee)
        row = await self._repository.get_by_id(db, partner_id)
        if row is None:
            raise AppError(status_code=404, error_code="PARTNER_NOT_FOUND", message="Partner does not exist")

        normalized = _normalize_status(status)
        if normalized not in _ALLOWED_STATUS_UPDATE:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        row.status = normalized
        row = await self._repository.update(db, row)

        await self._require_audit().log_event(
            db,
            action="PARTNER_UPDATE_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return row
