"""Business logic for camp report sections CRUD."""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import ensure_internal_employee
from modules.employee.service import EmployeeContext
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.models import CampReportSection
from modules.reports.schemas import CampReportSectionCreateRequest, CampReportSectionUpdateRequest


class CampReportSectionsService:
    """Employee-facing CRUD for camp report section definitions."""

    def __init__(
        self,
        *,
        repository: CampReportSectionsRepository,
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

    def _row_to_dict(self, row: CampReportSection) -> dict:
        return {
            "report_sections": row.report_sections,
            "section": row.section,
            "section_key": row.section_key,
            "description": row.description,
        }

    async def list_sections(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
    ) -> tuple[list[CampReportSection], int]:
        self._ensure_employee_access(employee)
        rows = await self._repository.list_sections(db, page=page, limit=limit)
        total = await self._repository.count_sections(db)
        return rows, total

    async def create_section(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: CampReportSectionCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> CampReportSection:
        self._ensure_employee_access(employee)

        section = self._normalize_text(payload.section)
        section_key = self._normalize_text(payload.section_key)
        if not section or not section_key:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        existing = await self._repository.get_by_section_key(db, section_key=section_key)
        if existing is not None:
            raise AppError(status_code=409, error_code="DUPLICATE_SECTION_KEY", message="Section key already exists")

        description = self._normalize_text(payload.description)
        row = CampReportSection(section=section, section_key=section_key, description=description)
        try:
            created = await self._repository.create(db, row)
        except IntegrityError:
            raise AppError(status_code=409, error_code="DUPLICATE_SECTION_KEY", message="Section key already exists") from None

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_CREATE_CAMP_REPORT_SECTION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return created

    async def update_section(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        report_sections: int,
        payload: CampReportSectionUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> CampReportSection:
        self._ensure_employee_access(employee)

        row = await self._repository.get_by_id(db, report_sections=report_sections)
        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_SECTION_NOT_FOUND",
                message="Camp report section does not exist",
            )

        if payload.section is not None:
            section = self._normalize_text(payload.section)
            if not section:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            row.section = section

        if payload.section_key is not None:
            section_key = self._normalize_text(payload.section_key)
            if not section_key:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            dup = await self._repository.get_by_section_key(db, section_key=section_key)
            if dup is not None and dup.report_sections != report_sections:
                raise AppError(status_code=409, error_code="DUPLICATE_SECTION_KEY", message="Section key already exists")
            row.section_key = section_key

        if payload.description is not None:
            row.description = self._normalize_text(payload.description)

        try:
            await db.flush()
        except IntegrityError:
            raise AppError(status_code=409, error_code="DUPLICATE_SECTION_KEY", message="Section key already exists") from None

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_UPDATE_CAMP_REPORT_SECTION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return row

    async def delete_section(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        report_sections: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)

        existing = await self._repository.get_by_id(db, report_sections=report_sections)
        if existing is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_SECTION_NOT_FOUND",
                message="Camp report section does not exist",
            )

        await self._repository.delete_by_id(db, report_sections=report_sections)
        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_DELETE_CAMP_REPORT_SECTION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
