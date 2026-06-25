"""Business logic for camp report init and delete."""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import (
    ensure_camp_access,
    ensure_camp_access_admin_or_org_manager,
    ensure_internal_employee,
)
from modules.employee.service import EmployeeContext
from modules.engagements.camp_no import format_camp_name, format_department_camp_name
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository
from modules.organizations.service import get_department_slugs
from modules.reports.camp_report_section_builders import (
    SECTION_BUILDERS,
    build_kpis,
    build_overall_risk_score,
    build_participation_by_age,
)
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.models import CampReport


class CampReportsService:
    """Employee-facing init/delete for camp-level reports."""

    def __init__(
        self,
        *,
        repository: CampReportsRepository,
        sections_repository: CampReportSectionsRepository,
        organizations_repository: OrganizationsRepository | None = None,
        audit_service: AuditService,
    ) -> None:
        self._repository = repository
        self._sections_repository = sections_repository
        self._organizations_repository = organizations_repository or OrganizationsRepository()
        self._audit_service = audit_service

    async def _resolve_camp_context(self, db: AsyncSession, *, camp_no: int) -> dict:
        row = await self._repository.get_camp_context(db, camp_no=camp_no)
        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_NOT_FOUND",
                message="Camp does not exist",
            )
        organization_id, organization_name, camp_start_date, camp_end_date = row
        return {
            "organization_id": int(organization_id),
            "organization_name": organization_name or "",
            "camp_start_date": camp_start_date,
            "camp_end_date": camp_end_date,
        }

    @staticmethod
    def _iso_date(value: date | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    def _build_initial_report(
        self,
        *,
        camp_name: str,
        camp_start_date: date | None,
        camp_end_date: date | None,
    ) -> dict:
        return {
            "meta": {
                "camp_name": camp_name,
                "summary_available": False,
                "refreshed_at": None,
                "next_refresh": None,
                "camp_start_date": self._iso_date(camp_start_date),
                "camp_end_date": self._iso_date(camp_end_date),
            }
        }

    async def _validate_department_slug(
        self,
        db: AsyncSession,
        *,
        organization_id: int,
        slug: str,
    ) -> None:
        result = await db.get(Organization, organization_id)
        if result is None:
            raise AppError(
                status_code=404,
                error_code="ORGANIZATION_NOT_FOUND",
                message="Organization does not exist",
            )
        allowed = get_department_slugs(result)
        normalized = slug.strip()
        if normalized not in allowed:
            raise AppError(
                status_code=404,
                error_code="DEPARTMENT_NOT_FOUND",
                message="Department does not exist for this organization",
            )

    async def init_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> CampReport:
        ensure_internal_employee(employee)

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        existing = await self._repository.get_overall_by_camp_no(db, camp_no=camp_no)
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            )

        camp_name = format_camp_name(context["organization_name"], context["camp_start_date"])
        report_payload = self._build_initial_report(
            camp_name=camp_name,
            camp_start_date=context["camp_start_date"],
            camp_end_date=context["camp_end_date"],
        )

        row = CampReport(
            report=report_payload,
            camp_no=camp_no,
            department=None,
            organization_id=context["organization_id"],
        )
        try:
            created = await self._repository.create(db, row)
        except IntegrityError:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            ) from None

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_INIT_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return created

    async def init_department_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        slug: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> CampReport:
        ensure_internal_employee(employee)

        normalized_slug = slug.strip()
        if not normalized_slug:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await self._validate_department_slug(
            db,
            organization_id=context["organization_id"],
            slug=normalized_slug,
        )

        existing = await self._repository.get_by_camp_no_and_department(
            db,
            camp_no=camp_no,
            department=normalized_slug,
        )
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            )

        camp_name = format_department_camp_name(
            context["organization_name"],
            normalized_slug,
            context["camp_start_date"],
        )
        report_payload = self._build_initial_report(
            camp_name=camp_name,
            camp_start_date=context["camp_start_date"],
            camp_end_date=context["camp_end_date"],
        )

        row = CampReport(
            report=report_payload,
            camp_no=camp_no,
            department=normalized_slug,
            organization_id=context["organization_id"],
        )
        try:
            created = await self._repository.create(db, row)
        except IntegrityError:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            ) from None

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_INIT_DEPARTMENT_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return created

    async def delete_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        ensure_internal_employee(employee)

        deleted = await self._repository.delete_overall(db, camp_no=camp_no)
        if deleted == 0:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_DELETE_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def delete_department_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        slug: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        ensure_internal_employee(employee)

        normalized_slug = slug.strip()
        if not normalized_slug:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        deleted = await self._repository.delete_by_department(
            db,
            camp_no=camp_no,
            department=normalized_slug,
        )
        if deleted == 0:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_DELETE_DEPARTMENT_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def purge_camp_reports_if_orphaned(
        self,
        db: AsyncSession,
        *,
        camp_no: int | None,
    ) -> int:
        """Delete all camp reports when no engagements remain for camp_no."""
        if camp_no is None:
            return 0
        remaining = await self._repository.count_engagements_for_camp_no(db, camp_no=camp_no)
        if remaining > 0:
            return 0
        return await self._repository.delete_all_for_camp_no(db, camp_no=camp_no)

    @staticmethod
    def _serialize_camp_report(row: CampReport) -> dict:
        return {
            "report_id": row.report_id,
            "camp_no": int(row.camp_no),
            "department": row.department,
            "organization_id": row.organization_id,
            "report": row.report,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    async def list_camp_reports(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
    ) -> list[dict]:
        ensure_internal_employee(employee)
        await self._resolve_camp_context(db, camp_no=camp_no)
        rows = await self._repository.list_by_camp_no(db, camp_no=camp_no)
        return [self._serialize_camp_report(row) for row in rows]

    async def _get_camp_report_row(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
    ) -> CampReport:
        if department is None:
            row = await self._repository.get_overall_by_camp_no(db, camp_no=camp_no)
        else:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            row = await self._repository.get_by_camp_no_and_department(
                db,
                camp_no=camp_no,
                department=normalized_department,
            )

        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )
        return row

    async def get_camp_report_meta(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        department: str | None = None,
    ) -> dict:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department)
        report = row.report or {}
        return dict(report.get("meta") or {})

    async def list_camp_report_section_keys(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        department: str | None = None,
    ) -> list[str]:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access_admin_or_org_manager(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department)
        report = row.report or {}
        return list(report.keys())

    async def get_camp_report_dashboard(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        section: str,
        department: str | None = None,
    ) -> dict:
        normalized_section = section.strip()
        if not normalized_section:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        section_row = await self._sections_repository.get_by_section_key(
            db,
            section_key=normalized_section,
        )
        if section_row is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_SECTION",
                message="Invalid report section",
            )

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department)
        report = row.report or {}
        if normalized_section not in report:
            raise AppError(
                status_code=404,
                error_code="SECTION_NOT_FOUND",
                message="Report section has not been refreshed",
            )
        return dict(report[normalized_section])

    async def refresh_camp_report_section(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        section: str,
        department: str | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        normalized_section = section.strip()
        if not normalized_section:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is None:
            row = await self._repository.get_overall_by_camp_no(db, camp_no=camp_no)
            audit_action = "EMPLOYEE_REFRESH_CAMP_REPORT_SECTION"
        else:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            row = await self._repository.get_by_camp_no_and_department(
                db,
                camp_no=camp_no,
                department=normalized_department,
            )
            department = normalized_department
            audit_action = "EMPLOYEE_REFRESH_DEPARTMENT_CAMP_REPORT_SECTION"

        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )

        section_row = await self._sections_repository.get_by_section_key(
            db,
            section_key=normalized_section,
        )
        if section_row is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_SECTION",
                message="Invalid report section",
            )

        builder = SECTION_BUILDERS.get(normalized_section)
        if builder is None:
            raise AppError(
                status_code=400,
                error_code="SECTION_NOT_IMPLEMENTED",
                message="Report section is not implemented",
            )

        built_payload = await self._build_section_payload(
            db,
            section_key=normalized_section,
            camp_no=camp_no,
            department=department,
            camp_start_date=context["camp_start_date"],
            camp_end_date=context["camp_end_date"],
        )

        report = dict(row.report or {})
        meta = dict(report.get("meta") or {})
        meta["refreshed_at"] = datetime.now(timezone.utc).isoformat()
        meta["summary_available"] = True
        report["meta"] = meta

        section_payload = {
            **built_payload,
            "name": section_row.section,
            "description": section_row.description,
        }
        report[normalized_section] = section_payload

        await self._repository.update_report(db, row, report)

        await self._audit_service.log_event(
            db,
            action=audit_action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "report_id": row.report_id,
            "section": section_payload,
        }

    async def _build_section_payload(
        self,
        db: AsyncSession,
        *,
        section_key: str,
        camp_no: int,
        department: str | None,
        camp_start_date: date | None,
        camp_end_date: date | None,
    ) -> dict:
        participation_reference = camp_start_date or date.today()
        age_reference = camp_end_date or date.today()

        if section_key == "participation_by_age":
            users = await self._repository.list_distinct_enrolled_users(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_participation_by_age(users, reference_date=participation_reference)

        if section_key == "kpis":
            metrics = await self._repository.compute_kpi_metrics(
                db,
                camp_no=camp_no,
                department=department,
                age_reference_date=age_reference,
            )
            return build_kpis(metrics)

        if section_key == "overall_risk_score":
            scores = await self._repository.list_metabolic_scores(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_overall_risk_score(scores)

        raise AppError(
            status_code=400,
            error_code="SECTION_NOT_IMPLEMENTED",
            message="Report section is not implemented",
        )
