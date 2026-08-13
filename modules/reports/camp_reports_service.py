"""Business logic for camp report init and delete."""

from __future__ import annotations

import asyncio
import math
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import (
    ensure_camp_access,
    ensure_camp_access_admin_or_org_manager,
    ensure_internal_employee,
)
from modules.employee.service import EmployeeContext
from modules.engagements.camp_no import (
    format_camp_name,
    format_city_camp_name,
    format_city_department_camp_name,
    format_department_camp_name,
)
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository
from modules.organizations.service import get_department_slugs
from modules.reports.camp_report_section_builders import (
    SECTION_BUILDERS,
    aggregate_top_healthy_habits,
    aggregate_top_healthy_profiles,
    aggregate_top_low_risk,
    build_blood_and_lab_intelligence,
    build_company_average_scores,
    build_distribution_by_gender_by_metabolic_syndrome,
    build_distribution_by_oxidative_stress,
    build_distribution_by_physical_activity_frequency,
    build_distribution_by_sleeping_hours,
    build_kpis,
    build_overall_risk_score_details,
    build_participation_by_age_details,
    build_positive_wins,
    build_ranking,
)
from modules.reports.camp_report_bts import (
    build_kpis_bts,
    build_not_implemented_bts,
    build_overall_risk_score_bts,
    build_participation_by_age_bts,
)
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.assessments.repository import AssessmentsRepository
from modules.diagnostics.repository import DiagnosticsRepository
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository, EnrolledAssessmentContext
from modules.reports.models import CampReport, IndividualHealthReport
from modules.reports.service import BLOOD_DATA_UNAVAILABLE_ERROR_CODES, ReportsService
from db.session import AsyncSessionLocal

# (base_seconds, per_unit_seconds, unit_kind)
# unit_kind: "participants" | "fitprint" | "health" | "kpi_metsights"
_SECTION_ESTIMATE_COSTS: dict[str, tuple[float, float, str]] = {
    "participation_by_age": (2.0, 0.01, "participants"),
    # KPI refresh may call Metsights fetch-collections per enrolled user without booking_id
    "kpis": (3.0, 0.35, "kpi_metsights"),
    "overall_risk_score": (3.0, 0.02, "participants"),
    "distribution_by_physical_activity_frequency": (2.0, 0.02, "participants"),
    "distribution_by_sleeping_hours": (2.0, 0.02, "participants"),
    "distribution_by_oxidative_stress": (3.0, 0.02, "participants"),
    "distribution_by_gender_by_metabolic_syndrome": (3.0, 0.02, "participants"),
    "blood_and_lab_intelligence": (5.0, 0.05, "participants"),
    "ranking": (5.0, 0.03, "participants"),
    # Loops health assessments (type 1/2); cache-only camp refresh is mostly DB work
    "positive_wins": (5.0, 0.08, "health"),
    "company_average_scores": (5.0, 2.5, "fitprint"),
}

_DEFAULT_ESTIMATE_COST: tuple[float, float, str] = (3.0, 0.02, "participants")

# Concurrent workers for positive_wins refresh / KPI Metsights checks (bounded by DB pool).
_POSITIVE_WINS_CONCURRENCY = 4
_KPI_BLOOD_METSIGHTS_CONCURRENCY = 4


class CampReportsService:
    """Employee-facing init/delete for camp-level reports."""

    def __init__(
        self,
        *,
        repository: CampReportsRepository,
        sections_repository: CampReportSectionsRepository,
        organizations_repository: OrganizationsRepository | None = None,
        audit_service: AuditService,
        reports_service: ReportsService,
        assessments_repository: AssessmentsRepository | None = None,
        diagnostics_repository: DiagnosticsRepository | None = None,
    ) -> None:
        self._repository = repository
        self._sections_repository = sections_repository
        self._organizations_repository = organizations_repository or OrganizationsRepository()
        self._audit_service = audit_service
        self._reports_service = reports_service
        self._assessments_repository = assessments_repository or AssessmentsRepository()
        self._diagnostics_repository = diagnostics_repository or DiagnosticsRepository()

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


    @staticmethod
    def _normalize_city(city: str | None) -> str:
        normalized = (city or "").strip()
        if not normalized:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return normalized

    async def _validate_camp_city(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        city: str,
    ) -> str:
        normalized = self._normalize_city(city)
        count = await self._repository.count_engagements_for_camp_city(
            db,
            camp_no=camp_no,
            city=normalized,
        )
        if count == 0:
            raise AppError(
                status_code=404,
                error_code="CITY_NOT_FOUND",
                message="City does not exist for this camp",
            )
        # Prefer canonical stored casing from engagements
        cities = await self._repository.list_distinct_cities_for_camp(db, camp_no=camp_no)
        for existing in cities:
            if existing.lower() == normalized.lower():
                return existing
        return normalized

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
            city=None,
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
            city=None,
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

    async def init_city_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        city: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> CampReport:
        ensure_internal_employee(employee)

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        normalized_city = await self._validate_camp_city(db, camp_no=camp_no, city=city)

        existing = await self._repository.get_by_camp_no_and_city(
            db,
            camp_no=camp_no,
            city=normalized_city,
        )
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            )

        camp_name = format_city_camp_name(
            context["organization_name"],
            normalized_city,
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
            department=None,
            city=normalized_city,
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
            action="EMPLOYEE_INIT_CITY_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return created

    async def init_city_department_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        city: str,
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
        normalized_city = await self._validate_camp_city(db, camp_no=camp_no, city=city)
        await self._validate_department_slug(
            db,
            organization_id=context["organization_id"],
            slug=normalized_slug,
        )

        existing = await self._repository.get_by_camp_no_city_and_department(
            db,
            camp_no=camp_no,
            city=normalized_city,
            department=normalized_slug,
        )
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            )

        camp_name = format_city_department_camp_name(
            context["organization_name"],
            normalized_city,
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
            city=normalized_city,
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
            action="EMPLOYEE_INIT_CITY_DEPARTMENT_CAMP_REPORT",
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

        # Delete overall + every department report for this camp_no.
        deleted = await self._repository.delete_all_for_camp_no(db, camp_no=camp_no)
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
            "city": row.city,
            "organization_id": row.organization_id,
            "report": row.report,
            "report_bts": row.report_bts,
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

    @staticmethod
    def _camp_participant_to_dict(row: tuple) -> dict:
        (
            engagement_participant_id,
            engagement_id,
            user_id,
            first_name,
            last_name,
            phone,
            gender,
            participant_blood_group,
            participant_department,
        ) = row
        return {
            "engagement_participant_id": engagement_participant_id,
            "engagement_id": engagement_id,
            "user_id": user_id,
            "first_name": first_name,
            "last_name": last_name,
            "phone": phone,
            "gender": gender,
            "participant_blood_group": participant_blood_group,
            "participant_department": participant_department,
        }

    async def list_camp_participants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        page: int,
        limit: int,
        department: str | None = None,
        city: str | None = None,
    ) -> tuple[list[dict], int]:
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

        if city is not None:
            city = await self._validate_camp_city(db, camp_no=camp_no, city=city)

        rows = await self._repository.list_participants_by_camp_no(
            db,
            camp_no=camp_no,
            page=page,
            limit=limit,
            department=department,
            city=city,
        )
        total = await self._repository.count_participants_by_camp_no(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        return [self._camp_participant_to_dict(row) for row in rows], total

    async def _get_camp_report_row(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None = None,
    ) -> CampReport:
        if city is None:
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
        else:
            normalized_city = self._normalize_city(city)
            if department is None:
                row = await self._repository.get_by_camp_no_and_city(
                    db,
                    camp_no=camp_no,
                    city=normalized_city,
                )
            else:
                normalized_department = department.strip()
                if not normalized_department:
                    raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
                row = await self._repository.get_by_camp_no_city_and_department(
                    db,
                    camp_no=camp_no,
                    city=normalized_city,
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
        city: str | None = None,
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

        if city is not None:
            city = await self._validate_camp_city(db, camp_no=camp_no, city=city)

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department,
            city=city,
        )
        report = row.report or {}
        return dict(report.get("meta") or {})

    async def list_camp_report_section_keys(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
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

        if city is not None:
            city = await self._validate_camp_city(db, camp_no=camp_no, city=city)

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department,
            city=city,
        )
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
        city: str | None = None,
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

        if city is not None:
            city = await self._validate_camp_city(db, camp_no=camp_no, city=city)

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

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department,
            city=city,
        )
        report = row.report or {}
        if normalized_section not in report:
            raise AppError(
                status_code=404,
                error_code="SECTION_NOT_FOUND",
                message="Report section has not been refreshed",
            )
        return dict(report[normalized_section])

    async def update_camp_report_section_payload(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        section: str,
        payload: dict[str, Any],
        department: str | None = None,
        city: str | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Replace ``report[section]`` with the given JSON object (manual admin edit)."""
        ensure_internal_employee(employee)

        normalized_section = section.strip()
        if not normalized_section:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if not isinstance(payload, dict):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="payload must be a JSON object",
            )

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

        if city is not None:
            city = await self._validate_camp_city(db, camp_no=camp_no, city=city)

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

        row = await self._load_camp_report_row_for_refresh(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )

        report = dict(row.report or {})
        section_payload = dict(payload)
        report[normalized_section] = section_payload
        await self._repository.update_report(db, row, report)

        await self._audit_service.log_event(
            db,
            action=self._update_section_audit_action(department=department, city=city),
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

    @staticmethod
    def _update_section_audit_action(
        *,
        department: str | None,
        city: str | None,
    ) -> str:
        if city is None and department is None:
            return "EMPLOYEE_UPDATE_CAMP_REPORT_SECTION"
        if city is None and department is not None:
            return "EMPLOYEE_UPDATE_DEPARTMENT_CAMP_REPORT_SECTION"
        if city is not None and department is None:
            return "EMPLOYEE_UPDATE_CITY_CAMP_REPORT_SECTION"
        return "EMPLOYEE_UPDATE_CITY_DEPARTMENT_CAMP_REPORT_SECTION"

    @staticmethod
    def _refresh_audit_action(
        *,
        department: str | None,
        city: str | None,
        cron: bool,
    ) -> str:
        prefix = "CRON" if cron else "EMPLOYEE"
        if city is None and department is None:
            return f"{prefix}_REFRESH_CAMP_REPORT_SECTION"
        if city is None and department is not None:
            return f"{prefix}_REFRESH_DEPARTMENT_CAMP_REPORT_SECTION"
        if city is not None and department is None:
            return f"{prefix}_REFRESH_CITY_CAMP_REPORT_SECTION"
        return f"{prefix}_REFRESH_CITY_DEPARTMENT_CAMP_REPORT_SECTION"

    async def _load_camp_report_row_for_refresh(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None,
    ) -> CampReport:
        if city is None and department is None:
            row = await self._repository.get_overall_by_camp_no(db, camp_no=camp_no)
        elif city is None and department is not None:
            row = await self._repository.get_by_camp_no_and_department(
                db,
                camp_no=camp_no,
                department=department,
            )
        elif city is not None and department is None:
            row = await self._repository.get_by_camp_no_and_city(
                db,
                camp_no=camp_no,
                city=city,
            )
        else:
            row = await self._repository.get_by_camp_no_city_and_department(
                db,
                camp_no=camp_no,
                city=city,
                department=department,
            )
        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )
        return row

    async def _refresh_camp_report_section_core(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        section: str,
        department: str | None,
        city: str | None,
        context: dict,
        audit_action: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        user_id: int | None = None,
    ) -> dict:
        normalized_section = section.strip()
        if not normalized_section:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        row = await self._load_camp_report_row_for_refresh(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
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

        checked_at = datetime.now(timezone.utc).isoformat()
        report = dict(row.report or {})
        previous_section = report.get(normalized_section)
        previous_data = None
        if isinstance(previous_section, dict) and isinstance(previous_section.get("data"), dict):
            previous_data = previous_section.get("data")

        kpi_metrics: dict[str, Any] | None = None
        age_bts_details: dict[str, Any] | None = None
        ors_bts_details: dict[str, Any] | None = None
        if normalized_section == "kpis":
            built_payload, kpi_metrics = await self._build_kpis_payload_with_metrics(
                db,
                camp_no=camp_no,
                department=department,
                city=city,
                age_reference_date=context["camp_end_date"] or date.today(),
            )
        elif normalized_section == "participation_by_age":
            built_payload, age_bts_details = await self._build_participation_by_age_with_details(
                db,
                camp_no=camp_no,
                department=department,
                city=city,
                camp_start_date=context["camp_start_date"],
            )
        elif normalized_section == "overall_risk_score":
            built_payload, ors_bts_details = await self._build_overall_risk_score_with_details(
                db,
                camp_no=camp_no,
                department=department,
                city=city,
            )
        else:
            built_payload = await self._build_section_payload(
                db,
                section_key=normalized_section,
                camp_no=camp_no,
                department=department,
                city=city,
                camp_start_date=context["camp_start_date"],
                camp_end_date=context["camp_end_date"],
            )

        meta = dict(report.get("meta") or {})
        meta["refreshed_at"] = checked_at
        meta["summary_available"] = True
        report["meta"] = meta

        section_payload = {
            **built_payload,
            "name": section_row.section,
            "description": section_row.description,
        }
        report[normalized_section] = section_payload

        report_bts = dict(row.report_bts or {})
        # Refresh/validate always writes ``section_payload`` first. BTS must validate
        # that just-written data — not the pre-refresh snapshot. Comparing to
        # ``previous_data`` made the first refresh look like a mismatch even though
        # the report was already corrected (same for KPIs, participation_by_age,
        # and any future section BTS).
        if normalized_section == "kpis":
            expected_data = section_payload.get("data") if isinstance(section_payload.get("data"), dict) else {}
            blood_details = dict((kpi_metrics or {}).get("blood_details") or {})
            kpi_details = dict((kpi_metrics or {}).get("kpi_bts_details") or {})
            if previous_data is not None:
                kpi_details["previous"] = previous_data
            report_bts[normalized_section] = build_kpis_bts(
                expected_data=expected_data,
                stored_data=expected_data,
                blood_details=blood_details,
                checked_at=checked_at,
                kpi_details=kpi_details,
            )
        elif normalized_section == "participation_by_age":
            expected_data = section_payload.get("data") if isinstance(section_payload.get("data"), dict) else {}
            age_details = dict(age_bts_details or {})
            if previous_data is not None:
                age_details["previous"] = previous_data
            report_bts[normalized_section] = build_participation_by_age_bts(
                expected_data=expected_data,
                stored_data=expected_data,
                details=age_details,
                checked_at=checked_at,
            )
        elif normalized_section == "overall_risk_score":
            expected_data = section_payload.get("data") if isinstance(section_payload.get("data"), dict) else {}
            ors_details = dict(ors_bts_details or {})
            if previous_data is not None:
                ors_details["previous"] = previous_data
            report_bts[normalized_section] = build_overall_risk_score_bts(
                expected_data=expected_data,
                stored_data=expected_data,
                details=ors_details,
                checked_at=checked_at,
            )
        else:
            report_bts[normalized_section] = build_not_implemented_bts(checked_at=checked_at)

        await self._repository.update_report_and_bts(db, row, report, report_bts)

        await self._audit_service.log_event(
            db,
            action=audit_action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return {
            "report_id": row.report_id,
            "section": section_payload,
            "report_bts": report_bts.get(normalized_section),
        }

    @staticmethod
    def _resolve_estimate_cost(section: str, action: str) -> tuple[float, float, str]:
        normalized_section = section.strip()
        normalized_action = action.strip().lower()
        if normalized_action not in {"validate", "refresh"}:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="action must be 'refresh' or 'validate'",
            )
        # Validate now refreshes the section and writes report_bts — same cost as refresh.
        return _SECTION_ESTIMATE_COSTS.get(normalized_section, _DEFAULT_ESTIMATE_COST)

    async def estimate_camp_report_operations(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        operations: list[dict[str, Any]],
    ) -> dict:
        ensure_internal_employee(employee)
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        timeout_seconds = settings.CAMP_REPORT_CLIENT_TIMEOUT_SECONDS
        count_cache: dict[tuple[str, str | None], int] = {}
        results: list[dict[str, Any]] = []

        for op in operations:
            section = str(op.get("section") or "").strip()
            action = str(op.get("action") or "").strip().lower()
            department_raw = op.get("department")
            department: str | None = None
            if department_raw is not None:
                normalized_department = str(department_raw).strip()
                if not normalized_department:
                    raise AppError(
                        status_code=400,
                        error_code="INVALID_INPUT",
                        message="Invalid request",
                    )
                await self._validate_department_slug(
                    db,
                    organization_id=context["organization_id"],
                    slug=normalized_department,
                )
                department = normalized_department

            if not section:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="section is required",
                )

            base, per_unit, unit_kind = self._resolve_estimate_cost(section, action)

            participant_key = ("participants", department)
            if participant_key not in count_cache:
                count_cache[participant_key] = await self._repository.count_participants_by_camp_no(
                    db,
                    camp_no=camp_no,
                    department=department,
                )
            participant_count = count_cache[participant_key]

            if unit_kind == "fitprint":
                fitprint_key = ("fitprint", department)
                if fitprint_key not in count_cache:
                    count_cache[fitprint_key] = await self._repository.count_fitprint_assessment_contexts(
                        db,
                        camp_no=camp_no,
                        department=department,
                    )
                unit_count = count_cache[fitprint_key]
            elif unit_kind == "health":
                health_key = ("health", department)
                if health_key not in count_cache:
                    count_cache[health_key] = await self._repository.count_health_assessment_contexts(
                        db,
                        camp_no=camp_no,
                        department=department,
                    )
                unit_count = count_cache[health_key]
            elif unit_kind == "kpi_metsights":
                metsights_key = ("kpi_metsights", department)
                if metsights_key not in count_cache:
                    candidates = await self._repository.list_kpi_blood_candidates(
                        db,
                        camp_no=camp_no,
                        department=department,
                    )
                    count_cache[metsights_key] = sum(
                        1
                        for _uid, has_booking_id, record_id in candidates
                        if (not has_booking_id) and record_id
                    )
                unit_count = count_cache[metsights_key]
            else:
                unit_count = participant_count

            estimated_seconds = max(1, math.ceil(base + per_unit * unit_count))
            allowed = estimated_seconds <= timeout_seconds
            results.append(
                {
                    "section": section,
                    "action": action,
                    "department": department,
                    "participant_count": participant_count,
                    "unit_count": unit_count,
                    "estimated_seconds": estimated_seconds,
                    "allowed": allowed,
                }
            )

        total_estimated_seconds = sum(item["estimated_seconds"] for item in results)
        return {
            "timeout_seconds": timeout_seconds,
            "operations": results,
            "total_estimated_seconds": total_estimated_seconds,
            "all_allowed": all(item["allowed"] for item in results),
        }

    async def refresh_camp_report_section(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        section: str,
        department: str | None = None,
        city: str | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if city is not None:
            city = await self._validate_camp_city(db, camp_no=camp_no, city=city)

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

        return await self._refresh_camp_report_section_core(
            db,
            camp_no=camp_no,
            section=section,
            department=department,
            city=city,
            context=context,
            audit_action=self._refresh_audit_action(
                department=department,
                city=city,
                cron=False,
            ),
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
            user_id=employee.user_id,
        )

    async def refresh_camp_report_section_for_cron(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        section: str,
        department: str | None = None,
        city: str | None = None,
    ) -> dict:
        """Refresh one section without employee auth (CLI / cron use)."""
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        return await self._refresh_camp_report_section_core(
            db,
            camp_no=camp_no,
            section=section,
            department=department,
            city=city,
            context=context,
            audit_action=self._refresh_audit_action(
                department=department,
                city=city,
                cron=True,
            ),
            ip_address="cron",
            user_agent="db.jobs.refresh_camp_reports",
            endpoint="cron:refresh_camp_reports",
            user_id=None,
        )

    async def _positive_wins_for_context(
        self,
        db: AsyncSession,
        *,
        ctx: EnrolledAssessmentContext,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str | None]], list[str]]:
        """Compute low-risk / habits / profiles for one health assessment context.

        Camp report refresh uses cached DB data only — no live Metsights/Healthians
        fan-out per participant (those calls belong on single-user overview/load).
        """
        individual_report = ctx.individual_report
        # Blood may live on a different IHR row for the same engagement.
        if individual_report is None or individual_report.blood_parameters is None:
            blood_report = await self._reports_service._get_blood_individual_report(
                db,
                user_id=int(ctx.assessment_instance.user_id),
                engagement_id=int(ctx.assessment_instance.engagement_id),
                assessment_instance_id=int(ctx.assessment_instance.assessment_instance_id),
            )
            if blood_report is not None:
                if individual_report is None:
                    individual_report = blood_report
                elif individual_report.blood_parameters is None:
                    individual_report.blood_parameters = blood_report.blood_parameters

        low_risk_items = await self._reports_service.compute_low_risk_for_instance(
            db,
            assessment_instance=ctx.assessment_instance,
            package=ctx.package,
            individual_report=individual_report,
            allow_remote_fetch=False,
        )
        low_risk = [
            {
                "code": item.code,
                "name": item.name,
                "risk_status": item.risk_status,
                "risk_score_scaled": item.risk_score_scaled,
            }
            for item in low_risk_items
        ]
        try:
            habits, profiles = await self._reports_service.compute_healthy_habits_and_profiles_for_instance(
                db,
                assessment_instance=ctx.assessment_instance,
                package=ctx.package,
                engagement=ctx.engagement,
                individual_report=individual_report,
                user_gender=ctx.user_gender,
                allow_provider_fetch=False,
            )
        except AppError as exc:
            if exc.error_code not in BLOOD_DATA_UNAVAILABLE_ERROR_CODES:
                raise
            habits, profiles = [], []
        return (
            low_risk,
            [{"habit_key": h.habit_key, "habit_label": h.habit_label} for h in habits],
            profiles,
        )

    async def _positive_wins_for_context_isolated(
        self,
        *,
        assessment_instance_id: int,
        user_gender: str | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str | None]], list[str]]:
        """Run one participant's positive_wins work on a dedicated DB session."""
        async with AsyncSessionLocal() as session:
            ai = await session.get(AssessmentInstance, assessment_instance_id)
            if ai is None:
                return [], [], []
            package = await session.get(AssessmentPackage, ai.package_id) if ai.package_id else None
            engagement = await session.get(Engagement, ai.engagement_id) if ai.engagement_id else None
            # Multiple IHR rows per assessment are allowed; prefer blood, then latest.
            ihr_result = await session.execute(
                select(IndividualHealthReport)
                .where(
                    IndividualHealthReport.assessment_instance_id == assessment_instance_id
                )
                .order_by(
                    IndividualHealthReport.blood_parameters.isnot(None).desc(),
                    IndividualHealthReport.report_id.desc(),
                )
                .limit(1)
            )
            individual_report = ihr_result.scalar_one_or_none()
            ctx = EnrolledAssessmentContext(
                assessment_instance=ai,
                package=package,
                engagement=engagement,
                individual_report=individual_report,
                user_gender=user_gender,
            )
            result = await self._positive_wins_for_context(session, ctx=ctx)
            await session.commit()
            return result

    async def _compute_positive_wins_payload(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None = None,
    ) -> dict:
        contexts = await self._repository.list_health_assessment_contexts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        if not contexts:
            return build_positive_wins(
                low_risk=[],
                healthy_habits=[],
                healthy_profiles=[],
            )

        # Small camps: keep work on the request session (avoids pool churn).
        if len(contexts) <= _POSITIVE_WINS_CONCURRENCY:
            participant_habits: list[list[dict[str, str | None]]] = []
            participant_profiles: list[list[str]] = []
            participant_low_risk: list[list[dict[str, Any]]] = []
            for ctx in contexts:
                low_risk, habits, profiles = await self._positive_wins_for_context(db, ctx=ctx)
                participant_low_risk.append(low_risk)
                participant_habits.append(habits)
                participant_profiles.append(profiles)
            return build_positive_wins(
                low_risk=aggregate_top_low_risk(participant_low_risk),
                healthy_habits=aggregate_top_healthy_habits(participant_habits),
                healthy_profiles=aggregate_top_healthy_profiles(participant_profiles),
            )

        semaphore = asyncio.Semaphore(_POSITIVE_WINS_CONCURRENCY)

        async def run_one(
            ctx: EnrolledAssessmentContext,
        ) -> tuple[list[dict[str, Any]], list[dict[str, str | None]], list[str]]:
            async with semaphore:
                return await self._positive_wins_for_context_isolated(
                    assessment_instance_id=int(ctx.assessment_instance.assessment_instance_id),
                    user_gender=ctx.user_gender,
                )

        results = await asyncio.gather(
            *[run_one(ctx) for ctx in contexts],
            return_exceptions=True,
        )

        participant_habits = []
        participant_profiles = []
        participant_low_risk = []
        for result in results:
            if isinstance(result, BaseException):
                if isinstance(result, AppError) and result.error_code in BLOOD_DATA_UNAVAILABLE_ERROR_CODES:
                    participant_low_risk.append([])
                    participant_habits.append([])
                    participant_profiles.append([])
                    continue
                raise result
            low_risk, habits, profiles = result
            participant_low_risk.append(low_risk)
            participant_habits.append(habits)
            participant_profiles.append(profiles)

        return build_positive_wins(
            low_risk=aggregate_top_low_risk(participant_low_risk),
            healthy_habits=aggregate_top_healthy_habits(participant_habits),
            healthy_profiles=aggregate_top_healthy_profiles(participant_profiles),
        )

    async def _compute_company_average_scores_payload(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None = None,
    ) -> dict:
        contexts = await self._repository.list_fitprint_assessment_contexts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )

        participant_scores: list[dict[str, float | None]] = []

        for ctx in contexts:
            try:
                report_dict = await self._reports_service._resolve_report_dict_for_instance(
                    db,
                    assessment_instance=ctx.assessment_instance,
                    package=ctx.package,
                    individual_report=ctx.individual_report,
                )
            except Exception:
                continue

            fitness_spec = report_dict.get("fitness_specification") or {}
            activity_spec = report_dict.get("activity_specification") or {}

            raw_lifestyle = fitness_spec.get("score") if isinstance(fitness_spec, dict) else None
            lifestyle_score = float(raw_lifestyle) if isinstance(raw_lifestyle, (int, float)) else None

            raw_fitness = activity_spec.get("score") if isinstance(activity_spec, dict) else None
            fitness_score = float(raw_fitness) if isinstance(raw_fitness, (int, float)) else None

            all_instances = await self._assessments_repository.list_instances_for_user_engagement(
                db,
                user_id=ctx.assessment_instance.user_id,
                engagement_id=ctx.assessment_instance.engagement_id,
            )
            source_ids = [inst.assessment_instance_id for inst in all_instances]

            nutrition_score: float | None = None
            if source_ids:
                try:
                    lookup, key_to_qid = await self._reports_service._build_questionnaire_lookup(
                        db,
                        source_assessment_instance_ids=source_ids,
                    )
                    option_reverse_map = await self._reports_service._build_option_reverse_map(db, key_to_qid)
                    nutrition_payload = self._reports_service._build_nutrition_api_payload(
                        lookup, user_gender=ctx.user_gender, option_reverse_map=option_reverse_map,
                    )
                    nutrition_response = await self._reports_service._call_nutrition_api(
                        db,
                        nutrition_payload,
                        user_id=ctx.assessment_instance.user_id,
                        engagement_id=ctx.assessment_instance.engagement_id,
                    )
                    raw_nutrition = nutrition_response.get("nutrition_score")
                    nutrition_score = float(raw_nutrition) if isinstance(raw_nutrition, (int, float)) else None
                except Exception:
                    nutrition_score = None

            participant_scores.append({
                "nutrition": nutrition_score,
                "fitness": fitness_score,
                "lifestyle": lifestyle_score,
            })

        return build_company_average_scores(participant_scores)

    _BLOOD_INTELLIGENCE_GROUP_KEYS = (
        "vitamin_profile",
        "diabetes_profile",
        "lipid_profile",
        "inflammatory",
    )

    _BLOOD_INTELLIGENCE_COMBINED_KEYS: dict[str, list[str]] = {
        "lipid_profile": ["cholesterol_total", "triglycerides", "ldl_cholestrol"],
        "inflammatory": ["homocysteine", "hs-crp", "esr"],
    }

    async def _compute_blood_and_lab_intelligence_payload(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None = None,
    ) -> dict:
        participants = await self._repository.list_blood_parameters_by_gender(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )

        group_tests: list[tuple[str, list]] = []
        for group_key in self._BLOOD_INTELLIGENCE_GROUP_KEYS:
            group = await self._diagnostics_repository.get_group_by_group_key(db, group_key=group_key)
            if group is None:
                continue
            tests = await self._diagnostics_repository.get_parameters_for_group(db, group_id=group.group_id)
            group_tests.append((group_key, tests))

        tests_by_group: dict[str, dict[str, object]] = {}
        for group_key, tests in group_tests:
            tests_by_group[group_key] = {t.parameter_key: t for t in tests if t.parameter_key}

        group_stats: dict[str, dict[str, dict[str, int]]] = {}
        for group_key, tests in group_tests:
            test_stats: dict[str, dict[str, int]] = {}
            for test in tests:
                param_key = test.parameter_key
                if not param_key:
                    continue
                in_range_count = 0
                total_valid = 0

                for gender, blood_params in participants:
                    value, lower_range, higher_range = self._extract_test_value_and_range(
                        blood_params, test, gender
                    )
                    if value is None or lower_range is None or higher_range is None:
                        continue
                    total_valid += 1
                    if lower_range <= value <= higher_range:
                        in_range_count += 1

                test_stats[param_key] = {"in_range": in_range_count, "total": total_valid}

            combined_params = self._BLOOD_INTELLIGENCE_COMBINED_KEYS.get(group_key)
            if combined_params:
                combined_key = "__".join(combined_params)
                group_test_map = tests_by_group.get(group_key, {})
                combined_tests = [group_test_map[k] for k in combined_params if k in group_test_map]

                if combined_tests:
                    combined_in_range = 0
                    combined_total = 0

                    for gender, blood_params in participants:
                        all_valid = True
                        all_in_range = True
                        for ct in combined_tests:
                            value, lower_range, higher_range = self._extract_test_value_and_range(
                                blood_params, ct, gender
                            )
                            if value is None or lower_range is None or higher_range is None:
                                all_valid = False
                                break
                            if not (lower_range <= value <= higher_range):
                                all_in_range = False
                        if not all_valid:
                            continue
                        combined_total += 1
                        if all_in_range:
                            combined_in_range += 1

                    test_stats[combined_key] = {"in_range": combined_in_range, "total": combined_total}

            group_stats[group_key] = test_stats

        return build_blood_and_lab_intelligence(group_stats)

    @staticmethod
    def _extract_test_value_and_range(
        blood_params: Any,
        test,
        gender: str | None,
    ) -> tuple[float | None, float | None, float | None]:
        """Extract value and range for a single test from a participant's blood_parameters."""
        from modules.reports.blood_parameters_read_service import BloodParametersReadService
        from modules.reports.blood_parameters_schemas import (
            is_canonical_blood_parameters,
            is_grouped_blood_parameters,
            is_legacy_healthians_format,
            is_legacy_metsights_flat_format,
        )

        if is_grouped_blood_parameters(blood_params) or is_canonical_blood_parameters(blood_params):
            return BloodParametersReadService.extract_canonical_value_and_range(
                blood_params,
                parameter_key=test.parameter_key,
                gender=gender,
                catalog_lower_male=test.low_risk_lower_range_male,
                catalog_higher_male=test.low_risk_higher_range_male,
                catalog_lower_female=test.low_risk_lower_range_female,
                catalog_higher_female=test.low_risk_higher_range_female,
            )

        if is_legacy_metsights_flat_format(blood_params):
            value: float | None = None
            lower_range: float | None = None
            higher_range: float | None = None
            param_key = test.parameter_key
            if not param_key:
                return None, None, None
            raw_val = blood_params.get(param_key)
            if raw_val is not None:
                try:
                    value = float(raw_val)
                except (TypeError, ValueError):
                    pass
            normalized_gender = (gender or "").strip().lower()
            if normalized_gender in ("male", "m", "1"):
                if test.low_risk_lower_range_male is not None:
                    lower_range = float(test.low_risk_lower_range_male)
                if test.low_risk_higher_range_male is not None:
                    higher_range = float(test.low_risk_higher_range_male)
            elif normalized_gender in ("female", "f", "2"):
                if test.low_risk_lower_range_female is not None:
                    lower_range = float(test.low_risk_lower_range_female)
                if test.low_risk_higher_range_female is not None:
                    higher_range = float(test.low_risk_higher_range_female)
            return value, lower_range, higher_range

        if not is_legacy_healthians_format(blood_params):
            return None, None, None

        value = None
        lower_range = None
        higher_range = None
        external_pid = test.external_parameter_id
        if external_pid is None:
            return None, None, None
        for entry in blood_params["digital_data"]:
            entry_pid = entry.get("parameter_id")
            if entry_pid is not None and str(entry_pid) == str(external_pid):
                raw_val = entry.get("value")
                if raw_val is not None:
                    try:
                        value = float(raw_val)
                    except (TypeError, ValueError):
                        pass
                raw_min = entry.get("min_range")
                if raw_min is not None:
                    try:
                        lower_range = float(raw_min)
                    except (TypeError, ValueError):
                        pass
                raw_max = entry.get("max_range")
                if raw_max is not None:
                    try:
                        higher_range = float(raw_max)
                    except (TypeError, ValueError):
                        pass
                break

        return value, lower_range, higher_range

    async def _check_metsights_sample_collection(self, *, record_id: str) -> str:
        """Return collected | missing | failed for a Metsights fetch-collections check."""
        try:
            await self._reports_service._metsights_service.get_fetch_collections(record_id=record_id)
            return "collected"
        except AppError as exc:
            if exc.error_code == "BLOOD_SAMPLE_NOT_COLLECTED":
                return "missing"
            return "failed"
        except Exception:
            return "failed"

    async def _resolve_kpi_blood_tested_users(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None,
    ) -> tuple[set[int], dict[str, int]]:
        candidates = await self._repository.list_kpi_blood_candidates(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        blood_tested: set[int] = set()
        with_booking_id = 0
        no_record_id = 0
        to_check: list[tuple[int, str]] = []
        for user_id, has_booking_id, record_id in candidates:
            if has_booking_id:
                blood_tested.add(user_id)
                with_booking_id += 1
                continue
            if not record_id:
                no_record_id += 1
                continue
            to_check.append((user_id, record_id))

        with_metsights_collection = 0
        missing_collection = 0
        check_failed = 0

        if to_check:
            semaphore = asyncio.Semaphore(_KPI_BLOOD_METSIGHTS_CONCURRENCY)

            async def _one(uid: int, rid: str) -> tuple[int, str]:
                async with semaphore:
                    status = await self._check_metsights_sample_collection(record_id=rid)
                    return uid, status

            results = await asyncio.gather(*[_one(uid, rid) for uid, rid in to_check])
            for uid, status in results:
                if status == "collected":
                    blood_tested.add(uid)
                    with_metsights_collection += 1
                elif status == "missing":
                    missing_collection += 1
                else:
                    check_failed += 1

        blood_details = {
            "with_booking_id": with_booking_id,
            "with_metsights_collection": with_metsights_collection,
            "missing_collection": missing_collection,
            "no_record_id": no_record_id,
            "check_failed": check_failed,
            "users_needing_metsights_check": len(to_check),
        }
        return blood_tested, blood_details

    async def _build_kpis_payload_with_metrics(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None,
        age_reference_date: date,
    ) -> tuple[dict, dict]:
        blood_tested_user_ids, blood_details = await self._resolve_kpi_blood_tested_users(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        metrics = await self._repository.compute_kpi_metrics(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
            age_reference_date=age_reference_date,
            blood_tested_user_ids=blood_tested_user_ids,
            blood_details=blood_details,
        )
        return build_kpis(metrics), metrics

    @staticmethod
    def _age_participation_scope_label(*, department: str | None, city: str | None) -> str:
        if city and department:
            return f"City: {city} · Department: {department}"
        if city:
            return f"City: {city}"
        if department:
            return f"Department: {department}"
        return "Whole camp"

    async def _build_overall_risk_score_with_details(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None,
    ) -> tuple[dict, dict]:
        status_rows = await self._repository.list_metabolic_score_status(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        total_enrolled = await self._repository.count_enrolled_users(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        bio_ai_reports = await self._repository.count_bio_ai_reports(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        return build_overall_risk_score_details(
            status_rows,
            total_enrolled=total_enrolled,
            bio_ai_reports=bio_ai_reports,
            scope_label=self._age_participation_scope_label(
                department=department,
                city=city,
            ),
        )

    async def _build_participation_by_age_with_details(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None,
        camp_start_date: date | None,
    ) -> tuple[dict, dict]:
        participation_reference = camp_start_date or date.today()
        users = await self._repository.list_distinct_enrolled_users_for_age_bts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        engagement_count = await self._repository.count_engagements_for_camp_scope(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        participant_rows = await self._repository.count_participants_by_camp_no(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        return build_participation_by_age_details(
            users,
            reference_date=participation_reference,
            engagement_count=engagement_count,
            participant_rows=participant_rows,
            scope_label=self._age_participation_scope_label(
                department=department,
                city=city,
            ),
        )

    async def _build_section_payload(
        self,
        db: AsyncSession,
        *,
        section_key: str,
        camp_no: int,
        department: str | None,
        city: str | None = None,
        camp_start_date: date | None,
        camp_end_date: date | None,
    ) -> dict:
        age_reference = camp_end_date or date.today()

        if section_key == "participation_by_age":
            payload, _details = await self._build_participation_by_age_with_details(
                db,
                camp_no=camp_no,
                department=department,
                city=city,
                camp_start_date=camp_start_date,
            )
            return payload

        if section_key == "kpis":
            payload, _metrics = await self._build_kpis_payload_with_metrics(
                db,
                camp_no=camp_no,
                department=department,
                city=city,
                age_reference_date=age_reference,
            )
            return payload

        if section_key == "overall_risk_score":
            payload, _details = await self._build_overall_risk_score_with_details(
                db,
                camp_no=camp_no,
                department=department,
                city=city,
            )
            return payload

        if section_key == "distribution_by_oxidative_stress":
            scores = await self._repository.list_oxidative_stress_scores(
                db,
                camp_no=camp_no,
                department=department,
            city=city,
            )
            return build_distribution_by_oxidative_stress(scores)

        if section_key == "distribution_by_physical_activity_frequency":
            rows = await self._repository.list_physical_activity_frequency_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            city=city,
            )
            return build_distribution_by_physical_activity_frequency(rows)

        if section_key == "distribution_by_sleeping_hours":
            rows = await self._repository.list_sleeping_hours_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            city=city,
            )
            return build_distribution_by_sleeping_hours(rows)

        if section_key == "distribution_by_gender_by_metabolic_syndrome":
            rows = await self._repository.list_health_reports_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            city=city,
            )
            return build_distribution_by_gender_by_metabolic_syndrome(rows)

        if section_key == "positive_wins":
            return await self._compute_positive_wins_payload(
                db,
                camp_no=camp_no,
                department=department,
            city=city,
            )

        if section_key == "company_average_scores":
            return await self._compute_company_average_scores_payload(
                db,
                camp_no=camp_no,
                department=department,
            city=city,
            )

        if section_key == "blood_and_lab_intelligence":
            return await self._compute_blood_and_lab_intelligence_payload(
                db,
                camp_no=camp_no,
                department=department,
            city=city,
            )

        if section_key == "ranking":
            return await self._compute_ranking_payload(
                db,
                camp_no=camp_no,
            )

        raise AppError(
            status_code=400,
            error_code="SECTION_NOT_IMPLEMENTED",
            message="Report section is not implemented",
        )

    async def _compute_ranking_payload(self, db: AsyncSession, *, camp_no: int) -> dict:
        """Compute per-city camp ranking and return the ranking section payload."""
        from datetime import date as _date
        from sqlalchemy import select as _select

        cities = await self._repository.list_distinct_cities_for_camp(db, camp_no=camp_no)
        if not cities:
            return build_ranking({})

        engagement_result = await db.execute(
            _select(Engagement.organization_id)
            .where(Engagement.camp_no == camp_no, Engagement.organization_id.isnot(None))
            .limit(1)
        )
        org_id_row = engagement_result.first()

        org: Organization | None = None
        if org_id_row:
            org_result = await db.execute(
                _select(Organization).where(Organization.organization_id == org_id_row[0])
            )
            org = org_result.scalar_one_or_none()

        industry_key = org.industry_key if org is not None else None
        organization_id = org.organization_id if org is not None else None

        data: dict[str, dict[str, int | None]] = {}
        for city in cities:
            year = await self._repository.get_camp_city_year(db, camp_no=camp_no, city=city)
            if year is None:
                year = _date.today().year

            this_scores = await self._repository.list_metabolic_scores(
                db,
                camp_no=camp_no,
                department=None,
                city=city,
            )
            this_avg: float | None = (
                round(sum(this_scores) / len(this_scores), 2) if this_scores else None
            )

            peers = await self._repository.list_camp_avg_metabolic_scores_by_city(
                db,
                city=city,
                year=year,
            )

            if (
                this_avg is not None
                and organization_id is not None
                and not any(p["camp_no"] == camp_no for p in peers)
            ):
                peers = [
                    *peers,
                    {
                        "camp_no": camp_no,
                        "organization_id": organization_id,
                        "industry_key": industry_key,
                        "avg_score": this_avg,
                    },
                ]

            rank: int | None = None
            industry_rank: int | None = None
            total_camps = len(peers)
            total_industry_camps = 0

            if peers:
                sorted_city = sorted(peers, key=lambda x: x["avg_score"])
                for idx, entry in enumerate(sorted_city, start=1):
                    if entry["camp_no"] == camp_no:
                        rank = idx
                        break

                if industry_key:
                    industry_peers = [
                        e for e in peers if e.get("industry_key") == industry_key
                    ]
                    total_industry_camps = len(industry_peers)
                    sorted_industry = sorted(industry_peers, key=lambda x: x["avg_score"])
                    for idx, entry in enumerate(sorted_industry, start=1):
                        if entry["camp_no"] == camp_no:
                            industry_rank = idx
                            break

            data[city] = {
                "rank": rank,
                "total_camps": total_camps,
                "industry_rank": industry_rank,
                "total_industry_camps": total_industry_camps,
            }

        return build_ranking(data)

