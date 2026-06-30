"""Business logic for camp report init and delete."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

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
    build_overall_risk_score,
    build_participation_by_age,
    build_positive_wins,
)
from modules.assessments.repository import AssessmentsRepository
from modules.diagnostics.repository import DiagnosticsRepository
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.models import CampReport
from modules.reports.service import BLOOD_DATA_UNAVAILABLE_ERROR_CODES, ReportsService


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
    ) -> tuple[list[dict], int]:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        rows = await self._repository.list_participants_by_camp_no(
            db,
            camp_no=camp_no,
            page=page,
            limit=limit,
        )
        total = await self._repository.count_participants_by_camp_no(db, camp_no=camp_no)
        return [self._camp_participant_to_dict(row) for row in rows], total

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

    async def _compute_positive_wins_payload(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
    ) -> dict:
        contexts = await self._repository.list_enrolled_assessment_contexts(
            db,
            camp_no=camp_no,
            department=department,
        )
        participant_habits: list[list[dict[str, str | None]]] = []
        participant_profiles: list[list[str]] = []
        participant_low_risk: list[list[dict[str, Any]]] = []
        for ctx in contexts:
            low_risk_items = await self._reports_service.compute_low_risk_for_instance(
                db,
                assessment_instance=ctx.assessment_instance,
                package=ctx.package,
                individual_report=ctx.individual_report,
            )
            participant_low_risk.append(
                [
                    {
                        "code": item.code,
                        "name": item.name,
                        "risk_status": item.risk_status,
                        "risk_score_scaled": item.risk_score_scaled,
                    }
                    for item in low_risk_items
                ]
            )
            try:
                habits, profiles = await self._reports_service.compute_healthy_habits_and_profiles_for_instance(
                    db,
                    assessment_instance=ctx.assessment_instance,
                    package=ctx.package,
                    engagement=ctx.engagement,
                    individual_report=ctx.individual_report,
                    user_gender=ctx.user_gender,
                )
            except AppError as exc:
                if exc.error_code not in BLOOD_DATA_UNAVAILABLE_ERROR_CODES:
                    raise
                habits, profiles = [], []
            participant_habits.append(
                [{"habit_key": h.habit_key, "habit_label": h.habit_label} for h in habits]
            )
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
    ) -> dict:
        contexts = await self._repository.list_fitprint_assessment_contexts(
            db,
            camp_no=camp_no,
            department=department,
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
                    lookup, _ = await self._reports_service._build_questionnaire_lookup(
                        db,
                        source_assessment_instance_ids=source_ids,
                    )
                    nutrition_payload = self._reports_service._build_nutrition_api_payload(
                        lookup, user_gender=ctx.user_gender
                    )
                    nutrition_response = await self._reports_service._call_nutrition_api(nutrition_payload)
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

    async def _compute_blood_and_lab_intelligence_payload(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
    ) -> dict:
        participants = await self._repository.list_blood_parameters_by_gender(
            db,
            camp_no=camp_no,
            department=department,
        )

        group_tests: list[tuple[str, list]] = []
        for group_key in self._BLOOD_INTELLIGENCE_GROUP_KEYS:
            group = await self._diagnostics_repository.get_group_by_group_key(db, group_key=group_key)
            if group is None:
                continue
            tests = await self._diagnostics_repository.get_parameters_for_group(db, group_id=group.group_id)
            group_tests.append((group_key, tests))

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
            group_stats[group_key] = test_stats

        return build_blood_and_lab_intelligence(group_stats)

    @staticmethod
    def _extract_test_value_and_range(
        blood_params: dict[str, Any],
        test,
        gender: str | None,
    ) -> tuple[float | None, float | None, float | None]:
        """Extract value and range for a single test from a participant's blood_parameters."""
        is_provider = isinstance(blood_params.get("digital_data"), list)

        value: float | None = None
        lower_range: float | None = None
        higher_range: float | None = None

        if is_provider:
            healthians_pid = test.healthians_parameter_id
            if healthians_pid is None:
                return None, None, None
            for entry in blood_params["digital_data"]:
                entry_pid = entry.get("parameter_id")
                if entry_pid is not None and str(entry_pid) == str(healthians_pid):
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
        else:
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

        if section_key == "distribution_by_oxidative_stress":
            scores = await self._repository.list_oxidative_stress_scores(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_distribution_by_oxidative_stress(scores)

        if section_key == "distribution_by_physical_activity_frequency":
            rows = await self._repository.list_physical_activity_frequency_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_distribution_by_physical_activity_frequency(rows)

        if section_key == "distribution_by_sleeping_hours":
            rows = await self._repository.list_sleeping_hours_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_distribution_by_sleeping_hours(rows)

        if section_key == "distribution_by_gender_by_metabolic_syndrome":
            rows = await self._repository.list_health_reports_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_distribution_by_gender_by_metabolic_syndrome(rows)

        if section_key == "positive_wins":
            return await self._compute_positive_wins_payload(
                db,
                camp_no=camp_no,
                department=department,
            )

        if section_key == "company_average_scores":
            return await self._compute_company_average_scores_payload(
                db,
                camp_no=camp_no,
                department=department,
            )

        if section_key == "blood_and_lab_intelligence":
            return await self._compute_blood_and_lab_intelligence_payload(
                db,
                camp_no=camp_no,
                department=department,
            )

        raise AppError(
            status_code=400,
            error_code="SECTION_NOT_IMPLEMENTED",
            message="Report section is not implemented",
        )
