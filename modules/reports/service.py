"""Reports business service."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.exceptions import AppError
from db.session import AsyncSessionLocal
from modules.assessments.models import AssessmentInstance
from modules.assessments.repository import AssessmentsRepository
from modules.audit.service import AuditService
from modules.diagnostics.service import DiagnosticsService
from modules.metsights.service import MetsightsService
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState
from modules.reports.repository import ReportsRepository
from modules.questionnaire.healthy_habits_service import HealthyHabitsService
from modules.reports.schemas import (
    BloodParameterGroupInReportResponse,
    BloodParameterTestInReportResponse,
    DiseaseDetailResponse,
    DiseaseListItem,
    DiseaseOverview,
    HealthyHabitItem,
    OverviewReportResponse,
    PositiveWins,
    RiskAnalysisItem,
    RiskAnalysisListResponse,
)


_SYNC_IDLE = "idle"
_SYNC_IN_PROGRESS = "in_progress"
_SYNC_FAILED = "failed"


class ReportsService:
    """Business logic for report retrieval and caching."""

    def __init__(
        self,
        repository: ReportsRepository,
        assessments_repository: AssessmentsRepository,
        metsights_service: MetsightsService,
        diagnostics_service: DiagnosticsService,
        audit_service: AuditService | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        healthy_habits_service: HealthyHabitsService | None = None,
    ):
        self._repository = repository
        self._assessments_repository = assessments_repository
        self._metsights_service = metsights_service
        self._diagnostics_service = diagnostics_service
        self._audit_service = audit_service
        self._session_factory = session_factory or AsyncSessionLocal
        self._healthy_habits_service = healthy_habits_service

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def get_blood_parameters_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        user_gender: str | None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Any:
        assessment_row = await self._assessments_repository.get_instance_for_user_with_engagement(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if assessment_row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, _package, engagement = assessment_row
        if engagement is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment is missing engagement context",
            )

        diagnostic_package_id = engagement.diagnostic_package_id
        normalized_gender = (user_gender or "").strip().lower() or None
        if normalized_gender not in {"male", "female"}:
            normalized_gender = None
        existing_report = await self._repository.get_individual_report_by_assessment(
            db,
            assessment_instance_id=assessment_id,
        )

        if existing_report is not None and existing_report.blood_parameters is not None:
            return await self._build_blood_parameter_groups_report(
                db=db,
                blood_parameters=existing_report.blood_parameters,
                diagnostic_package_id=diagnostic_package_id,
                user_gender=normalized_gender,
            )

        record_id = (assessment_instance.metsights_record_id or "").strip()
        if not record_id:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Metsights record id is missing for this assessment",
            )

        blood_parameters = await self._metsights_service.get_blood_parameters(record_id=record_id)

        if existing_report is None:
            report = IndividualHealthReport(
                user_id=assessment_instance.user_id,
                engagement_id=assessment_instance.engagement_id,
                assessment_instance_id=assessment_instance.assessment_instance_id,
                reports=None,
                blood_parameters=blood_parameters,
            )
            await self._repository.create_individual_report(db, report)
        else:
            existing_report.blood_parameters = blood_parameters
            await self._repository.update_individual_report(db, existing_report)

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_BLOOD_PARAMETERS_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return await self._build_blood_parameter_groups_report(
            db=db,
            blood_parameters=blood_parameters,
            diagnostic_package_id=diagnostic_package_id,
            user_gender=normalized_gender,
        )

    @staticmethod
    def _top_healthy_profile_group_names(
        groups: list[BloodParameterGroupInReportResponse],
        *,
        limit: int = 3,
    ) -> list[str]:
        """Names of test groups with the most in-range parameters, up to ``limit``."""
        scored: list[tuple[int, str]] = []
        for group in groups:
            optimal = 0
            for test in group.tests:
                if test.value is None or test.lower_range is None or test.higher_range is None:
                    continue
                if test.lower_range <= test.value <= test.higher_range:
                    optimal += 1
            if optimal > 0:
                scored.append((optimal, group.group_name))
        scored.sort(key=lambda x: (-x[0], x[1]))
        return [name for _, name in scored[:limit]]

    async def _resolve_blood_parameters_for_overview(
        self,
        db: AsyncSession,
        *,
        assessment_instance: AssessmentInstance,
        individual_report: IndividualHealthReport,
    ) -> dict[str, Any]:
        if individual_report.blood_parameters is not None:
            raw = individual_report.blood_parameters
            return raw if isinstance(raw, dict) else {}

        record_id = (assessment_instance.metsights_record_id or "").strip()
        if not record_id:
            return {}

        blood_parameters = await self._metsights_service.get_blood_parameters(record_id=record_id)
        individual_report.blood_parameters = blood_parameters
        await self._repository.update_individual_report(db, individual_report)
        return blood_parameters if isinstance(blood_parameters, dict) else {}

    async def get_overview_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        user_gender: str | None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> OverviewReportResponse:
        row = await self._assessments_repository.get_instance_for_user_with_engagement(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, package, engagement = row
        if package is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package is missing",
            )

        assessment_type_code = (package.assessment_type_code or "").strip()
        if assessment_type_code == "7":
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="FitPrint report overview is not allowed",
            )

        individual_report = await self._repository.get_individual_report_by_assessment(
            db,
            assessment_instance_id=assessment_id,
        )

        if individual_report is not None and individual_report.reports is not None:
            report_data: Any = individual_report.reports
        else:
            record_id = (assessment_instance.metsights_record_id or "").strip()
            if not record_id:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Metsights record id is missing for this assessment",
                )

            report_data = await self._metsights_service.get_report(
                record_id=record_id,
                assessment_type_code=package.assessment_type_code,
            )

            if individual_report is None:
                individual_report = IndividualHealthReport(
                    user_id=assessment_instance.user_id,
                    engagement_id=assessment_instance.engagement_id,
                    assessment_instance_id=assessment_instance.assessment_instance_id,
                    reports=report_data,
                    blood_parameters=None,
                )
                await self._repository.create_individual_report(db, individual_report)
            else:
                individual_report.reports = report_data
                await self._repository.update_individual_report(db, individual_report)

        report_dict = report_data if isinstance(report_data, dict) else {}

        ma = report_dict.get("metabolic_age")
        if isinstance(ma, (int, float)):
            metabolic_age = float(ma)
        else:
            metabolic_age = None
        dis = report_dict.get("diseases", [])
        diseases_raw: list[Any] = dis if isinstance(dis, list) else []

        positive_wins_list: list[DiseaseOverview] = []
        risk_analysis_list: list[RiskAnalysisItem] = []
        for d in diseases_raw:
            if not isinstance(d, dict):
                continue
            code = str(d.get("code") or "")
            name = str(d.get("name") or "")
            rs = d.get("risk_status")
            risk_status = str(rs) if rs is not None else ""
            rsc = d.get("risk_score_scaled")
            try:
                risk_score_scaled = int(rsc) if rsc is not None else 0
            except (TypeError, ValueError):
                risk_score_scaled = 0
            if risk_status == "Healthy":
                positive_wins_list.append(
                    DiseaseOverview(
                        code=code,
                        name=name,
                        risk_status=risk_status,
                        risk_score_scaled=risk_score_scaled,
                    )
                )
            hp = d.get("healthy_percentile")
            try:
                healthy_percentile = int(hp) if hp is not None else 0
            except (TypeError, ValueError):
                healthy_percentile = 0
            risk_analysis_list.append(
                RiskAnalysisItem(
                    code=code,
                    name=name,
                    risk_status=risk_status,
                    risk_score_scaled=risk_score_scaled,
                    healthy_percentile=healthy_percentile,
                )
            )

        positive_wins_list.sort(key=lambda x: (x.risk_score_scaled, x.code))
        positive_wins_list = positive_wins_list[:3]

        risk_analysis_list.sort(key=lambda x: (-x.risk_score_scaled, x.code))
        risk_analysis_list = risk_analysis_list[:3]

        healthy_profiles: list[str] = []
        if (
            engagement is not None
            and engagement.diagnostic_package_id is not None
            and individual_report is not None
        ):
            normalized_gender = (user_gender or "").strip().lower() or None
            if normalized_gender not in {"male", "female"}:
                normalized_gender = None
            blood_raw = await self._resolve_blood_parameters_for_overview(
                db,
                assessment_instance=assessment_instance,
                individual_report=individual_report,
            )
            groups = await self._build_blood_parameter_groups_report(
                db=db,
                blood_parameters=blood_raw,
                diagnostic_package_id=int(engagement.diagnostic_package_id),
                user_gender=normalized_gender,
            )
            healthy_profiles = self._top_healthy_profile_group_names(groups)

        healthy_habits: list[HealthyHabitItem] = []
        if self._healthy_habits_service is not None:
            computed = await self._healthy_habits_service.top_habits_for_assessment(
                db,
                assessment_instance_id=assessment_id,
                package_id=int(assessment_instance.package_id),
                limit=3,
            )
            healthy_habits = [
                HealthyHabitItem(habit_key=h.habit_key, habit_label=h.habit_label) for h in computed
            ]

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_OVERVIEW_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return OverviewReportResponse(
            assessment_id=assessment_id,
            metabolic_age=metabolic_age,
            positive_wins=PositiveWins(
                low_risk=positive_wins_list,
                healthy_habits=healthy_habits,
                healthy_profiles=healthy_profiles,
            ),
            risk_analysis=risk_analysis_list,
        )

    async def _get_or_fetch_report(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
    ) -> Any:
        row = await self._assessments_repository.get_instance_for_user(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, package = row
        if package is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package is missing",
            )

        existing_report = await self._repository.get_individual_report_by_assessment(
            db,
            assessment_instance_id=assessment_id,
        )

        if existing_report is not None and existing_report.reports is not None:
            return existing_report.reports

        record_id = (assessment_instance.metsights_record_id or "").strip()
        if not record_id:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Metsights record id is missing for this assessment",
            )

        report_data = await self._metsights_service.get_report(
            record_id=record_id,
            assessment_type_code=package.assessment_type_code,
        )

        if existing_report is None:
            report = IndividualHealthReport(
                user_id=assessment_instance.user_id,
                engagement_id=assessment_instance.engagement_id,
                assessment_instance_id=assessment_instance.assessment_instance_id,
                reports=report_data,
                blood_parameters=None,
            )
            await self._repository.create_individual_report(db, report)
        else:
            existing_report.reports = report_data
            await self._repository.update_individual_report(db, existing_report)

        return report_data

    async def get_risk_analysis_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> RiskAnalysisListResponse:
        report_data = await self._get_or_fetch_report(
            db,
            assessment_id=assessment_id,
            user_id=user_id,
        )

        report_dict = report_data if isinstance(report_data, dict) else {}
        ms = report_dict.get("metabolic_score")
        if isinstance(ms, (int, float)):
            metabolic_score = float(ms)
        else:
            metabolic_score = None

        raw_diseases = report_dict.get("diseases", [])
        diseases_raw: list[Any] = raw_diseases if isinstance(raw_diseases, list) else []
        diseases: list[DiseaseListItem] = []
        for d in diseases_raw:
            if not isinstance(d, dict):
                continue
            code = str(d.get("code") or "")
            name = str(d.get("name") or "")
            rsc = d.get("risk_score_scaled")
            try:
                risk_score_scaled = int(rsc) if rsc is not None else 0
            except (TypeError, ValueError):
                risk_score_scaled = 0
            diseases.append(
                DiseaseListItem(
                    code=code,
                    name=name,
                    risk_score_scaled=risk_score_scaled,
                )
            )

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_RISK_ANALYSIS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return RiskAnalysisListResponse(
            assessment_id=assessment_id,
            metabolic_score=metabolic_score,
            diseases=diseases,
        )

    async def get_disease_detail_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        disease_code: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiseaseDetailResponse:
        report_data = await self._get_or_fetch_report(
            db,
            assessment_id=assessment_id,
            user_id=user_id,
        )

        report_dict = report_data if isinstance(report_data, dict) else {}
        raw_diseases = report_dict.get("diseases", [])
        diseases_raw: list[Any] = raw_diseases if isinstance(raw_diseases, list) else []

        matched: dict[str, Any] | None = None
        for d in diseases_raw:
            if isinstance(d, dict) and str(d.get("code") or "") == disease_code:
                matched = d
                break

        if matched is None:
            raise AppError(
                status_code=404,
                error_code="DISEASE_NOT_FOUND",
                message=f"Disease '{disease_code}' not found in this report",
            )

        rsc = matched.get("risk_score_scaled")
        try:
            risk_score_scaled = int(rsc) if rsc is not None else 0
        except (TypeError, ValueError):
            risk_score_scaled = 0

        lc = matched.get("lifestyle_contribution")
        if lc is None:
            lifestyle_contribution: int | None = None
        else:
            try:
                lifestyle_contribution = int(lc)
            except (TypeError, ValueError):
                lifestyle_contribution = None

        dp = matched.get("disease_percentile")
        if dp is None:
            disease_percentile: int | None = None
        else:
            try:
                disease_percentile = int(dp)
            except (TypeError, ValueError):
                disease_percentile = None

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_DISEASE_DETAIL",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        hp = await self._diagnostics_service.get_health_parameter_by_parameter_key(
            db, parameter_key=disease_code
        )

        return DiseaseDetailResponse(
            code=str(matched.get("code") or ""),
            name=str(matched.get("name") or ""),
            meaning=hp.meaning if hp is not None else None,
            unit=hp.unit if hp is not None else None,
            risk_score_scaled=risk_score_scaled,
            lifestyle_contribution=lifestyle_contribution,
            disease_percentile=disease_percentile,
            lower_range_male=hp.lower_range_male if hp is not None else None,
            higher_range_male=hp.higher_range_male if hp is not None else None,
            lower_range_female=hp.lower_range_female if hp is not None else None,
            higher_range_female=hp.higher_range_female if hp is not None else None,
            causes_when_high=hp.causes_when_high if hp is not None else None,
            causes_when_low=hp.causes_when_low if hp is not None else None,
            effects_when_high=hp.effects_when_high if hp is not None else None,
            effects_when_low=hp.effects_when_low if hp is not None else None,
            what_to_do_when_low=hp.what_to_do_when_low if hp is not None else None,
            what_to_do_when_high=hp.what_to_do_when_high if hp is not None else None,
        )

    async def _build_blood_parameter_groups_report(
        self,
        *,
        db: AsyncSession,
        blood_parameters: Any,
        diagnostic_package_id: int,
        user_gender: str | None,
    ) -> list[BloodParameterGroupInReportResponse]:
        raw: dict[str, Any] = blood_parameters if isinstance(blood_parameters, dict) else {}

        # Use diagnostic reference data for the scheduled diagnostic package.
        package_tests = await self._diagnostics_service.get_package_tests(db=db, package_id=diagnostic_package_id)

        groups: list[BloodParameterGroupInReportResponse] = []
        for group in package_tests.groups:
            tests: list[BloodParameterTestInReportResponse] = []
            for test in group.tests:
                parameter_key = test.parameter_key

                raw_value: Any = raw.get(parameter_key) if parameter_key else None
                value: float | None = None
                if raw_value is not None:
                    try:
                        value = float(raw_value)
                    except (TypeError, ValueError):
                        value = None

                unit_key = f"{parameter_key}_unit" if parameter_key else None
                raw_unit = raw.get(unit_key) if unit_key else None
                if isinstance(raw_unit, str) and raw_unit.strip():
                    unit: str | None = raw_unit.strip()
                else:
                    unit = test.unit.strip() if isinstance(test.unit, str) else None

                lower_range: float | None = None
                higher_range: float | None = None
                if user_gender == "male":
                    lower_range = float(test.lower_range_male) if test.lower_range_male is not None else None
                    higher_range = float(test.higher_range_male) if test.higher_range_male is not None else None
                elif user_gender == "female":
                    lower_range = float(test.lower_range_female) if test.lower_range_female is not None else None
                    higher_range = float(test.higher_range_female) if test.higher_range_female is not None else None

                tests.append(
                    BloodParameterTestInReportResponse(
                        test_id=test.test_id,
                        test_name=test.test_name,
                        parameter_key=parameter_key,
                        unit=unit,
                        value=value,
                        lower_range=lower_range,
                        higher_range=higher_range,
                    )
                )

            groups.append(
                BloodParameterGroupInReportResponse(
                    group_name=group.group_name,
                    test_count=len(tests),
                    tests=tests,
                )
            )

        return groups

    async def _get_or_create_sync_state(self, db: AsyncSession, *, user_id: int) -> ReportsUserSyncState:
        state = await self._repository.get_user_sync_state(db, user_id=user_id)
        if state is not None:
            return state
        return await self._repository.create_user_sync_state(db, user_id=user_id)

    async def _build_trend_payload(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        parameter_key: str,
    ) -> dict[str, Any]:
        rows = await self._repository.list_individual_reports_for_user_with_assessment(
            db,
            user_id=user_id,
        )

        unit_key = f"{parameter_key}_unit"
        data_points: list[dict[str, Any]] = []
        unit: str | None = None

        for report, assessment in rows:
            blood_parameters = report.blood_parameters
            if not isinstance(blood_parameters, dict):
                continue
            raw_value = blood_parameters.get(parameter_key)
            if raw_value is None:
                continue
            try:
                numeric_value = float(raw_value)
            except (TypeError, ValueError):
                continue

            raw_unit = blood_parameters.get(unit_key)
            if unit is None and isinstance(raw_unit, str):
                unit = raw_unit

            point_date = assessment.completed_at or assessment.assigned_at
            if point_date is None:
                continue
            if isinstance(point_date, date):
                date_value = point_date.isoformat()[:10]
            else:
                date_value = str(point_date)[:10]

            data_points.append(
                {
                    "date": date_value,
                    "value": numeric_value,
                    "engagement_id": int(assessment.engagement_id),
                }
            )
        return {
            "parameter": parameter_key,
            "unit": unit,
            "data_points": data_points,
        }

    async def _refresh_user_blood_parameters(self, *, user_id: int) -> None:
        async with self._session_factory() as db:
            state = await self._get_or_create_sync_state(db, user_id=user_id)
            try:
                if (state.sync_status or _SYNC_IDLE) != _SYNC_IN_PROGRESS:
                    state.sync_status = _SYNC_IN_PROGRESS
                    await self._repository.update_user_sync_state(db, state)
                    await db.commit()
                after_id = int(state.last_synced_assessment_instance_id or 0)
                rows = await self._repository.list_unsynced_assessments_with_record_id(
                    db,
                    user_id=user_id,
                    after_assessment_instance_id=after_id,
                )

                latest_synced_id = after_id
                for assessment in rows:
                    record_id = (assessment.metsights_record_id or "").strip()
                    if not record_id:
                        continue
                    blood_parameters = await self._metsights_service.get_blood_parameters(record_id=record_id)
                    existing_report = await self._repository.get_individual_report_by_assessment(
                        db,
                        assessment_instance_id=assessment.assessment_instance_id,
                    )
                    if existing_report is None:
                        await self._repository.create_individual_report(
                            db,
                            IndividualHealthReport(
                                user_id=assessment.user_id,
                                engagement_id=assessment.engagement_id,
                                assessment_instance_id=assessment.assessment_instance_id,
                                reports=None,
                                blood_parameters=blood_parameters,
                            ),
                        )
                    else:
                        existing_report.blood_parameters = blood_parameters
                        await self._repository.update_individual_report(db, existing_report)
                    latest_synced_id = max(latest_synced_id, int(assessment.assessment_instance_id))

                state.last_synced_assessment_instance_id = latest_synced_id if latest_synced_id > 0 else None
                state.sync_status = _SYNC_IDLE
                state.last_sync_error = None
                state.last_synced_at = datetime.now(timezone.utc)
                await self._repository.update_user_sync_state(db, state)
                await db.commit()
            except Exception as exc:
                await db.rollback()
                failed_state = await self._get_or_create_sync_state(db, user_id=user_id)
                failed_state.sync_status = _SYNC_FAILED
                failed_state.last_sync_error = str(exc)[:1000]
                await self._repository.update_user_sync_state(db, failed_state)
                await db.commit()

    def trigger_user_blood_parameters_refresh(self, *, user_id: int) -> None:
        asyncio.create_task(self._refresh_user_blood_parameters(user_id=user_id))

    async def get_blood_parameter_trends_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        blood_parameter: str,
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        parameter_key = (blood_parameter or "").strip().lower()
        if not parameter_key:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        payload = await self._build_trend_payload(
            db,
            user_id=user_id,
            parameter_key=parameter_key,
        )
        state = await self._get_or_create_sync_state(db, user_id=user_id)
        latest = await self._repository.get_latest_assessment_with_record_id(db, user_id=user_id)
        latest_assessment_id = int(latest.assessment_instance_id) if latest is not None else None
        cursor = int(state.last_synced_assessment_instance_id or 0)
        stale = latest_assessment_id is not None and cursor < latest_assessment_id
        should_trigger = False
        if stale and (state.sync_status or _SYNC_IDLE) != _SYNC_IN_PROGRESS:
            state.sync_status = _SYNC_IN_PROGRESS
            state.last_sync_error = None
            await self._repository.update_user_sync_state(db, state)
            should_trigger = True

        meta = {
            "is_stale": stale,
            "sync_status": state.sync_status,
            "last_synced_at": state.last_synced_at,
            "last_synced_assessment_instance_id": state.last_synced_assessment_instance_id,
            "latest_assessment_instance_id": latest_assessment_id,
        }
        return payload, meta, should_trigger
