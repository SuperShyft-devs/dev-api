"""Reports business service."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.exceptions import AppError
from db.session import AsyncSessionLocal
from modules.assessments.repository import AssessmentsRepository
from modules.audit.service import AuditService
from modules.diagnostics.service import DiagnosticsService
from modules.metsights.service import MetsightsService
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState
from modules.reports.repository import ReportsRepository
from modules.reports.schemas import (
    BloodParameterGroupInReportResponse,
    BloodParameterTestInReportResponse,
    DiseaseOverview,
    OverviewReportResponse,
    PositiveWins,
    RiskAnalysisItem,
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
    ):
        self._repository = repository
        self._assessments_repository = assessments_repository
        self._metsights_service = metsights_service
        self._diagnostics_service = diagnostics_service
        self._audit_service = audit_service
        self._session_factory = session_factory or AsyncSessionLocal

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

    async def get_overview_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> OverviewReportResponse:
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

        assessment_type_code = (package.assessment_type_code or "").strip()
        if assessment_type_code == "7":
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="FitPrint report overview is not allowed",
            )

        existing_report = await self._repository.get_individual_report_by_assessment(
            db,
            assessment_instance_id=assessment_id,
        )

        if existing_report is not None and existing_report.reports is not None:
            report_data: Any = existing_report.reports
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

        risk_analysis_list.sort(key=lambda x: x.risk_score_scaled, reverse=True)

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
            positive_wins=PositiveWins(low_risk=positive_wins_list),
            risk_analysis=risk_analysis_list,
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
