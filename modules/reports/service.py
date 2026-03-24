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
from modules.metsights.service import MetsightsService
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState
from modules.reports.repository import ReportsRepository


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
        audit_service: AuditService | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ):
        self._repository = repository
        self._assessments_repository = assessments_repository
        self._metsights_service = metsights_service
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
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Any:
        assessment_row = await self._assessments_repository.get_instance_for_user(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if assessment_row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, _package = assessment_row
        existing_report = await self._repository.get_individual_report_by_assessment(
            db,
            assessment_instance_id=assessment_id,
        )

        if existing_report is not None and existing_report.blood_parameters is not None:
            return existing_report.blood_parameters

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
                metsights_output=None,
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

        return blood_parameters

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
                                metsights_output=None,
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
