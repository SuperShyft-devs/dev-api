"""Controlled mapping from existing SuperShyft report services to HealthContext."""

from __future__ import annotations

import re
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.service import AssessmentsService
from modules.reports.service import ReportsService
from modules.qura.schemas import ContextMeta, HealthContext, Marker, QueryPlan, RiskScore


class HealthContextBuilder(Protocol):
    async def build(
        self,
        *,
        user_id: int,
        plan: QueryPlan,
        user_gender: str | None = None,
        user_first_name: str = "",
        user_last_name: str = "",
        ip_address: str = "unknown",
        user_agent: str = "unknown",
    ) -> HealthContext:
        """Build a PHI-minimized context from existing platform services."""


class MockContextBuilder:
    """Safe temporary builder that retrieves no health data and persists nothing."""

    async def build(self, *, user_id: int, plan: QueryPlan, **_: Any) -> HealthContext:
        del user_id  # User identity must never be represented in HealthContext.
        requested: list[str] = []
        if plan.required_markers:
            requested.append("requested_markers_unavailable")
        if plan.include_current_report_markers:
            requested.append("current_report_markers_unavailable")
        if plan.include_bio_ai_risks:
            requested.append("bio_ai_risks_unavailable")
        return HealthContext(
            metadata=ContextMeta(
                source="mock",
                data_available=False,
                requested_data=_requested_data(plan),
                missing_data=[*plan.missing_data, *requested],
            )
        )


class ContextBuilder:
    """Read existing report services only; no provider or normalization access exists here."""

    def __init__(
        self,
        *,
        db: AsyncSession,
        reports_service: ReportsService,
        assessments_service: AssessmentsService,
    ) -> None:
        self._db = db
        self._reports_service = reports_service
        self._assessments_service = assessments_service

    async def build(
        self,
        *,
        user_id: int,
        plan: QueryPlan,
        user_gender: str | None = None,
        user_first_name: str = "",
        user_last_name: str = "",
        ip_address: str = "unknown",
        user_agent: str = "unknown",
    ) -> HealthContext:
        requested = _requested_data(plan)
        assessment_id = await self._latest_assessment_id(user_id=user_id)
        if assessment_id is None:
            return _unavailable_context(requested, [*plan.missing_data, "latest_assessment_unavailable"])

        markers: list[Marker] = []
        risks: list[RiskScore] = []
        missing = list(plan.missing_data)
        try:
            if plan.required_markers or plan.include_current_report_markers:
                groups = await self._reports_service.get_blood_parameters_for_user(
                    self._db,
                    assessment_id=assessment_id,
                    user_id=user_id,
                    user_gender=user_gender,
                    user_first_name=user_first_name,
                    user_last_name=user_last_name,
                    load_from="provider",
                    reload=0,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint="/qura/chat",
                )
                markers = _map_markers(groups, required_markers=plan.required_markers)
                if plan.required_markers:
                    found_codes = {marker.code for marker in markers}
                    missing.extend(
                        f"marker:{code}"
                        for code in plan.required_markers
                        if _normalize_code(code) not in found_codes
                    )
                elif not markers:
                    missing.append("current_report_markers_unavailable")

            if plan.include_bio_ai_risks:
                risk_response = await self._reports_service.get_risk_analysis_for_user(
                    self._db,
                    assessment_id=assessment_id,
                    user_id=user_id,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint="/qura/chat",
                )
                risks = _map_risks(risk_response)
                if not risks:
                    missing.append("bio_ai_risks_unavailable")
        except AppError:
            # Existing service errors are deliberately reduced to an explicit availability state.
            return _unavailable_context(requested, [*missing, "health_data_unavailable"])
        except Exception:
            return _unavailable_context(requested, [*missing, "health_data_unavailable"])

        found = [f"marker:{marker.code}" for marker in markers] + [f"risk:{risk.code}" for risk in risks]
        return HealthContext(
            selected_markers=markers,
            risk_scores=risks,
            metadata=ContextMeta(
                source="latest_health_report",
                data_available=bool(found),
                requested_data=requested,
                found_data=found,
                missing_data=list(dict.fromkeys(missing)),
            ),
        )

    async def _latest_assessment_id(self, *, user_id: int) -> int | None:
        try:
            rows, _total = await self._assessments_service.list_my_assessments(
                self._db,
                user_id=user_id,
                page=1,
                limit=1,
            )
        except Exception:
            return None
        if not rows:
            return None
        instance, _package = rows[0]
        value = getattr(instance, "assessment_instance_id", None)
        return int(value) if value is not None else None


def _requested_data(plan: QueryPlan) -> list[str]:
    requested = [f"marker:{_normalize_code(code)}" for code in plan.required_markers]
    if plan.include_current_report_markers:
        requested.append("current_report_markers")
    if plan.include_bio_ai_risks:
        requested.append("bio_ai_risks")
    return requested


def _unavailable_context(requested: list[str], missing: list[str]) -> HealthContext:
    return HealthContext(
        metadata=ContextMeta(
            source="latest_health_report",
            data_available=False,
            requested_data=requested,
            missing_data=list(dict.fromkeys(missing)),
        )
    )


def _normalize_code(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _map_markers(groups: Any, *, required_markers: list[str]) -> list[Marker]:
    requested = {_normalize_code(code) for code in required_markers}
    mapped: list[Marker] = []
    for group in groups if isinstance(groups, list) else []:
        group_name = getattr(group, "group_name", None)
        for test in getattr(group, "tests", []):
            raw_code = getattr(test, "parameter_key", None) or getattr(test, "test_name", "")
            code = _normalize_code(raw_code)
            if not code or (requested and code not in requested):
                continue
            mapped.append(
                Marker(
                    code=code,
                    name=str(getattr(test, "test_name", "") or code),
                    value=getattr(test, "value", None),
                    unit=getattr(test, "unit", None),
                    ref_low=getattr(test, "lower_range", None),
                    ref_high=getattr(test, "higher_range", None),
                    flag="unavailable",
                    category=str(group_name) if group_name else None,
                )
            )
    return mapped


def _map_risks(response: Any) -> list[RiskScore]:
    return [
        RiskScore(
            code=_normalize_code(getattr(item, "code", "")),
            name=str(getattr(item, "name", "") or ""),
            score=getattr(item, "risk_score_scaled", None),
            status=None,
        )
        for item in getattr(response, "diseases", [])
        if _normalize_code(getattr(item, "code", ""))
    ]
