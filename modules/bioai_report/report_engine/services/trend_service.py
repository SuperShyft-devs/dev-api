"""Build historical Bio-AI disease-score series for a user.

Uses the existing assessment fetch + normalizer + score-band helpers.
Does not assemble a full BioReport (no KB slabs, nutrition, or summaries).
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.assessments.repository import AssessmentsRepository
from modules.bioai_report.report_engine.builders.disease_builder import _resolve_risk_label
from modules.bioai_report.report_engine.models.assessment import AssessmentPayload
from modules.bioai_report.report_engine.models.trends import (
    BioAITrendAssessment,
    BioAITrendPoint,
    BioAITrendResponse,
    BioAITrendsByDisease,
)
from modules.bioai_report.report_engine.services.assessment_service import AssessmentFetchService
from modules.bioai_report.report_engine.utils.disease_codes import normalize_disease_code
from modules.bioai_report.report_engine.utils.patient_enrichment import normalize_gender_label
from modules.bioai_report.report_engine.utils.score_bands import (
    clamp_score,
    risk_band_range,
    score_to_risk_band,
)
from modules.users.repository import UsersRepository

logger = logging.getLogger(__name__)

# Canonical KB disease ids used by the Bio-AI engine (not display names).
TREND_DISEASE_IDS: tuple[str, ...] = (
    "metabolic_syndrome",
    "dyslipidemia",
    "pcos",
    "oxidative_stress",
    "nafld",
    "hypertension",
    "obesity",
    "thyroid_health",
    "type2_diabetes",
    "cardiac_health",
)

# Metsights Basic / Pro only. FitPrint (7) and face-scan packages have no disease scores.
_BIOAI_TREND_TYPE_CODES = frozenset({"1", "2"})


def _is_bioai_trend_package(package: AssessmentPackage | None) -> bool:
    if package is None:
        return False
    type_code = str(getattr(package, "assessment_type_code", None) or "").strip()
    return type_code in _BIOAI_TREND_TYPE_CODES


def _date_only(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:10]


def _included_in_cutoff(
    *,
    point_date: str | None,
    instance_id: int,
    through_date: str | None,
    through_instance_id: int | None,
) -> bool:
    """Keep history on or before the requested report, including that report.

    Primary bound is ``assessment_date`` (``YYYY-MM-DD``), the same value the
    trend series uses. Same-day instances are not dropped by date alone.
    When ``through_instance_id`` is set, instance id is the secondary bound so
    a later same-day instance is excluded — consistent with sort by
    ``(assessment_date, assessment_instance_id)``.
    """
    if through_date is None:
        return True
    if point_date is None:
        return False
    if point_date < through_date:
        return True
    if point_date > through_date:
        return False
    if through_instance_id is None:
        return True
    return int(instance_id) <= int(through_instance_id)


def _instance_fallback_date(instance: AssessmentInstance) -> str | None:
    for attr in ("completed_at", "assigned_at"):
        value = getattr(instance, attr, None)
        if isinstance(value, datetime):
            return value.date().isoformat()
        if value is not None:
            return _date_only(str(value))
    return None


def _is_male_label(*values: object) -> bool:
    """True only when existing sex/gender data clearly resolves to male."""
    for value in values:
        if normalize_gender_label(value) == "male":
            return True
    return False


def empty_trend_response(user_id: int) -> BioAITrendResponse:
    return BioAITrendResponse(
        user_id=user_id,
        assessment_count=0,
        trend_available=False,
        assessments=[],
        trends=BioAITrendsByDisease(),
    )


class BioAITrendService:
    """Assemble per-disease historical scores from completed Bio-AI assessments."""

    def __init__(
        self,
        *,
        assessment_service: AssessmentFetchService,
        assessments_repository: AssessmentsRepository | None = None,
        users_repository: UsersRepository | None = None,
    ) -> None:
        self._assessment_service = assessment_service
        self._assessments = assessments_repository or AssessmentsRepository()
        self._users = users_repository or UsersRepository()

    async def get_trends_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        through_date: str | None = None,
        through_instance_id: int | None = None,
        patient_gender: str | None = None,
    ) -> BioAITrendResponse:
        user = await self._users.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        rows = await self._assessments.list_completed_instances_for_user(db, user_id=user_id)
        if not rows:
            return empty_trend_response(user_id)

        assessments: list[BioAITrendAssessment] = []
        series: dict[str, list[BioAITrendPoint]] = {key: [] for key in TREND_DISEASE_IDS}
        payload_genders: list[object] = []

        for instance, package in rows:
            if not _is_bioai_trend_package(package):
                continue
            point_date, scores_by_disease, payload_gender = await self._scores_for_instance(
                instance=instance,
                package=package,
            )
            if payload_gender is not None:
                payload_genders.append(payload_gender)
            instance_id = int(instance.assessment_instance_id)
            if not _included_in_cutoff(
                point_date=point_date,
                instance_id=instance_id,
                through_date=through_date,
                through_instance_id=through_instance_id,
            ):
                continue
            assessments.append(
                BioAITrendAssessment(
                    assessment_instance_id=instance_id,
                    assessment_date=point_date,
                )
            )
            for disease_id in TREND_DISEASE_IDS:
                entry = scores_by_disease.get(disease_id)
                if entry is None:
                    series[disease_id].append(
                        BioAITrendPoint(
                            date=point_date,
                            score=None,
                            risk_status=None,
                            risk_band=None,
                            assessment_instance_id=instance_id,
                        )
                    )
                    continue
                score, assessment_risk = entry
                band = score_to_risk_band(score)
                series[disease_id].append(
                    BioAITrendPoint(
                        date=point_date,
                        score=score,
                        risk_status=_resolve_risk_label(
                            score=score, assessment_risk=assessment_risk
                        ),
                        risk_band=risk_band_range(band),
                        assessment_instance_id=instance_id,
                    )
                )

        ordered = sorted(
            assessments,
            key=lambda item: (
                item.assessment_date or "9999-99-99",
                item.assessment_instance_id,
            ),
        )
        # Keep trend series in the same chronological order as ``ordered``.
        order_ids = [item.assessment_instance_id for item in ordered]
        for disease_id in TREND_DISEASE_IDS:
            by_id = {p.assessment_instance_id: p for p in series[disease_id]}
            series[disease_id] = [by_id[i] for i in order_ids if i in by_id]

        count = len(ordered)
        is_male = _is_male_label(
            patient_gender,
            getattr(user, "gender", None),
            getattr(user, "sex", None),
            *payload_genders,
        )
        if is_male:
            series["pcos"] = []
        return BioAITrendResponse(
            user_id=user_id,
            assessment_count=count,
            trend_available=count >= 2,
            assessments=ordered,
            trends=BioAITrendsByDisease.model_validate(series),
        )

    async def _scores_for_instance(
        self,
        *,
        instance: AssessmentInstance,
        package: AssessmentPackage | None,
    ) -> tuple[str | None, dict[str, tuple[int, str | None]], str | None]:
        record_id = (getattr(instance, "metsights_record_id", None) or "").strip()
        fallback_date = _instance_fallback_date(instance)
        if not record_id:
            return fallback_date, {}, None

        assessment_type_code = (
            getattr(package, "assessment_type_code", None) if package is not None else None
        )
        try:
            payload: AssessmentPayload = await self._assessment_service.fetch(
                record_id=record_id,
                assessment_type_code=assessment_type_code,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Bio-AI trends: skipping scores for assessment_instance_id=%s record_id=%s (%s: %s)",
                instance.assessment_instance_id,
                record_id,
                type(exc).__name__,
                exc,
            )
            return fallback_date, {}, None

        point_date = _date_only(payload.assessment_date) or fallback_date
        payload_gender = payload.gender or payload.sex
        scores: dict[str, tuple[int, str | None]] = {}
        for disease in payload.diseases:
            disease_id = normalize_disease_code(disease.code)
            if disease_id is None or disease_id not in TREND_DISEASE_IDS:
                continue
            if disease.risk_score_scaled is None:
                continue
            scores[disease_id] = (
                clamp_score(disease.risk_score_scaled),
                disease.risk_status,
            )
        # Overall metabolic risk is on metabolic_score, not a metabolic_syndrome disease row.
        if "metabolic_syndrome" not in scores and payload.metabolic_score is not None:
            scores["metabolic_syndrome"] = (
                clamp_score(payload.metabolic_score),
                payload.metabolic_health_status,
            )
        return point_date, scores, payload_gender

    async def embed_for_assessment_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        report_payload: dict[str, object],
    ) -> dict[str, object]:
        """Cutoff trends for the requested report. Never raises into report generation."""
        try:
            instance = await self._assessments.get_instance_by_id(
                db,
                assessment_instance_id=assessment_instance_id,
            )
        except Exception:
            logger.exception(
                "Bio-AI report trends: instance lookup failed for assessment_instance_id=%s",
                assessment_instance_id,
            )
            return empty_trend_response(0).to_report_field()

        if instance is None:
            return empty_trend_response(0).to_report_field()

        user_id = int(instance.user_id)
        patient = report_payload.get("patient") if isinstance(report_payload.get("patient"), dict) else {}
        summary = (
            report_payload.get("executive_summary")
            if isinstance(report_payload.get("executive_summary"), dict)
            else {}
        )
        cutoff = _date_only(patient.get("assessment_date") if isinstance(patient, dict) else None)
        if cutoff is None and isinstance(summary, dict):
            cutoff = _date_only(summary.get("assessment_date"))
        if cutoff is None:
            cutoff = _instance_fallback_date(instance)

        gender_hint = None
        if isinstance(patient, dict):
            gender_hint = patient.get("gender") or patient.get("sex")
        if gender_hint is None and isinstance(summary, dict):
            summary_patient = summary.get("patient")
            if isinstance(summary_patient, dict):
                gender_hint = summary_patient.get("gender") or summary_patient.get("sex")

        try:
            trend = await self.get_trends_for_user(
                db,
                user_id=user_id,
                through_date=cutoff,
                through_instance_id=int(instance.assessment_instance_id),
                patient_gender=str(gender_hint) if gender_hint is not None else None,
            )
        except AppError as exc:
            if exc.error_code == "USER_NOT_FOUND":
                return empty_trend_response(user_id).to_report_field()
            logger.warning(
                "Bio-AI report trends failed for assessment_instance_id=%s: %s",
                assessment_instance_id,
                exc,
            )
            return empty_trend_response(user_id).to_report_field()
        except Exception:
            logger.exception(
                "Bio-AI report trends failed for assessment_instance_id=%s",
                assessment_instance_id,
            )
            return empty_trend_response(user_id).to_report_field()

        return trend.to_report_field()
