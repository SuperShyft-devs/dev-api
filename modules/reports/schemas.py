"""Reports module schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from common.validation import (
    OptionalCityStateCountry,
    OptionalSafeDisplayName,
    OptionalSafeText,
    OptionalSlugKey,
    SafeDisplayName,
    SlugKey,
    validate_nested_strings,
)


class BloodParameterTestInReportResponse(BaseModel):
    test_id: int
    parameter_type: str = "test"
    test_name: str
    parameter_key: str | None = None
    unit: str | None = None
    value: float | None = None
    machine_value: float | None = None
    lower_range: float | None = None
    higher_range: float | None = None
    provider_test_name: str | None = None


class BloodParameterGroupInReportResponse(BaseModel):
    group_name: str
    test_count: int
    tests: list[BloodParameterTestInReportResponse]


class BloodParameterTrendPoint(BaseModel):
    date: str
    value: float
    engagement_id: int


class BloodParameterTrendResponse(BaseModel):
    parameter: str
    unit: str | None = None
    data_points: list[BloodParameterTrendPoint]


class DiseaseOverview(BaseModel):
    code: str
    name: str
    risk_status: str
    risk_score_scaled: int


class RiskAnalysisItem(BaseModel):
    code: str
    name: str
    risk_status: str
    risk_score_scaled: int
    healthy_percentile: int


class HealthyHabitItem(BaseModel):
    habit_key: str | None = None
    habit_label: str


class PositiveWins(BaseModel):
    low_risk: list[DiseaseOverview]
    healthy_habits: list[HealthyHabitItem] = Field(default_factory=list)
    healthy_profiles: list[str] = Field(default_factory=list)


class OverviewReportResponse(BaseModel):
    assessment_id: int
    metabolic_age: float | None
    positive_wins: PositiveWins
    risk_analysis: list[RiskAnalysisItem]


class DiseaseListItem(BaseModel):
    code: str
    name: str
    risk_score_scaled: int


class RiskAnalysisListResponse(BaseModel):
    assessment_id: int
    metabolic_score: float | None
    diseases: list[DiseaseListItem]


class BioAiPdfResponse(BaseModel):
    assessment_id: int
    report_url: str


class DiagnosticPdfResponse(BaseModel):
    assessment_id: int
    report_url: str


class DiseaseDetailResponse(BaseModel):
    code: str
    name: str
    meaning: str | None = None
    unit: str | None = None
    risk_score_scaled: int
    lifestyle_contribution: int | None
    disease_percentile: int | None
    low_risk_lower_range_male: float | None = None
    low_risk_higher_range_male: float | None = None
    moderate_risk_lower_range_male: float | None = None
    moderate_risk_higher_range_male: float | None = None
    high_risk_lower_range_male: float | None = None
    high_risk_higher_range_male: float | None = None
    low_risk_lower_range_female: float | None = None
    low_risk_higher_range_female: float | None = None
    moderate_risk_lower_range_female: float | None = None
    moderate_risk_higher_range_female: float | None = None
    high_risk_lower_range_female: float | None = None
    high_risk_higher_range_female: float | None = None
    causes_when_high: str | None = None
    causes_when_low: str | None = None
    effects_when_high: str | None = None
    effects_when_low: str | None = None
    what_to_do_when_low: str | None = None
    what_to_do_when_high: str | None = None


# --- Health Span Index ---


class HealthSpanIndexRequest(BaseModel):
    source_assessment_instance_ids: list[int]
    include_details: bool = False


class NutrientDetail(BaseModel):
    estimated_low: float | None = None
    estimated_high: float | None = None
    ideal_low: float | None = None
    ideal_high: float | None = None
    status: str | None = None


class WaterDetail(BaseModel):
    estimated_litres: float | None = None
    ideal_low_litres: float | None = None
    ideal_high_litres: float | None = None
    status: str | None = None


class FitPrintParameterRange(BaseModel):
    min: float | None = None
    max: float | None = None


class FitPrintParameter(BaseModel):
    parameter: str | None = None
    code: str | None = None
    value: float | None = None
    unit: str | None = None
    healthy_range: FitPrintParameterRange | None = None
    status: str | None = None


class IdealRangeDetail(BaseModel):
    low: float | None = None
    high: float | None = None
    unit: str | None = None


class WaistMeasurement(BaseModel):
    value: float | None = None
    unit: Literal["in", "cm"] | None = None


class HealthSpanFitnessDetail(BaseModel):
    systolic_blood_pressure: str | None = None
    diastolic_blood_pressure: str | None = None
    basal_metabolic_rate: FitPrintParameter | None = None
    waist: WaistMeasurement | None = None
    estimated_body_fat: FitPrintParameter | None = None
    ideal_waist: IdealRangeDetail | None = None
    ideal_bmr: IdealRangeDetail | None = None
    ideal_body_fat: IdealRangeDetail | None = None


class HealthSpanNutritionDetail(BaseModel):
    carbs: NutrientDetail | None = None
    fats: NutrientDetail | None = None
    protein: NutrientDetail | None = None
    fibre: NutrientDetail | None = None
    water: WaterDetail | None = None


class HealthSpanLifestyleDetail(BaseModel):
    physical_activity: str | None = None
    smoke: str | None = None
    alcohol: str | None = None
    sleep: str | None = None
    family_history: str | None = None


class HealthSpanIndexResponse(BaseModel):
    lifestyle_score: float | None = None
    nutrition_score: float | None = None
    fitness_score: float | None = None
    fitness: HealthSpanFitnessDetail | None = None
    nutrition: HealthSpanNutritionDetail | None = None
    lifestyle: HealthSpanLifestyleDetail | None = None


class CampReportSectionCreateRequest(BaseModel):
    section: SafeDisplayName
    section_key: SlugKey
    description: OptionalSafeText = None


class CampReportSectionUpdateRequest(BaseModel):
    section: OptionalSafeDisplayName = None
    section_key: OptionalSlugKey = None
    description: OptionalSafeText = None


class CampReportRefreshRequest(BaseModel):
    section: SlugKey


class CampReportSectionPayloadUpdateRequest(BaseModel):
    """Manually overwrite a stored camp report section JSON blob."""

    section: SlugKey
    payload: dict[str, Any]

    @model_validator(mode="after")
    def sanitize_payload(self) -> "CampReportSectionPayloadUpdateRequest":
        validate_nested_strings(self.payload)
        return self


class CampReportEstimateOperationRequest(BaseModel):
    section: SlugKey
    action: SlugKey
    department: OptionalSafeDisplayName = None
    city: OptionalCityStateCountry = None


class CampReportEstimateRequest(BaseModel):
    operations: list[CampReportEstimateOperationRequest] = Field(min_length=1)


class CampReportEstimateOperationResult(BaseModel):
    section: str
    action: str
    department: str | None = None
    city: str | None = None
    participant_count: int
    unit_count: int
    estimated_seconds: int
    allowed: bool


class CampReportEstimateResponse(BaseModel):
    timeout_seconds: int
    operations: list[CampReportEstimateOperationResult]
    total_estimated_seconds: int
    all_allowed: bool


class CampParticipantReports(BaseModel):
    blood_report_generated: bool = False
    blood_report_sent: bool = False
    bio_ai_report_generated: bool = False
    bio_ai_report_sent: bool = False


class CampParticipantResponse(BaseModel):
    engagement_participant_id: int
    engagement_id: int
    user_id: int
    first_name: str | None = None
    last_name: str | None = None
    phone: str | None = None
    email: str | None = None
    gender: str | None = None
    age: int | None = None
    participant_blood_group: str | None = None
    participant_department: str | None = None
    participants_employee_id: str | None = None
    questionnaires: dict[str, bool] = Field(default_factory=dict)
    reports: CampParticipantReports = Field(default_factory=CampParticipantReports)
    consultations: bool = False
