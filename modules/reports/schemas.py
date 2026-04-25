"""Reports module schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class BloodParameterTestInReportResponse(BaseModel):
    test_id: int
    test_name: str
    parameter_key: str | None = None
    unit: str | None = None
    value: float | None = None
    lower_range: float | None = None
    higher_range: float | None = None


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


class DiseaseDetailResponse(BaseModel):
    code: str
    name: str
    meaning: str | None = None
    unit: str | None = None
    risk_score_scaled: int
    lifestyle_contribution: int | None
    disease_percentile: int | None
    lower_range_male: float | None = None
    higher_range_male: float | None = None
    lower_range_female: float | None = None
    higher_range_female: float | None = None
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


class HealthSpanFitnessDetail(BaseModel):
    blood_pressure: str | None = None
    basal_metabolic_rate: FitPrintParameter | None = None
    waist: str | None = None
    estimated_body_fat: FitPrintParameter | None = None


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
