"""BioReport output contract consumed by the frontend renderer.

Nested disease sections are render-ready PDF blocks. Knowledge-base field
names are never exposed to the frontend.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from modules.bioai_report.report_engine.models.allergy import (
    AllergyAvailability,
    AllergyBreakdown,
    AllergyDetailedInsights,
    AllergyOverview,
)


class PatientInfo(BaseModel):
    """Patient demographics and metabolic snapshot for the report."""

    model_config = ConfigDict(extra="ignore", ser_json_inf_nan="null")

    record_id: str | None = None
    name: str | None = None
    age: float | int | None = None
    sex: str | None = None
    gender: str | None = None
    date_of_birth: str | None = None
    height: float | int | None = None
    weight: float | int | None = None
    bmi: float | int | None = None
    profile_id: str | None = None
    user_id: int | None = None
    metabolic_age: float | int | None = None
    metabolic_score: float | int | None = None
    metabolic_health_status: str | None = None
    assessment_code: str | None = None
    assessment_date: str | None = None

    @field_validator("date_of_birth", "assessment_date", mode="before")
    @classmethod
    def _coerce_date_strings(cls, value: Any, info: ValidationInfo) -> str | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, datetime):
            if info.field_name == "date_of_birth":
                return value.date().isoformat()
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, (dict, list, tuple, set, bytes)):
            return None
        text = str(value).strip()
        return text or None


class DiseaseHighlight(BaseModel):
    """Compact disease reference used in the executive summary."""

    model_config = ConfigDict(extra="ignore")

    disease_id: str
    title: str
    score: int
    risk: str
    band: str


class ExecutiveSummary(BaseModel):
    """Front-matter summary assembled before disease pages."""

    model_config = ConfigDict(extra="ignore")

    patient: PatientInfo
    assessment_date: str | None = None
    metabolic_score: float | int | None = None
    metabolic_age: float | int | None = None
    overall_status: str | None = None
    top_disease_risks: list[DiseaseHighlight] = Field(default_factory=list)
    top_health_strengths: list[DiseaseHighlight] = Field(default_factory=list)
    actionable_insights: list[str] = Field(default_factory=list)


class DiseaseCurrentStatus(BaseModel):
    """Score / risk block rendered at the top of a disease page."""

    model_config = ConfigDict(extra="ignore", ser_json_inf_nan="null")

    score: int
    risk: str
    band: str
    percentile: float | int | None = None
    healthy_percentile: float | int | None = None
    lifestyle_contribution: float | int | None = None
    interpretation: str = ""
    clinical_warning: str = ""


class DiseaseLifestyle(BaseModel):
    """Lifestyle tips and exercise recommendations for a disease page."""

    model_config = ConfigDict(extra="ignore")

    tips: list[str] = Field(default_factory=list)
    exercise: list[str] = Field(default_factory=list)


class DiseaseNutrition(BaseModel):
    """Nutrition block for a disease page."""

    model_config = ConfigDict(extra="ignore")

    recommendations: list[str] = Field(default_factory=list)
    foods_to_include: list[str] = Field(default_factory=list)
    foods_to_avoid: list[str] = Field(default_factory=list)


class DiseaseMonitoring(BaseModel):
    """Monitoring recommendations for a disease page."""

    model_config = ConfigDict(extra="ignore")

    recommendations: list[str] = Field(default_factory=list)


class DiseaseResultFactors(BaseModel):
    """Score-selected factors explaining what may be affecting disease results."""

    model_config = ConfigDict(extra="ignore")

    title: str = "What could be affecting your results?"
    factors: list[str] = Field(default_factory=list)


class DiseaseSection(BaseModel):
    """Render-ready disease page — nested blocks match PDF sections."""

    model_config = ConfigDict(extra="ignore")

    disease_id: str
    title: str
    overview: str
    current_status: DiseaseCurrentStatus
    lifestyle: DiseaseLifestyle
    nutrition: DiseaseNutrition
    monitoring: DiseaseMonitoring
    positive_takeaway: str = ""
    result_factors: DiseaseResultFactors

    @property
    def score(self) -> int:
        """Convenience accessor used for sorting and executive-summary highlights."""
        return self.current_status.score

    @property
    def risk(self) -> str:
        return self.current_status.risk

    @property
    def band(self) -> str:
        return self.current_status.band


class CancerDefinition(BaseModel):
    model_config = ConfigDict(extra="ignore")

    heading: str
    content: str


class CancerInsightBlock(BaseModel):
    model_config = ConfigDict(extra="ignore")

    heading: str
    items: list[str] = Field(default_factory=list)


class IndividualCancerInformation(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cancer_code: str
    cancer_name: str
    section_title: str
    definition: CancerDefinition
    actionable_insights: CancerInsightBlock
    influencing_factors: CancerInsightBlock


class CancerSnapshotHighlight(BaseModel):
    model_config = ConfigDict(extra="ignore", ser_json_inf_nan="null")

    cancer_code: str
    cancer_name: str
    marker_name: str | None = None
    value: float | int | str | None = None
    unit: str | None = None
    reference_range: str | None = None
    range_flag: str = "unknown"


class CancerMarkerSnapshot(BaseModel):
    model_config = ConfigDict(extra="ignore")

    total_markers: int = 0
    markers_in_range: int = 0
    markers_out_of_range: int = 0
    markers_unknown: int = 0
    summary: str = ""
    highlighted_markers: list[CancerSnapshotHighlight] = Field(default_factory=list)


class CancerResultCard(BaseModel):
    model_config = ConfigDict(extra="ignore", ser_json_inf_nan="null")

    cancer_code: str
    cancer_name: str
    result_type: str
    marker_name: str | None = None
    value: float | int | str | None = None
    unit: str | None = None
    reference_range: str | None = None
    predicted_risk: float | int | str | None = None
    source_classification: str | None = None
    range_flag: str = "unknown"
    display_status: str = "unknown"
    interpretation: str = ""


class CancerKeyInsights(BaseModel):
    model_config = ConfigDict(extra="ignore")

    heading: str = "Key Insights"
    content: str = ""


class DetailedCancerScreening(BaseModel):
    model_config = ConfigDict(extra="ignore")

    title: str = "Detailed Cancer Screening"
    introduction: CancerDefinition
    snapshot: CancerMarkerSnapshot
    cancer_results: list[CancerResultCard] = Field(default_factory=list)
    key_insights: CancerKeyInsights
    status_legend: list[str] = Field(
        default_factory=lambda: ["optimal", "marginal", "critical", "unknown"]
    )
    gender_filter_note: str | None = None


class ReportMetadata(BaseModel):
    """Engine provenance for the assembled report."""

    model_config = ConfigDict(extra="ignore")

    record_id: str
    engine_version: str
    kb_version: str | None = None
    template_version: str
    generated_at: str | None = None
    disease_count: int = 0
    source: str = "metsights"


class BioReport(BaseModel):
    """Final JSON contract between the content engine and the frontend."""

    model_config = ConfigDict(extra="ignore", ser_json_inf_nan="null")

    patient: PatientInfo
    executive_summary: ExecutiveSummary
    disease_sections: list[DiseaseSection] = Field(default_factory=list)
    individual_cancer_information: list[IndividualCancerInformation] = Field(
        default_factory=list
    )
    detailed_cancer_screening: DetailedCancerScreening | None = None
    cancer_results_available: bool | None = None
    allergy_results_available: bool | None = None
    allergy_availability: AllergyAvailability | None = None
    allergy_overview: AllergyOverview | None = None
    allergy_detailed_insights: AllergyDetailedInsights | None = None
    allergy_breakdown: AllergyBreakdown | None = None
    report_metadata: ReportMetadata

    def to_dict(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        for key in (
            "detailed_cancer_screening",
            "cancer_results_available",
            "allergy_results_available",
            "allergy_availability",
            "allergy_overview",
            "allergy_detailed_insights",
            "allergy_breakdown",
        ):
            if payload.get(key) is None:
                payload.pop(key, None)
        if payload.get("cancer_results_available") is False:
            payload.pop("individual_cancer_information", None)
            payload.pop("detailed_cancer_screening", None)
        return payload
