"""Normalized allergy assessment and the three page payloads."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

ResultStatus = Literal[
    "positive",
    "negative_per_lab",
    "indeterminate",
    "invalid",
    "missing",
]
MatchStatus = Literal["exact_name", "provider_alias", "parameter_id", "unresolved"]


class AllergyResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    lab_test_name: str
    allergen_id: str | None = None
    match_status: MatchStatus = "unresolved"
    common_name: str | None = None
    source_group: str | None = None
    source_group_name: str | None = None
    canonical_category: str | None = None
    scientific_name: str | None = None
    display_name: str | None = None
    raw_value: str | None = None
    comparator: Literal["<", ">", "<=", ">="] | None = None
    applied_rule_id: str | None = None
    interpretation_confidence: str | None = None
    numeric_value: Decimal | None = None
    unit: str | None = None
    lab_class: str | None = None
    lab_flag: str | None = None
    reference_low: Decimal | None = None
    reference_high: Decimal | None = None
    provider_parameter_id: str | None = None
    result_status: ResultStatus
    review_status: str = "pending"
    provenance: str = "blood_report_raw.digital_data"


class TotalIgeResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    lab_test_name: str
    allergen_id: str = "ige_total"
    raw_value: str | None = None
    comparator: Literal["<", ">", "<=", ">="] | None = None
    applied_rule_id: str | None = None
    interpretation_confidence: str | None = None
    numeric_value: Decimal | None = None
    unit: str | None = None
    reference_low: Decimal | None = None
    reference_high: Decimal | None = None
    reference_text: str | None = None
    band: str | None = None
    status_label: str | None = None
    result_status: str = "uninterpretable"
    narratives_approved: bool = False


class AllergyAvailability(BaseModel):
    """Whether this booking ordered allergy profiles and whether values arrived."""

    model_config = ConfigDict(extra="ignore")

    parameters_source: str = "participant_blood_bookings.blood_parameters"
    allergy_tests_booked: bool = False
    allergy_results_available: bool = False
    total_ige_booked: bool = False
    total_ige_reported: bool = False
    booked_source_groups: list[str] = Field(default_factory=list)
    expected_test_count: int = 0
    reported_test_count: int = 0
    missing_test_count: int = 0
    panel_complete: bool = False
    processing_status: Literal[
        "not_booked",
        "awaiting_results",
        "partial",
        "ready",
        "needs_review",
    ] = "not_booked"
    validation_warnings: list[str] = Field(default_factory=list)


class AllergyAssessment(BaseModel):
    model_config = ConfigDict(extra="ignore")

    assessment_instance_id: int | None = None
    blood_booking_id: int | None = None
    booking_reference: str | None = None
    collection_date: str | None = None
    laboratory_source: str | None = None
    resolution_status: Literal["resolved", "unresolved", "not_found"] = "resolved"
    resolution_reason: str | None = None
    panel_present: bool = False
    data_completeness: Literal["complete", "partial", "absent"] = "absent"
    availability: AllergyAvailability | None = None
    total_ige: TotalIgeResult | None = None
    results: list[AllergyResult] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)
    clinical_review_status: str = "pending"
    narratives_approved: bool = False
    qualified_clinical_approval: str = "pending"
    kb_version: str | None = None


class AllergyOverview(BaseModel):
    model_config = ConfigDict(extra="ignore")

    what_is_ige: str | None = None
    narrative_withheld: bool = False
    withheld_reason: str | None = None
    total_ige: TotalIgeResult | None = None
    total_ige_interpretation: str | None = None
    total_ige_status_label: str | None = None
    specific_tested: int = 0
    detected_count: int = 0
    non_detected_count: int = 0
    indeterminate_count: int = 0
    missing_count: int = 0
    negative_count_unverified: bool = False
    detected_allergens: list[AllergyResult] = Field(default_factory=list)
    class_explanation: str | None = None
    sensitization_note: str | None = None


class AllergyInsight(BaseModel):
    model_config = ConfigDict(extra="ignore")

    lab_test_name: str
    allergen_id: str | None = None
    match_status: MatchStatus = "unresolved"
    raw_value: str | None = None
    unit: str | None = None
    result_status: ResultStatus
    common_name: str | None = None
    display_name: str | None = None
    scientific_name: str | None = None
    lab_class: str | None = None
    definition: str | None = None
    card_summary: str | None = None
    summary: str | None = None
    exposure_sources: list[str] = Field(default_factory=list)
    interpretation_insight: str | None = None
    caution: str | None = None
    possible_symptoms: list[str] = Field(default_factory=list)
    symptom_note: str = (
        "Possible symptoms are general education for this allergen. "
        "They are not a statement that this patient has those symptoms."
    )
    narrative_withheld: bool = True


class RecommendationCard(BaseModel):
    model_config = ConfigDict(extra="ignore")

    recommendation_id: str
    title: str
    description: str


class InterpretiveNote(BaseModel):
    model_config = ConfigDict(extra="ignore")

    label: str
    text: str


class AllergyDetailedInsights(BaseModel):
    model_config = ConfigDict(extra="ignore")

    insights: list[AllergyInsight] = Field(default_factory=list)
    recommendations: list[RecommendationCard] = Field(default_factory=list)
    interpretive_notes: list[InterpretiveNote] = Field(default_factory=list)
    screening_explanation: str | None = None
    narrative_withheld: bool = False
    withheld_reason: str | None = None


class AllergyBreakdownGroup(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_group: str
    source_groups: list[str] = Field(default_factory=list)
    title: str
    summary: str | None = None
    badge: str | None = None
    status_line: str | None = None
    findings_line: str | None = None
    laboratory_group_name: str | None = None
    total_tests: int = 0
    detected_count: int = 0
    non_detected_count: int = 0
    indeterminate_count: int = 0
    missing_count: int = 0
    category_status: str
    results: list[AllergyResult] = Field(default_factory=list)
    highlighted: list[AllergyResult] = Field(default_factory=list)
    continuation_required: bool = False


class AllergyBreakdown(BaseModel):
    model_config = ConfigDict(extra="ignore")

    groups: list[AllergyBreakdownGroup] = Field(default_factory=list)
    no_positives_note_title: str | None = None
    no_positives_note: str | None = None


class AllergySections(BaseModel):
    model_config = ConfigDict(extra="ignore")

    assessment: AllergyAssessment
    overview: AllergyOverview
    detailed_insights: AllergyDetailedInsights
    breakdown: AllergyBreakdown

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
