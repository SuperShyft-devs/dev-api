"""Knowledge-base domain models (loaded from disease JSON files)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

RiskBandName = Literal["healthy", "increased_risk", "high_risk", "very_high_risk"]


class ScoreBandContent(BaseModel):
    """Content for a single 5-point score slab inside a disease KB."""

    model_config = ConfigDict(extra="ignore")

    score_range: str
    risk_interpretation: str = ""
    lifestyle_tips: list[str] = Field(default_factory=list)
    diet_recommendations: list[str] = Field(default_factory=list)
    foods_to_include: list[str] = Field(default_factory=list)
    foods_to_avoid: list[str] = Field(default_factory=list)
    exercise_recommendations: list[str] = Field(default_factory=list)
    monitoring_recommendations: list[str] = Field(default_factory=list)
    clinical_warning: str = ""
    positive_takeaway: str = ""


class RiskBandContent(BaseModel):
    """A coarse risk band containing nested 5-point score slabs."""

    model_config = ConfigDict(extra="ignore")

    range: str
    score_bands: dict[str, ScoreBandContent] = Field(default_factory=dict)


class DiseaseKnowledgeBase(BaseModel):
    """Full knowledge base for one disease (never exposed wholesale to the frontend)."""

    model_config = ConfigDict(extra="ignore")

    knowledge_base_version: str = "1.0"
    disease_id: str
    display_name: str
    overview: str = ""
    risk_bands: dict[str, RiskBandContent] = Field(default_factory=dict)

    def get_score_band(self, slab_key: str) -> ScoreBandContent | None:
        """Look up a score slab by key (e.g. '66_70') across all risk bands."""
        for risk_band in self.risk_bands.values():
            band = risk_band.score_bands.get(slab_key)
            if band is not None:
                return band
        return None
