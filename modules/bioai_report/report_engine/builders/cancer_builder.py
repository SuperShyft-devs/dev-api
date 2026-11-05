"""Build cancer education and screening blocks from stored assessment JSON.

Predicted disease scores and measured markers stay separate. Nothing is
invented when a field is absent from the source payload.
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from typing import Any

from modules.bioai_report.report_engine.knowledge_base.loader import KnowledgeBaseStore
from modules.bioai_report.report_engine.models.assessment import AssessmentPayload
from modules.bioai_report.report_engine.models.knowledge_base import CancerEducation
from modules.bioai_report.report_engine.models.report import (
    CancerDefinition,
    CancerInsightBlock,
    CancerKeyInsights,
    CancerMarkerSnapshot,
    CancerResultCard,
    CancerSnapshotHighlight,
    DetailedCancerScreening,
    IndividualCancerInformation,
)
from modules.bioai_report.report_engine.utils.score_bands import score_to_risk_band

logger = logging.getLogger(__name__)

MALE_CANCER_ORDER: tuple[str, ...] = (
    "pancreatic_cancer",
    "colorectal_cancer",
    "liver_cancer",
    "prostate_cancer",
)
FEMALE_CANCER_ORDER: tuple[str, ...] = (
    "pancreatic_cancer",
    "colorectal_cancer",
    "liver_cancer",
    "breast_cancer",
    "ovarian_cancer",
)
ALL_CANCER_ORDER: tuple[str, ...] = (
    "pancreatic_cancer",
    "colorectal_cancer",
    "liver_cancer",
    "prostate_cancer",
    "breast_cancer",
    "ovarian_cancer",
)

CANCER_INTRODUCTION = (
    "Cancer occurs when cells in the body grow and divide uncontrollably, "
    "sometimes forming abnormal growths called tumours."
)

_GENDER_NOTE = (
    "Gender was not available on the assessment or profile, so gender-based "
    "cancer filtering could not be completed. Every reliably matched cancer "
    "result is included."
)

# Normalized tokens → cancer code. PCOS and NAFLD are intentionally absent.
_ALIAS_TO_CODE: dict[str, str] = {}


def _register(code: str, aliases: tuple[str, ...]) -> None:
    for alias in aliases:
        _ALIAS_TO_CODE[_norm_token(alias)] = code


def _norm_token(value: str) -> str:
    text = value.strip().lower().replace("-", " ").replace("_", " ")
    text = text.replace("prostrate", "prostate")
    text = re.sub(r"[^a-z0-9.+ ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


_register(
    "pancreatic_cancer",
    (
        "pancreatic cancer",
        "pancreatic",
        "pancreas cancer",
        "ca 19-9",
        "ca19-9",
        "ca199",
        "ca 19.9",
        "ca-19.9",
    ),
)
_register(
    "colorectal_cancer",
    (
        "colorectal cancer",
        "colorectal",
        "colon cancer",
        "rectal cancer",
        "bowel cancer",
        "cea",
    ),
)
_register(
    "liver_cancer",
    (
        "liver cancer",
        "hepatocellular carcinoma",
        "hepatocellular",
        "afp",
        "alpha fetoprotein",
        "alpha-fetoprotein",
    ),
)
_register(
    "prostate_cancer",
    ("prostate cancer", "prostate", "prostrate", "prostrate cancer", "psa"),
)
_register(
    "breast_cancer",
    ("breast cancer", "breast", "ca 15-3", "ca15-3", "ca153", "ca 15.3"),
)
_register(
    "ovarian_cancer",
    ("ovarian cancer", "ovarian", "ovary cancer", "ca 125", "ca125", "ca-125"),
)

_MARKER_LIST_KEYS = {
    "biomarkers",
    "markers",
    "lab_results",
    "laboratory_results",
    "tumour_markers",
    "tumor_markers",
    "parameters",
    "observations",
}

_PREDICTED_RISK_TO_STATUS = {
    "healthy": "optimal",
    "increased_risk": "marginal",
    "high_risk": "critical",
    "very_high_risk": "critical",
}


@dataclass
class _MarkerHit:
    cancer_code: str
    marker_name: str | None
    value: float | int | str | None
    unit: str | None
    reference_range: str | None
    range_flag: str
    source_classification: str | None


@dataclass
class _RiskHit:
    cancer_code: str
    name: str | None
    score: float | int
    source_classification: str | None


@dataclass
class _CancerBucket:
    markers: list[_MarkerHit] = field(default_factory=list)
    risks: list[_RiskHit] = field(default_factory=list)


def _unwrap(raw: dict[str, Any]) -> dict[str, Any]:
    if "diseases" in raw or "metabolic_score" in raw:
        return raw
    nested = raw.get("data")
    if isinstance(nested, dict):
        return nested
    return raw


def _match_cancer(*labels: Any) -> str | None:
    for label in labels:
        if not isinstance(label, str) or not label.strip():
            continue
        token = _norm_token(label)
        if token in _ALIAS_TO_CODE:
            return _ALIAS_TO_CODE[token]
        # "prostate cancer risk" style names: require the alias as a whole phrase.
        for alias, code in _ALIAS_TO_CODE.items():
            if alias and re.search(rf"(^| ){re.escape(alias)}($| )", token):
                return code
    return None


def _as_number(value: Any) -> float | int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if not math.isfinite(number):
            return None
        return int(number) if number.is_integer() else number
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
        if not math.isfinite(number):
            return None
        return int(number) if number.is_integer() else number
    return None


def _reference_text(entry: dict[str, Any]) -> str | None:
    for key in ("reference_range", "ref_range", "normal_range", "range"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            low = value.get("low", value.get("min"))
            high = value.get("high", value.get("max"))
            if low is not None and high is not None:
                return f"{low}-{high}"
    low = entry.get("reference_low", entry.get("min"))
    high = entry.get("reference_high", entry.get("max"))
    if low is not None and high is not None and not isinstance(low, dict):
        return f"{low}-{high}"
    return None


def _bounds(reference: str | None) -> tuple[float, float] | None:
    if not reference:
        return None
    match = re.search(
        r"(-?\d+(?:\.\d+)?)\s*(?:-|–|to)\s*(-?\d+(?:\.\d+)?)",
        reference.replace(",", ""),
    )
    if not match:
        return None
    low = float(match.group(1))
    high = float(match.group(2))
    if low > high:
        return None
    return low, high


def _range_flag(value: Any, reference: str | None, classification: str | None) -> str:
    number = _as_number(value)
    bounds = _bounds(reference)
    if number is not None and bounds is not None:
        low, high = bounds
        return "in_range" if low <= float(number) <= high else "out_of_range"
    if classification:
        token = _norm_token(classification)
        if token in {"normal", "within range", "in range", "optimal"}:
            return "in_range"
        if token in {"high", "low", "abnormal", "out of range", "elevated"}:
            return "out_of_range"
    return "unknown"


def _iter_marker_entries(node: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(current: Any) -> None:
        if isinstance(current, dict):
            for key, value in current.items():
                if key in _MARKER_LIST_KEYS and isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            found.append(item)
                else:
                    walk(value)
        elif isinstance(current, list):
            for item in current:
                walk(item)

    walk(node)
    return found


def _marker_from_entry(entry: dict[str, Any]) -> _MarkerHit | None:
    labels = [
        entry.get("name"),
        entry.get("marker_name"),
        entry.get("marker"),
        entry.get("code"),
        entry.get("test_name"),
        entry.get("analyte"),
    ]
    cancer_code = _match_cancer(*labels)
    if cancer_code is None:
        return None
    value = None
    for key in ("value", "result", "result_value"):
        if key in entry and entry[key] is not None:
            raw_value = entry[key]
            value = _as_number(raw_value)
            if value is None and isinstance(raw_value, str) and raw_value.strip():
                value = raw_value.strip()
            break
    if value is None:
        return None
    unit = entry.get("unit") or entry.get("units")
    unit_text = unit.strip() if isinstance(unit, str) and unit.strip() else None
    classification = None
    for key in ("status", "flag", "classification", "range_status"):
        raw = entry.get(key)
        if isinstance(raw, str) and raw.strip():
            classification = raw.strip()
            break
    name = next(
        (item.strip() for item in labels if isinstance(item, str) and item.strip()),
        None,
    )
    reference = _reference_text(entry)
    return _MarkerHit(
        cancer_code=cancer_code,
        marker_name=name,
        value=value,
        unit=unit_text,
        reference_range=reference,
        range_flag=_range_flag(value, reference, classification),
        source_classification=classification,
    )


def _collect(
    payload: AssessmentPayload,
    blood_markers: list[dict[str, Any]] | None = None,
) -> dict[str, _CancerBucket]:
    buckets: dict[str, _CancerBucket] = {}
    raw = _unwrap(payload.raw) if isinstance(payload.raw, dict) else {}
    diseases = raw.get("diseases")
    if not isinstance(diseases, list):
        diseases = []

    seen_risk: set[str] = set()
    for entry in diseases:
        if not isinstance(entry, dict):
            continue
        code = _match_cancer(entry.get("code"), entry.get("name"))
        if code is None or code in seen_risk:
            continue
        score = _as_number(entry.get("risk_score_scaled"))
        if score is None:
            continue
        seen_risk.add(code)
        buckets.setdefault(code, _CancerBucket()).risks.append(
            _RiskHit(
                cancer_code=code,
                name=entry.get("name") if isinstance(entry.get("name"), str) else None,
                score=score,
                source_classification=(
                    entry.get("risk_status").strip()
                    if isinstance(entry.get("risk_status"), str) and entry.get("risk_status").strip()
                    else None
                ),
            )
        )

    seen_markers: set[tuple[str, str | None, str]] = set()
    marker_entries = list(_iter_marker_entries(raw))
    marker_entries.extend(blood_markers or [])
    for entry in marker_entries:
        hit = _marker_from_entry(entry)
        if hit is None:
            continue
        key = (hit.cancer_code, hit.marker_name, str(hit.value))
        if key in seen_markers:
            continue
        seen_markers.add(key)
        buckets.setdefault(hit.cancer_code, _CancerBucket()).markers.append(hit)
    return buckets


def _display_order(gender: str | None) -> tuple[str, ...]:
    if gender == "male":
        return MALE_CANCER_ORDER
    if gender == "female":
        return FEMALE_CANCER_ORDER
    return ALL_CANCER_ORDER


def _ordered_codes(buckets: dict[str, _CancerBucket], gender: str | None) -> list[str]:
    preferred = _display_order(gender)
    ordered = [code for code in preferred if code in buckets]
    if gender in {"male", "female"}:
        extras = [code for code in ALL_CANCER_ORDER if code in buckets and code not in ordered]
        ordered.extend(extras)
    return ordered


def _education_section(education: CancerEducation) -> IndividualCancerInformation:
    return IndividualCancerInformation(
        cancer_code=education.cancer_code,
        cancer_name=education.cancer_name,
        section_title=education.section_title,
        definition=CancerDefinition(
            heading=education.definition.heading,
            content=education.definition.content,
        ),
        actionable_insights=CancerInsightBlock(
            heading=education.actionable_insights.heading,
            items=list(education.actionable_insights.items),
        ),
        influencing_factors=CancerInsightBlock(
            heading=education.influencing_factors.heading,
            items=list(education.influencing_factors.items),
        ),
    )


def _choose_marker(markers: list[_MarkerHit]) -> _MarkerHit | None:
    if not markers:
        return None
    out_of_range = [item for item in markers if item.range_flag == "out_of_range"]
    return out_of_range[0] if out_of_range else markers[0]


def _predicted_status(score: float | int) -> str:
    return _PREDICTED_RISK_TO_STATUS.get(score_to_risk_band(score), "unknown")


def _interpretation(
    *,
    result_type: str,
    range_flag: str,
    display_status: str,
) -> str:
    if result_type == "biomarker":
        if range_flag == "out_of_range":
            return (
                "This recorded value falls outside the supplied reference range. "
                "An out-of-range marker does not diagnose cancer and should be discussed with a doctor."
            )
        if range_flag == "in_range":
            return (
                "This recorded value sits inside the supplied reference range. "
                "A result inside the range does not rule out cancer."
            )
        return (
            "A usable reference range was not available, so this marker cannot be "
            "classified as inside or outside range."
        )
    if result_type == "predicted_risk":
        if display_status == "optimal":
            return (
                "This is a predicted risk score, not a tumour-marker test or a diagnosis. "
                "A lower score does not mean cancer has been ruled out."
            )
        return (
            "This predicted risk score sits above the lower-risk band. "
            "It is not a cancer diagnosis and should be discussed with a doctor."
        )
    return "This result could not be classified as a measured marker or a predicted risk score."


def _card(code: str, bucket: _CancerBucket, education: CancerEducation) -> CancerResultCard:
    marker = _choose_marker(bucket.markers)
    risk = bucket.risks[0] if bucket.risks else None
    if marker is not None:
        result_type = "biomarker"
        display_status = "optimal" if marker.range_flag == "in_range" else "unknown"
        return CancerResultCard(
            cancer_code=code,
            cancer_name=education.cancer_name,
            result_type=result_type,
            marker_name=marker.marker_name,
            value=marker.value,
            unit=marker.unit,
            reference_range=marker.reference_range,
            predicted_risk=risk.score if risk is not None else None,
            source_classification=marker.source_classification,
            range_flag=marker.range_flag,
            display_status=display_status,
            interpretation=_interpretation(
                result_type=result_type,
                range_flag=marker.range_flag,
                display_status=display_status,
            ),
        )
    if risk is not None:
        display_status = _predicted_status(risk.score)
        return CancerResultCard(
            cancer_code=code,
            cancer_name=education.cancer_name,
            result_type="predicted_risk",
            predicted_risk=risk.score,
            source_classification=risk.source_classification,
            range_flag="unknown",
            display_status=display_status,
            interpretation=_interpretation(
                result_type="predicted_risk",
                range_flag="unknown",
                display_status=display_status,
            ),
        )
    return CancerResultCard(
        cancer_code=code,
        cancer_name=education.cancer_name,
        result_type="other",
        interpretation=_interpretation(
            result_type="other",
            range_flag="unknown",
            display_status="unknown",
        ),
    )


def _snapshot(cards: list[CancerResultCard]) -> CancerMarkerSnapshot:
    measured = [card for card in cards if card.result_type == "biomarker"]
    in_range = [card for card in measured if card.range_flag == "in_range"]
    out_of_range = [card for card in measured if card.range_flag == "out_of_range"]
    unknown = [card for card in measured if card.range_flag == "unknown"]
    highlights = [
        CancerSnapshotHighlight(
            cancer_code=card.cancer_code,
            cancer_name=card.cancer_name,
            marker_name=card.marker_name,
            value=card.value,
            unit=card.unit,
            reference_range=card.reference_range,
            range_flag=card.range_flag,
        )
        for card in out_of_range
    ]
    if not measured:
        summary = (
            "No measured cancer markers were available. "
            "Predicted risk scores are not counted as blood markers."
            if any(card.result_type == "predicted_risk" for card in cards)
            else "No measured cancer markers were available."
        )
    else:
        summary = (
            f"{len(measured)} measured cancer marker"
            f"{'' if len(measured) == 1 else 's'}: "
            f"{len(in_range)} inside the recorded range, "
            f"{len(out_of_range)} outside it, "
            f"and {len(unknown)} without a usable range."
        )
    return CancerMarkerSnapshot(
        total_markers=len(measured),
        markers_in_range=len(in_range),
        markers_out_of_range=len(out_of_range),
        markers_unknown=len(unknown),
        summary=summary,
        highlighted_markers=highlights,
    )


def _key_insights(cards: list[CancerResultCard], snapshot: CancerMarkerSnapshot) -> str:
    sentences: list[str] = []
    predicted = [card for card in cards if card.result_type == "predicted_risk" or card.predicted_risk is not None]
    measured = [card for card in cards if card.result_type == "biomarker"]
    if snapshot.highlighted_markers:
        names = ", ".join(
            item.marker_name or item.cancer_name for item in snapshot.highlighted_markers
        )
        sentences.append(
            f"{names} is outside the recorded reference range."
            if len(snapshot.highlighted_markers) == 1
            else f"{names} are outside the recorded reference range."
        )
        sentences.append(
            "An out-of-range marker does not diagnose cancer and should be discussed with a doctor."
        )
    elif measured and snapshot.markers_unknown == snapshot.total_markers:
        sentences.append(
            "Measured cancer markers are present, but a reference range was not available to classify them."
        )
    elif measured:
        sentences.append(
            "Recorded cancer markers fall inside the supplied reference ranges. "
            "Results inside range do not mean a person is cancer-free."
        )
    if any(card.result_type == "predicted_risk" for card in cards) and not measured:
        sentences.append(
            "These cancer entries are predicted risk scores, not measured tumour-marker tests, and they are not a diagnosis."
        )
    elif measured and predicted and any(card.predicted_risk is not None and card.result_type == "biomarker" for card in cards):
        if len(sentences) < 2:
            sentences.append(
                "Predicted risk scores are shown separately from measured markers and do not by themselves diagnose cancer."
            )
    return " ".join(sentences[:2])


def build_cancer_sections(
    payload: AssessmentPayload,
    *,
    kb_store: KnowledgeBaseStore,
    blood_markers: list[dict[str, Any]] | None = None,
) -> tuple[list[IndividualCancerInformation], DetailedCancerScreening]:
    """Return individual cancer pages, then the consolidated screening block."""
    gender = payload.gender or payload.sex
    buckets = _collect(payload, blood_markers)
    codes = _ordered_codes(buckets, gender)

    information: list[IndividualCancerInformation] = []
    cards: list[CancerResultCard] = []
    for code in codes:
        if not kb_store.has_cancer(code):
            logger.warning("Skipping cancer '%s': education file unavailable", code)
            continue
        try:
            education = kb_store.get_cancer(code)
        except Exception as exc:
            logger.warning("Skipping cancer '%s': %s", code, exc)
            continue
        information.append(_education_section(education))
        cards.append(_card(code, buckets[code], education))

    snapshot = _snapshot(cards)
    note = _GENDER_NOTE if gender not in {"male", "female"} and codes else None
    detailed = DetailedCancerScreening(
        title="Detailed Cancer Screening",
        introduction=CancerDefinition(
            heading="What is cancer?",
            content=CANCER_INTRODUCTION,
        ),
        snapshot=snapshot,
        cancer_results=cards,
        key_insights=CancerKeyInsights(heading="Key Insights", content=_key_insights(cards, snapshot)),
        gender_filter_note=note,
    )
    return information, detailed
