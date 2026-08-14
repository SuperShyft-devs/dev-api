"""Insight generation — display helpers, structured narrative, leadership, public API.

Flat merge (no business-logic changes) of:
  - intelligence/insight/display.py
  - intelligence/insight/generate_structured_insight.py
  - intelligence/insight/generate_leadership.py
  - intelligence/compose/compose_insight.py
  - intelligence/generate.py

``format_chart_footer`` / ``format_leadership_cards`` are lazy-imported from
``.formatter`` inside the public generate_* functions to avoid an import cycle.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .analyzer import (
    DashboardInput,
    compose_company_profile,
    rebuild_profile_with_finding,
)
from .knowledge import intervention_phrase, is_disease_metric, medical_frame
from .knowledge import get_default_levers, get_disease_knowledge, get_lifestyle_knowledge
from .knowledge import (
    ELEVATED_SHARE_LEADS,
    disease_interpretation_clauses,
    disease_leadership_takeaways,
    disease_positive_narratives,
    lever_action_variants,
    select_variant,
)
from .models import (
    ChartNarrative,
    CompanyHealthProfile,
    ConfidenceFactors,
    InsightConfidence,
    InsightPlan,
    InsightTone,
    LeadershipTakeawayCard,
    MetricFinding,
    MetricId,
    SectionId,
    StructuredInsight,
    round1,
)
from .reasoning import ReasonOptions, reason_about_section

# ---------------------------------------------------------------------------
# insight/display.py
# ---------------------------------------------------------------------------

# Human labels only — never return snake_case metric ids.
FALLBACK_LABELS: dict[str, str] = {
    "overall_risk": "Overall risk",
    "physical_activity": "Physical activity",
    "sleep": "Sleep",
    "nutrition": "Nutrition",
    "oxidative_stress": "Oxidative stress",
    "metabolic_age": "Metabolic age",
    "bmi_waist": "BMI and waist",
    "positive_wins": "Positive health wins",
    "participation": "Participation",
    "type_2_diabetes": "Type 2 Diabetes",
    "hypertension": "Hypertension",
    "obesity": "Obesity",
    "pcos_pcod": "PCOS/PCOD",
    "nafld": "NAFLD",
    "cardiac_health": "Cardiac health",
    "thyroid_health": "Thyroid health",
    "dyslipidemia": "Dyslipidemia",
    "metabolic_syndrome": "Metabolic syndrome",
}

CLUSTER_LABELS: dict[str, str] = {
    "metabolic": "metabolic",
    "cardiovascular": "cardiovascular",
    "hormonal": "hormonal",
    "lifestyle": "lifestyle",
    "recovery": "recovery",
    "mixed": "mixed",
    "healthy": "healthy",
}

# Patterns that must never reach the UI.
_INTERNAL_ID_PATTERN = re.compile(
    r"\b(overall_risk|physical_activity|oxidative_stress|metabolic_age|bmi_waist|"
    r"positive_wins|type_2_diabetes|pcos_pcod|cardiac_health|thyroid_health|"
    r"metabolic_syndrome|disease_deep_dive|disease_risks|recovery_strain|"
    r"cardio_nutrition|movement_priority|metabolic_cluster|cardio_cluster|"
    r"workforce_resilience|medicalFrameId|metricId|effectId)\b",
    re.IGNORECASE,
)

_SNAKE_CASE_KEY_PATTERN = re.compile(r"\b[a-z]+(?:_[a-z]+)+\s*:\s*")
_BAD_PAREN_JOIN_PATTERN = re.compile(r"\)\.\s+([a-z])")
_MULTI_SPACE_PATTERN = re.compile(r"\s{2,}")
_SPACE_BEFORE_PUNCT_PATTERN = re.compile(r"\s+([,.;:!?])")
_REPEATED_PUNCT_PATTERN = re.compile(r"([.!?]){2,}")


def human_metric_name(metric_id: str) -> str:
    disease = get_disease_knowledge(metric_id)
    if disease is not None:
        return disease.display_name
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None:
        return lifestyle.display_name
    return FALLBACK_LABELS.get(metric_id, "This health indicator")


def human_cluster_name(cluster: str | None) -> str:
    if not cluster:
        return "overall"
    return CLUSTER_LABELS.get(cluster, "overall")


def sanitize_insight_text(text: str) -> str:
    """Strip any leaked internal identifiers from user-facing text."""

    def _replace_id(match: re.Match[str]) -> str:
        key = match.group(0).lower()
        return human_metric_name(key)

    out = _INTERNAL_ID_PATTERN.sub(_replace_id, text)
    # Remove "Label:" prefixes that look like code keys (snake_case before colon)
    out = _SNAKE_CASE_KEY_PATTERN.sub("", out)
    # Fix common join artifacts: "). indicating" -> "), indicating"
    out = _BAD_PAREN_JOIN_PATTERN.sub(r"), \1", out)
    # Fix punct splices from joining already-terminated phrases
    out = re.sub(r"\.\s*,", ".", out)
    out = re.sub(r",\s*\.", ".", out)
    out = re.sub(r"\.\s*\.", ".", out)
    out = _MULTI_SPACE_PATTERN.sub(" ", out)
    out = _SPACE_BEFORE_PUNCT_PATTERN.sub(r"\1", out)
    out = _REPEATED_PUNCT_PATTERN.sub(r"\1", out)
    return out.strip()


def count_words(text: str) -> int:
    return len(text.split())


def limit_words(text: str, max_words: int) -> str:
    """Soft trim to max_words while keeping sentence boundaries when possible."""
    words = text.split()
    if len(words) <= max_words:
        return text.strip()

    truncated = " ".join(words[:max_words])
    last_sentence = re.match(r"^(.+[.!?])\s", truncated)
    if last_sentence and count_words(last_sentence.group(1)) >= int(max_words * 0.6):
        return last_sentence.group(1)
    return truncated if re.search(r"[.!?]$", truncated) else f"{truncated}."


# ---------------------------------------------------------------------------
# insight/generate_structured_insight.py
# ---------------------------------------------------------------------------

CONCERN_MAX_WORDS = 50
POSITIVE_MAX_WORDS = 50
NEUTRAL_MAX_WORDS = 50

INSUFFICIENT_DATA_MESSAGE = (
    "Insufficient data is currently available to generate a meaningful organisational insight."
)


def _ensure_single_ending(text: str, ending: str) -> str:
    """Append ``ending`` once — strip any existing trailing sentence punctuation first."""
    trimmed = (text or "").strip()
    if not trimmed:
        return ""
    bare = re.sub(r"[.!,;:]+$", "", trimmed).strip()
    if not bare:
        return ""
    return f"{bare}{ending}"


# Participial / prepositional openings intended to follow a comma — must stay
# lowercase. Capitalised forms ("Increasing blood-pressure risk…") are full
# sentences and must not be treated as dangling clauses.
_CLAUSE_OPENERS = re.compile(
    r"^(indicating|increasing|impairing|supporting|reflecting|signalling|signaling|"
    r"highlighting|pointing|raising|showing|making|forming|warranting|reinforcing|"
    r"helping|suggesting|contributing|influencing|which |an |a |with )"
)

_CLAUSE_TO_SENTENCE: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^indicating\s+", re.I), "This indicates "),
    (re.compile(r"^suggesting\s+", re.I), "This suggests "),
    (re.compile(r"^highlighting\s+", re.I), "This highlights "),
    (re.compile(r"^reflecting\s+", re.I), "This reflects "),
    (re.compile(r"^pointing to\s+", re.I), "This points to "),
    (re.compile(r"^pointing\s+", re.I), "This points "),
    (re.compile(r"^signalling\s+", re.I), "This signals "),
    (re.compile(r"^signaling\s+", re.I), "This signals "),
    (re.compile(r"^showing\s+", re.I), "This shows "),
    (re.compile(r"^increasing\s+", re.I), "This increases "),
    (re.compile(r"^reinforcing\s+", re.I), "This reinforces "),
    (re.compile(r"^supporting\s+", re.I), "This supports "),
    (re.compile(r"^raising\s+", re.I), "This raises "),
    (re.compile(r"^impairing\s+", re.I), "This impairs "),
    (re.compile(r"^contributing\s+", re.I), "This contributes "),
    (re.compile(r"^influencing\s+", re.I), "This influences "),
    (re.compile(r"^helping\s+", re.I), "This helps "),
    (re.compile(r"^warranting\s+", re.I), "This warrants "),
    (re.compile(r"^making\s+", re.I), "This makes "),
    (re.compile(r"^forming\s+", re.I), "This forms "),
    (re.compile(r"^which may indicate\s+", re.I), "This may indicate "),
    (re.compile(r"^which may signal\s+", re.I), "This may signal "),
    (re.compile(r"^which may highlight\s+", re.I), "This may highlight "),
    (re.compile(r"^which may point\s+", re.I), "This may point "),
    (re.compile(r"^which may increase\s+", re.I), "This may increase "),
    (re.compile(r"^which may raise\s+", re.I), "This may raise "),
    (re.compile(r"^which may show\s+", re.I), "This may show "),
    (re.compile(r"^which may impair\s+", re.I), "This may impair "),
    (re.compile(r"^which\s+", re.I), "This "),
    (re.compile(r"^potentially an\s+", re.I), "This may be an "),
    (re.compile(r"^potentially a\s+", re.I), "This may be a "),
    (re.compile(r"^an\s+", re.I), "This reflects an "),
    (re.compile(r"^a\s+", re.I), "This reflects a "),
    (re.compile(r"^with\s+", re.I), "This comes with "),
]


def _is_clause(text: str) -> bool:
    """True only for lowercase participial/prepositional openings.

    Capitalised openings (``Increasing risk suggests…``) are complete sentences.
    """
    t = (text or "").strip()
    if not t:
        return False
    return bool(_CLAUSE_OPENERS.match(t))


def _promote_clause_to_sentence(text: str) -> str:
    """Turn a dangling clause into a complete executive sentence."""
    t = _strip_trailing_punct((text or "").strip())
    if not t:
        return ""
    if not _is_clause(t):
        return _capitalize(t)
    out = t
    for pattern, replacement in _CLAUSE_TO_SENTENCE:
        if pattern.match(out):
            out = pattern.sub(replacement, out, count=1)
            break
    else:
        out = f"This reflects {out}"
    return _capitalize(out)


def _normalize_narrative_sentence(text: str) -> str:
    """Ensure a field is a complete sentence ending with a single full stop."""
    t = (text or "").strip()
    if not t:
        return ""
    t = _strip_trailing_punct(t)
    if not t:
        return ""
    if _is_clause(t):
        t = _promote_clause_to_sentence(t)
    else:
        t = _capitalize(t)
    return _ensure_sentence(t)


def _join_explanation_parts(primary: str, secondary: str | None = None) -> str:
    """Join why-phrases without creating ``.,`` splices or dangling fragments.

    - Two complete sentences → spaced sentences (``A. B``).
    - Complete sentence + clause → comma attachment (``A, clause``).
    - Clause-only material → promoted to a full sentence.
    """
    parts = [p.strip() for p in (primary, secondary) if p and p.strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        part = parts[0]
        return _promote_clause_to_sentence(part) if _is_clause(part) else _strip_trailing_punct(part)

    first, second = parts[0], parts[1]
    first_bare = _strip_trailing_punct(first)
    second_bare = _strip_trailing_punct(second)
    first_is_clause = _is_clause(first_bare)
    second_is_clause = _is_clause(second_bare)

    if first_is_clause and second_is_clause:
        return _promote_clause_to_sentence(f"{first_bare}, {second_bare}")
    if first_is_clause and not second_is_clause:
        # Prefer the complete sentence; drop the orphan clause rather than splice badly.
        return second_bare
    if not first_is_clause and second_is_clause:
        return f"{first_bare}, {second_bare}"
    # Two complete sentences — never join with a comma.
    return f"{first_bare}. {_capitalize(second_bare)}"


def ensure_structured_field_punctuation(
    observation: str,
    explanation: str,
    recommendation: str,
) -> tuple[str, str, str]:
    """Ensure structured O → E → R fields are complete, well-punctuated sentences.

    Each non-empty field becomes a standalone sentence ending with a single full
    stop. Participial fragments are promoted (``indicating X`` → ``This indicates X``).
    Content wording is preserved aside from that grammatical normalisation.
    """
    return (
        _normalize_narrative_sentence(observation),
        _normalize_narrative_sentence(explanation),
        _normalize_narrative_sentence(recommendation),
    )


def _unique_preserve(items: list | None) -> list:
    """Deduplicate while preserving order."""
    seen: set = set()
    out: list = []
    for item in items or []:
        if item in seen or item is None or item == "":
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_compare(text: str) -> str:
    """Loose comparison key to detect near-duplicate O/E/R sentences."""
    t = re.sub(r"[^a-z0-9\s]", "", (text or "").lower())
    return re.sub(r"\s+", " ", t).strip()


def _shares_action_theme(explanation: str, recommendation: str) -> bool:
    """True when recommendation restates programme advice already in the explanation."""
    expl = _normalize_compare(explanation)
    rec = _normalize_compare(recommendation)
    if not expl or not rec:
        return False
    if expl == rec:
        return True
    themes = (
        "nutrition",
        "movement",
        "physical activity",
        "sleep",
        "recovery",
        "screening",
        "weight",
        "stress",
        "preventive",
    )
    shared = [t for t in themes if t in expl and t in rec]
    action_words = ("priorit", "promote", "strengthen", "focus", "should be", "key focus")
    expl_actionable = any(w in expl for w in action_words)
    return bool(shared) and expl_actionable


_ACTION_LIKE_EXPLANATION = re.compile(
    r"\b(should be|should become|must be|key focus|a priority for|prioritise|prioritize|promote)\b",
    re.IGNORECASE,
)

# Deterministic opener pools — medical clause after the opener is unchanged.
_WHY_STEM_POOLS: dict[str, list[str]] = {
    "indicates": [
        "Elevated findings indicate",
        "The observed pattern indicates",
        "Workforce data indicate",
        "Current results indicate",
    ],
    "suggests": [
        "Available evidence suggests",
        "The distribution suggests",
        "Clinical patterns suggest",
        "These findings suggest",
    ],
    "supports": [
        "The profile supports",
        "Current results support",
        "Healthy bands support",
        "The distribution supports",
    ],
    "highlights": [
        "The pattern highlights",
        "These findings highlight",
        "Workforce data highlight",
        "The distribution highlights",
    ],
    "shows": [
        "The distribution shows",
        "Current data show",
        "Workforce results show",
        "The profile shows",
    ],
    "increases": [
        "The pattern increases",
        "Elevated findings increase",
        "This distribution increases",
        "Sustained elevation increases",
    ],
    "affects": [
        "The pattern affects",
        "Elevated findings affect",
        "Poor sleep patterns affect",
        "The distribution affects",
    ],
    "contributes": [
        "The pattern contributes",
        "Elevated findings contribute",
        "This distribution contributes",
        "Sustained elevation contributes",
    ],
    "influences": [
        "The pattern influences",
        "Dietary findings influence",
        "These results influence",
        "The distribution influences",
    ],
    "reflects": [
        "The pattern reflects",
        "Current findings reflect",
        "Workforce data reflect",
        "The distribution reflects",
    ],
    "identifies": [
        "The pattern identifies",
        "Current findings identify",
        "Workforce data identify",
        "The distribution identifies",
    ],
}

_LEVER_PLAIN_LABELS: dict[str, str] = {
    "metabolic_screening": "metabolic screening",
    "lipid_screening": "lipid screening",
    "bp_screening": "blood-pressure screening",
    "thyroid_screening": "thyroid screening",
    "liver_screening": "liver health monitoring",
    "cardiac_screening": "cardiovascular screening",
    "nutrition_refined_carb": "nutrition",
    "nutrition_heart_healthy": "heart-healthy nutrition",
    "nutrition_sodium": "sodium-aware nutrition",
    "nutrition_whole_food": "whole-food nutrition",
    "movement_programme": "daily movement",
    "weight_management": "weight-management",
    "sleep_health": "sleep hygiene",
    "stress_management": "stress-management",
    "recovery_programme": "recovery",
    "womens_health": "women's-health support",
    "smoking_cessation": "smoking-cessation support",
    "alcohol_moderation": "alcohol-moderation education",
    "maintain_wellness": "preventive wellness monitoring",
    "target_high_risk": "targeted follow-up for elevated-risk groups",
    "scale_preventive_care": "scaled preventive care",
    "clinical_review": "clinical review pathways",
}


def _restyle_why_opening(text: str, plan: InsightPlan) -> str:
    """Rotate 'This indicates…' stems without changing the medical clause."""
    bare = _strip_trailing_punct(text or "")
    if not bare:
        return bare
    match = re.match(
        r"^This\s+(indicates|suggests|supports|highlights|shows|increases|affects|"
        r"contributes|influences|reflects|identifies)\b(.*)$",
        bare,
        re.IGNORECASE,
    )
    if not match:
        return bare
    stem = match.group(1).lower()
    remainder = match.group(2).strip()
    pool = _WHY_STEM_POOLS.get(stem)
    if not pool:
        return bare
    opener = select_variant(
        pool,
        plan.observation.metric_id,
        plan.section_id,
        plan.severity_band,
        _profile_cluster(plan),
        stem,
    )
    if not remainder:
        return opener
    return f"{opener} {remainder}"


def _generate_structured_insight_from_plan(
    plan: InsightPlan,
    *,
    used_phrases: frozenset[str] | None = None,
    used_why_stems: frozenset[str] | None = None,
) -> StructuredInsight:
    """Plan -> StructuredInsight (Observation / Why / Action)."""
    if _is_insufficient_plan(plan):
        return _insufficient_structured(plan)

    observation = sanitize_insight_text(_build_observation(plan))
    explanation = sanitize_insight_text(
        _build_explanation(plan, used_why_stems=used_why_stems)
    )
    render_levers = _select_render_levers(plan)
    recommendation = sanitize_insight_text(
        _build_recommendation(
            plan,
            render_levers=render_levers,
            used_phrases=used_phrases,
            explanation=explanation,
        )
    )

    if plan.confidence.band == "low":
        explanation = _soften_explanation(explanation)
        recommendation = _soften_recommendation(recommendation)

    observation, explanation, recommendation = ensure_structured_field_punctuation(
        observation, explanation, recommendation
    )

    # Final guard: recommendation must not restate explanation.
    if _shares_action_theme(explanation, recommendation):
        alt = sanitize_insight_text(
            _build_recommendation(
                plan,
                render_levers=render_levers,
                used_phrases=frozenset(
                    list(used_phrases or []) + [_strip_trailing_punct(recommendation)]
                ),
                explanation=explanation,
                force_alternate=True,
            )
        )
        _, _, recommendation = ensure_structured_field_punctuation(
            observation, explanation, alt
        )

    return StructuredInsight(
        section_id=plan.section_id,
        tone=plan.tone,
        severity_band=plan.severity_band,
        confidence=plan.confidence,
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
        effect_ids=_unique_preserve(plan.explanation.effect_ids),
        lever_ids=_unique_preserve(render_levers or plan.recommendation.lever_ids),
        related_metrics=[plan.observation.metric_id],
    )


def _profile_cluster(plan: InsightPlan) -> str:
    """Company-profile signal available on the plan (dominant cluster). Used only
    as a deterministic selection key so wording can vary by company without ever
    affecting reasoning."""
    return (plan.explanation.cluster or "mixed").lower()


def _population_noun(facts: dict) -> str:
    """Denominator label for a percentage, taken from existing analyzer facts.

    Does not invent a population. Defaults to employees when both genders (or
    neither) are represented.
    """
    explicit = str(facts.get("population_label") or facts.get("denominator_label") or "").strip()
    if explicit:
        return explicit
    male_has = facts.get("male_has_data")
    female_has = facts.get("female_has_data")
    if male_has is False and female_has is not False:
        return "assessed female employees"
    if female_has is False and male_has is not False:
        return "assessed male employees"
    view = str(facts.get("graph_view") or "").strip().lower()
    if view == "female":
        return "assessed female employees"
    if view == "male":
        return "assessed male employees"
    return "employees"


def _of_population(facts: dict) -> str:
    return f"of {_population_noun(facts)}"


def _few_elevated(
    elevated_value: float,
    elevated: str,
    predicate: str,
    *,
    population: str = "employees",
) -> str:
    """Lead a positive observation with the elevated share, without claiming a majority."""
    if elevated_value <= 0:
        return f"No {population} {predicate}"
    return f"Only {elevated}% of {population} {predicate}"


def _share_lead(plan: InsightPlan) -> str:
    """Deterministic synonym for the 'a substantial elevated group' quantifier.

    The exact share always follows in the sentence, so precision is preserved —
    only the qualitative framing rotates. The population noun follows the
    analyzer denominator when it is not the full workforce.
    """
    lead = select_variant(
        ELEVATED_SHARE_LEADS,
        plan.observation.metric_id,
        plan.severity_band,
        plan.section_id,
        _profile_cluster(plan),
    )
    population = _population_noun(plan.observation.facts)
    if population and population != "employees":
        lead = lead.replace("employees", population)
    return lead


def _oxidative_observation(plan: InsightPlan, *, elevated: str, healthy: str, majority_healthy: bool) -> str:
    """Present oxidative shares without claiming Low is dominant when it is not."""
    f = plan.observation.facts
    dominant = str(f.get("dominant_label", "") or "")
    dominant_pct = _fmt(float(f.get("dominant_percent", 0) or 0))
    population = _population_noun(f)
    if plan.mode == "positive":
        if majority_healthy:
            return (
                f"Oxidative stress remains well controlled for most {population} "
                f"({healthy}% in healthy bands)"
            )
        lead = _few_elevated(
            float(f.get("elevated_share", 0) or 0),
            elevated,
            "show high oxidative stress",
            population=population,
        )
        if dominant and dominant.lower() != "low":
            return (
                f"{lead}, though only {healthy}% sit in the healthy range, "
                f"with {dominant} the most common category at {dominant_pct}%"
            )
        return f"{lead}, though only {healthy}% sit in the healthy range"
    base = f"Elevated oxidative stress affects {elevated}% of {population}"
    if dominant and dominant.lower() not in ("high", "very high"):
        return f"{base}, with {dominant} the most common category at {dominant_pct}%"
    return base


def _strength_observation(facts: dict) -> str:
    names = str(facts.get("low_risk_names") or "").strip()
    profiles = str(facts.get("profile_names") or "").strip()
    habits = str(facts.get("habit_labels") or "").strip()
    parts: list[str] = []
    if names:
        parts.append(f"Low-risk disease areas include {names}")
    else:
        low_risk = int(facts.get("low_risk_count", 0) or 0)
        if low_risk:
            parts.append(f"{low_risk} disease areas remain predominantly healthy")
    if profiles:
        parts.append(f"in-range lab profiles include {profiles}")
    else:
        count = int(facts.get("profiles_count", 0) or 0)
        if count:
            parts.append(f"{count} lab profiles show strong in-range rates")
    if habits:
        parts.append(f"healthy habits include {habits}")
    if not parts:
        return "No positive-win signals are listed in this report"
    text = parts[0]
    for part in parts[1:]:
        text += ". " + part[0].upper() + part[1:]
    return text


def _build_observation(plan: InsightPlan) -> str:
    f = plan.observation.facts
    elevated_value = float(f.get("elevated_share", 0) or 0)
    healthy_value = float(f.get("healthy_share", 0) or 0)
    elevated = _fmt(elevated_value)
    healthy = _fmt(healthy_value)
    # A positive tone can come from a low elevated share alone, so "most" is only
    # truthful when the healthy band actually holds a majority.
    majority_healthy = healthy_value > 50
    metric = human_metric_name(plan.observation.metric_id)
    dominant = str(f.get("dominant_label", "") or "")
    people = _of_population(f)
    population = _population_noun(f)

    kind = plan.observation.kind

    if kind == "disease_lead":
        return f"{metric} leads with {elevated}% {people} in elevated risk bands"

    if kind == "strength":
        return _strength_observation(f)

    if kind == "status":
        if plan.section_id == "participation":
            group = str(f.get("top_age_group", "") or "").strip()
            if not group:
                return ""
            return f"The largest enrolled age group is {group} at {_fmt(float(f.get('top_percent', 0) or 0))}%"
        return f"{elevated}% {people} sit in elevated risk bands"

    if kind == "cluster":
        return (
            f"{elevated}% {people} sit in elevated risk bands, with a "
            f"{human_cluster_name(str(f.get('dominant_cluster', '')))} health profile"
        )

    if kind == "lifestyle":
        if plan.observation.metric_id == "oxidative_stress":
            return _oxidative_observation(
                plan, elevated=elevated, healthy=healthy, majority_healthy=majority_healthy
            )
        if plan.mode == "positive":
            if plan.observation.metric_id == "physical_activity":
                if majority_healthy:
                    return f"Most {population} maintain healthy physical activity levels ({healthy}% in healthy bands)"
                return (
                    f"{_few_elevated(elevated_value, elevated, 'report poor physical activity', population=population)}, "
                    f"though just {healthy}% reach healthy activity levels"
                )
            if plan.observation.metric_id == "sleep":
                if majority_healthy:
                    return f"Most {population} achieve healthy sleep duration ({healthy}% in the recommended range)"
                return (
                    f"{_few_elevated(elevated_value, elevated, 'sleep outside the recommended range', population=population)}, "
                    f"though just {healthy}% sit within it"
                )
            if majority_healthy:
                return f"Most {population} show healthy {metric.lower()} patterns ({healthy}% in healthy bands)"
            return (
                f"{_few_elevated(elevated_value, elevated, f'show elevated {metric.lower()}', population=population)}, "
                f"though just {healthy}% sit in healthy bands"
            )
        if plan.observation.metric_id == "physical_activity":
            return (
                f"{_share_lead(plan)} report low physical activity "
                f"({elevated}% {people} in elevated bands)"
            )
        if plan.observation.metric_id == "sleep":
            return (
                f"{_share_lead(plan)} fall outside the recommended sleep range "
                f"({elevated}% {people})"
            )
        if plan.observation.metric_id == "nutrition":
            return f"Nutrition patterns leave {elevated}% {people} below a healthy score range"
        return f"{elevated}% {people} show elevated concern for {metric.lower()}"

    # 'distribution' and default
    if plan.mode == "positive":
        if plan.observation.metric_id == "overall_risk":
            if majority_healthy:
                return f"Most {population} remain in healthy risk bands, while {elevated}% fall within elevated risk categories"
            return f"{healthy}% {people} remain in healthy risk bands, while {elevated}% fall within elevated risk categories"
        if is_disease_metric(plan.observation.metric_id):
            narrative = select_variant(
                disease_positive_narratives(plan.observation.metric_id),
                plan.observation.metric_id,
                plan.severity_band,
                plan.section_id,
                _profile_cluster(plan),
            )
            if narrative:
                return f"{_strip_trailing_punct(narrative)} ({healthy}% in healthy bands)"
        return f"{metric} remains favourable, with {healthy}% {people} in healthy bands"

    if plan.observation.metric_id == "overall_risk":
        if float(elevated) <= 40:
            return f"{elevated}% {people} fall within Increased or High risk bands"
        suffix = f", with {dominant} as a major share" if dominant else ""
        return f"Elevated risk affects {elevated}% {people}{suffix}"

    if plan.observation.metric_id == "oxidative_stress":
        return _oxidative_observation(
            plan, elevated=elevated, healthy=healthy, majority_healthy=majority_healthy
        )

    concern_frames = [
        f"{metric} shows {elevated}% {people} in elevated risk bands",
        f"{metric} places {elevated}% {people} in elevated risk bands",
        f"Elevated {metric.lower()} risk reaches {elevated}% {people}",
    ]
    dominant_is_healthy = dominant.lower() in ("healthy", "low", "optimal", "low risk")
    if dominant and not dominant_is_healthy:
        concern_frames = [f"{frame}, with {dominant} as a major share" for frame in concern_frames]
    return select_variant(
        concern_frames,
        plan.observation.metric_id,
        plan.severity_band,
        plan.section_id,
        _profile_cluster(plan),
    )


def _metric_specific_medical_frame(frame_id: str | None) -> str:
    """Return a knowledge-layer medical frame when it is specific to this id."""
    if not frame_id:
        return ""
    generic = medical_frame("disease_generic")
    text = medical_frame(frame_id)
    if not text:
        return ""
    if frame_id != "disease_generic" and text.strip() == generic.strip():
        return ""
    return _strip_trailing_punct(text)


def _select_interpretation_clause(plan: InsightPlan) -> str:
    """Disease interpretation from knowledge.py — medical meaning, not actions."""
    if not is_disease_metric(plan.observation.metric_id):
        return ""
    clauses = [
        clause
        for clause in disease_interpretation_clauses(plan.observation.metric_id)
        if clause
        and _is_clause(clause)
        and not _ACTION_LIKE_EXPLANATION.search(clause)
    ]
    if not clauses:
        return ""
    return select_variant(
        clauses,
        plan.observation.metric_id,
        plan.severity_band,
        plan.section_id,
        _profile_cluster(plan),
        "interpretation",
    )


def _clinical_focus_why(plan: InsightPlan) -> str:
    """Existing knowledge clinical_focus, used only as a wording fallback."""
    metric_id = plan.observation.metric_id
    disease = get_disease_knowledge(metric_id)
    if disease is not None and disease.clinical_focus:
        return _strip_trailing_punct(disease.clinical_focus)
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None and lifestyle.clinical_focus:
        return _strip_trailing_punct(lifestyle.clinical_focus)
    return ""


def _build_explanation(
    plan: InsightPlan,
    *,
    used_why_stems: frozenset[str] | None = None,
) -> str:
    """Compose the Why sentence — medical significance only, never programme advice."""
    metric_id = plan.observation.metric_id
    frame_id = plan.explanation.medical_frame_id
    effect_ids = _unique_preserve(plan.explanation.effect_ids)
    effect = effect_ids[0] if effect_ids else None

    if plan.mode == "positive":
        positive_frames = {
            "positive_wins",
            "maintain",
            "participation",
            "workforce_resilience",
        }
        knowledge_why = ""
        if str(frame_id).endswith("_healthy"):
            knowledge_why = _metric_specific_medical_frame(frame_id)
        if not knowledge_why:
            knowledge_why = _metric_specific_medical_frame(f"{metric_id}_healthy")
        short_why = (
            knowledge_why
            or SHORT_WHY.get(f"{frame_id}_healthy")
            or SHORT_WHY.get(f"{metric_id}_healthy")
            or (SHORT_WHY.get(frame_id) if str(frame_id).endswith("_healthy") else None)
            or (SHORT_WHY.get(frame_id) if frame_id in positive_frames else None)
        )
        if not short_why and is_disease_metric(metric_id):
            metric = human_metric_name(metric_id)
            short_why = select_variant(
                [
                    f"Favourable {metric} bands reduce near-term clinical escalation risk across the workforce",
                    f"A predominantly healthy {metric} profile is a protective feature of the current cohort",
                    f"Low elevated {metric} prevalence supports continued preventive momentum",
                ],
                metric_id,
                plan.section_id,
                plan.severity_band,
                _profile_cluster(plan),
            )
        if not short_why:
            short_why = (
                _metric_specific_medical_frame("maintain")
                or SHORT_WHY.get("maintain")
                or SHORT_WHY.get("positive_wins")
            )
        styled = _restyle_why_opening(_strip_trailing_punct(short_why or ""), plan)
        return _avoid_used_why_stem(styled, plan, used_why_stems)

    knowledge_why = _metric_specific_medical_frame(frame_id) or _metric_specific_medical_frame(
        metric_id
    )
    short_why = knowledge_why or SHORT_WHY.get(frame_id) or SHORT_WHY.get(metric_id)
    if short_why:
        effect_clause = EFFECT_CLAUSES.get(effect) if effect else None
        # Never fold programme advice into the explanation.
        extra = None
        if (
            effect_clause
            and effect != frame_id
            and not _ACTION_LIKE_EXPLANATION.search(effect_clause)
        ):
            extra = effect_clause
        else:
            extra = _select_interpretation_clause(plan) or None
        if extra:
            joined = _join_explanation_parts(short_why, extra)
        else:
            joined = _strip_trailing_punct(short_why)
        styled = _restyle_why_opening(joined, plan)
        return _avoid_used_why_stem(styled, plan, used_why_stems)

    styled = _restyle_why_opening(_strip_trailing_punct(medical_frame(frame_id)), plan)
    return _avoid_used_why_stem(styled, plan, used_why_stems)


def _avoid_used_why_stem(
    text: str,
    plan: InsightPlan,
    used_why_stems: frozenset[str] | None,
) -> str:
    """If this report already used the same opener stem, rotate once more."""
    bare = _strip_trailing_punct(text)
    if not bare or not used_why_stems:
        return bare
    stem = bare.split(" ", 2)
    key = " ".join(stem[:2]).lower() if len(stem) >= 2 else bare.lower()
    if key not in used_why_stems:
        return bare
    retry = _restyle_why_opening(bare, plan)
    if _normalize_compare(retry) != _normalize_compare(bare):
        return _strip_trailing_punct(retry)

    metric_id = plan.observation.metric_id
    metric = human_metric_name(metric_id)
    if plan.mode == "positive":
        knowledge_retry = (
            _metric_specific_medical_frame(f"{metric_id}_healthy")
            or _metric_specific_medical_frame("maintain")
        )
        if knowledge_retry and _normalize_compare(knowledge_retry) != _normalize_compare(bare):
            return knowledge_retry
        return select_variant(
            [
                f"A predominantly healthy {metric} profile is a protective feature of the current cohort",
                f"Favourable {metric} bands reduce near-term clinical escalation risk across the workforce",
                f"Low elevated {metric} prevalence supports continued preventive momentum",
            ],
            metric_id,
            plan.section_id,
            "retry-positive",
            _profile_cluster(plan),
        )

    knowledge_retry = (
        _metric_specific_medical_frame(plan.explanation.medical_frame_id)
        or _metric_specific_medical_frame(metric_id)
        or _clinical_focus_why(plan)
    )
    if knowledge_retry and _normalize_compare(knowledge_retry) != _normalize_compare(bare):
        return knowledge_retry
    interpretation = _select_interpretation_clause(plan)
    if interpretation:
        promoted = _promote_clause_to_sentence(interpretation)
        if promoted and _normalize_compare(promoted) != _normalize_compare(bare):
            return _strip_trailing_punct(promoted)
    return select_variant(
        [
            f"Clinically, the {metric} pattern warrants preventive attention",
            f"The {metric} pattern has clear implications for long-term metabolic and cardiovascular health",
        ],
        metric_id,
        plan.section_id,
        "retry-concern",
        _profile_cluster(plan),
    )


def _select_render_levers(plan: InsightPlan) -> list[str]:
    """Choose levers for narrative rendering.

    For disease sections in concern mode: disease-native defaults first; company
    levers from the plan are kept as modifiers (never as the sole replacement).
    Reasoning output is unchanged — this only affects how text is composed.
    """
    plan_levers = _unique_preserve(list(plan.recommendation.lever_ids or []))
    section = plan.section_id
    metric_id = plan.observation.metric_id

    if plan.mode == "positive" or section not in ("disease_risks", "disease_deep_dive"):
        return plan_levers[:2]

    if not is_disease_metric(metric_id) and section != "disease_risks":
        return plan_levers[:2]

    defaults = list(get_default_levers(metric_id).get("high") or [])
    native = _unique_preserve(defaults)[:3]
    if not native:
        return plan_levers[:2]

    # Company / graph levers that are not disease-native become modifiers.
    modifiers = [
        lev
        for lev in plan_levers
        if lev not in native and lev != "maintain_wellness"
    ]
    one = plan.recommendation.one_thing
    if one and one not in native and one not in modifiers:
        modifiers.insert(0, one)

    # Render order: native disease levers, then at most one company modifier.
    ordered = native[:2]
    for mod in modifiers[:1]:
        if mod not in ordered:
            ordered.append(mod)
    return ordered


def _compose_disease_first_recommendation(
    plan: InsightPlan,
    render_levers: list[str],
) -> str:
    """Disease-native programmes first; company strategy as a trailing modifier."""
    metric = human_metric_name(plan.observation.metric_id)
    cluster = human_cluster_name(plan.explanation.cluster or "mixed").lower()
    defaults = list(get_default_levers(plan.observation.metric_id).get("high") or [])
    native = [lev for lev in render_levers if lev in defaults] or defaults[:3]
    native = _unique_preserve(native)[:3]
    labels = [_LEVER_PLAIN_LABELS.get(lev, lev.replace("_", " ")) for lev in native]
    labels = [lab for lab in labels if lab]

    verb = select_variant(
        ["Strengthen", "Expand", "Reinforce", "Scale"],
        plan.observation.metric_id,
        plan.section_id,
        plan.severity_band,
        _profile_cluster(plan),
    )

    if not labels:
        core = f"{verb} targeted prevention programmes for employees with elevated {metric} risk"
    elif len(labels) == 1:
        core = f"{verb} {labels[0]} programmes for employees with elevated {metric} risk"
    elif len(labels) == 2:
        core = (
            f"{verb} {labels[0]} and {labels[1]} programmes for employees "
            f"with elevated {metric} risk"
        )
    else:
        core = (
            f"{verb} {labels[0]}, {labels[1]} and {labels[2]} programmes "
            f"for employees with elevated {metric} risk"
        )

    company = plan.recommendation.one_thing
    company_outside = company and company not in native
    cluster_mod = cluster in ("cardiovascular", "metabolic", "hormonal")
    if company_outside or (cluster_mod and plan.section_id in ("disease_risks", "disease_deep_dive")):
        return (
            f"{core}, while integrating these interventions into broader "
            f"{cluster} prevention efforts"
        )
    return core


def _build_recommendation(
    plan: InsightPlan,
    *,
    render_levers: list[str] | None = None,
    used_phrases: frozenset[str] | None = None,
    explanation: str | None = None,
    force_alternate: bool = False,
) -> str:
    """HR action sentence — distinct from explanation; disease-first for disease sections."""
    levers = render_levers if render_levers is not None else _select_render_levers(plan)
    if not levers:
        return "Continue routine workforce health monitoring with targeted follow-up where risk rises"

    section = plan.section_id
    if (
        section in ("disease_risks", "disease_deep_dive")
        and plan.mode != "positive"
        and is_disease_metric(plan.observation.metric_id)
    ):
        primary = _compose_disease_first_recommendation(plan, levers)
        variants = _unique_phrases(
            [primary, *_recommendation_variants(plan, levers[0], disease_first=True)]
        )
    else:
        variants = _recommendation_variants(plan, levers[0], disease_first=False)

    used = set(used_phrases or [])
    if force_alternate and variants:
        used.add(_strip_trailing_punct(variants[0]))

    for phrase in variants:
        bare = _strip_trailing_punct(phrase)
        if bare in used:
            continue
        if explanation and _shares_action_theme(explanation, bare):
            continue
        return bare
    return _strip_trailing_punct(variants[0]) if variants else (
        "Continue routine workforce health monitoring with targeted follow-up where risk rises"
    )


def _recommendation_variants(
    plan: InsightPlan,
    lever: str,
    *,
    disease_first: bool = False,
) -> list[str]:
    """Ordered phrase candidates for a lever — first unused wins."""
    role = plan.recommendation.action_role or "section_support"
    cluster = human_cluster_name(plan.explanation.cluster or "mixed").lower()
    metric = human_metric_name(plan.observation.metric_id)
    section = plan.section_id
    lifestyle = plan.recommendation.lifestyle_priority or ""
    severity = plan.severity_band
    base = SHORT_ACTIONS.get(lever) or _soften(intervention_phrase(lever))
    enriched = (
        select_variant(
            lever_action_variants(lever), lever, section, role, severity, cluster, metric
        )
        or base
    )

    variants: list[str] = []

    # Disease concern sections only: disease/enriched phrasing first.
    if disease_first:
        variants.append(enriched)
        variants.append(base)
        keyed = SECTION_SUPPORT_ACTIONS.get((lever, section))
        if keyed:
            variants.append(keyed.format(cluster=cluster, metric=metric))
        metric_tied = _metric_tied_support_action(
            lever, metric, section, cluster, lifestyle, prefer_disease=True
        )
        if metric_tied:
            variants.append(metric_tied)
        return _unique_phrases(variants)

    if role == "org_primary":
        org = ORG_PRIMARY_ACTIONS.get(lever)
        if org:
            variants.append(
                org.format(cluster=cluster, metric=metric, severity=severity.replace("_", "-"))
            )
        variants.append(
            f"Given this company's {cluster} health profile, "
            f"{enriched[0].lower() + enriched[1:] if enriched else enriched}"
        )

    if role == "section_support":
        keyed = SECTION_SUPPORT_ACTIONS.get((lever, section))
        if keyed:
            variants.append(keyed.format(cluster=cluster, metric=metric))
        metric_tied = _metric_tied_support_action(
            lever, metric, section, cluster, lifestyle, prefer_disease=False
        )
        if metric_tied:
            variants.append(metric_tied)
        if not keyed and not metric_tied:
            variants.append(enriched)

    if role == "maintain":
        if section == "disease_deep_dive":
            variants.extend(
                [
                    f"Keep {metric} under routine preventive monitoring while most employees remain in healthy bands",
                    f"Protect the favourable {metric} profile with light-touch screening and lifestyle reinforcement",
                    f"Retain existing {metric} controls so this area does not become an organisational priority",
                ]
            )
        maintain = MAINTAIN_ACTIONS.get(section)
        if maintain:
            variants.append(maintain.format(cluster=cluster, metric=metric))
        variants.append(
            "Continue preventive wellness initiatives with routine workforce health monitoring"
        )

    variants.append(enriched)
    variants.append(base)
    return _unique_phrases(variants)


def _metric_tied_support_action(
    lever: str,
    metric: str,
    section: str,
    cluster: str,
    lifestyle: str,
    *,
    prefer_disease: bool = False,
) -> str | None:
    """Section/metric-aware supporting action — complementary to the org primary."""
    if section == "disease_deep_dive" or (section == "disease_risks" and prefer_disease):
        if lever == "lipid_screening":
            return f"Expand lipid screening pathways for employees with elevated {metric} risk"
        if lever == "bp_screening":
            return f"Expand blood-pressure screening pathways alongside {metric} prevention"
        if lever == "metabolic_screening":
            return f"Expand metabolic screening for employees with elevated {metric} risk"
        if lever == "cardiac_screening":
            return f"Expand cardiovascular screening for employees with elevated {metric} risk"
        if lever == "thyroid_screening":
            return f"Include thyroid screening for employees with elevated {metric} risk"
        if lever == "liver_screening":
            return f"Expand liver health monitoring alongside {metric} prevention"
        if lever.startswith("nutrition"):
            return f"Strengthen nutrition programmes for employees with elevated {metric} risk"
        if lever == "movement_programme":
            return f"Strengthen daily movement programmes for employees with elevated {metric} risk"
        if lever == "weight_management":
            return f"Strengthen weight-management programmes for employees with elevated {metric} risk"
        if lever == "stress_management":
            return f"Add stress-management support alongside {metric} prevention"
        if lever == "sleep_health":
            return f"Strengthen sleep hygiene support for employees with elevated {metric} risk"
        return f"Target clinical and lifestyle support at elevated {metric} risk groups"

    # Generic cluster-replacement line removed for disease_risks — disease-first path handles it.
    if section == "disease_risks":
        return (
            f"Strengthen disease-specific programmes for elevated {metric} risk, "
            f"aligned with broader {cluster} prevention"
        )

    if section == "physical_activity" and lever == "movement_programme":
        return select_variant(
            [
                "Close the activity gap with daily movement programmes and active work breaks",
                "Build daily movement into the working day through structured activity programmes",
                "Raise activity levels with workplace movement programmes and active breaks",
            ],
            lever,
            section,
            metric,
        )

    if section == "sleep" and lever in ("sleep_health", "recovery_programme"):
        return select_variant(
            [
                "Improve sleep hygiene through recovery-focused wellbeing initiatives",
                "Raise recovery capacity with sleep-health programmes across the workforce",
                "Strengthen sleep and recovery support for employees outside healthy ranges",
            ],
            lever,
            section,
            metric,
        )

    if section == "oxidative_stress" and lever in ("recovery_programme", "nutrition_whole_food"):
        return "Strengthen recovery programmes that reduce oxidative load across the workforce"

    if section == "overall_risk" and lifestyle:
        return None

    if lever == "target_high_risk":
        return "Direct coaching and clinical follow-up to the highest-risk employee groups"

    return None


def _unique_phrases(phrases: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for phrase in phrases:
        cleaned = _strip_trailing_punct(phrase.strip())
        if not cleaned:
            continue
        key = _normalize_compare(cleaned)
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
    return out


def compose_footer_text(insight: StructuredInsight) -> str:
    """Join Observation + Why + Action into one polished paragraph.

    Complete sentences are joined with spaces. Lowercase clause-style why text
    (rare after normalisation) still attaches with a comma.
    """
    if insight.observation == INSUFFICIENT_DATA_MESSAGE or not insight.observation.strip():
        return INSUFFICIENT_DATA_MESSAGE

    obs = _strip_trailing_punct(sanitize_insight_text(insight.observation))
    why_raw = sanitize_insight_text(insight.explanation)
    why = _strip_trailing_punct(why_raw)
    action = _ensure_sentence(sanitize_insight_text(insight.recommendation))

    if not why:
        body = _ensure_sentence(obs)
    elif _is_clause(why):
        body = f"{obs}, {why}."
    else:
        # Structured fields are already complete sentences — join without
        # introducing ``.,`` or lowercase mid-paragraph starts.
        body = f"{_ensure_sentence(obs)} {_ensure_sentence(_capitalize(why))}"

    text = _polish_punctuation(f"{body} {action}")
    text = sanitize_insight_text(text)

    max_words = word_limit_for_tone(insight.tone)
    if count_words(text) <= max_words:
        return text

    # Prefer observation + action when over budget
    compact = _polish_punctuation(f"{_ensure_sentence(obs)} {action}")
    if count_words(compact) <= max_words:
        return sanitize_insight_text(compact)

    return limit_words(text, max_words)


def word_limit_for_tone(tone: InsightTone) -> int:
    if tone == "positive":
        return POSITIVE_MAX_WORDS
    if tone == "neutral":
        return NEUTRAL_MAX_WORDS
    return CONCERN_MAX_WORDS


def _is_insufficient_plan(plan: InsightPlan) -> bool:
    f = plan.observation.facts
    if plan.observation.kind == "status" and plan.section_id == "participation":
        return not str(f.get("top_age_group", "") or "").strip()
    if plan.observation.kind == "strength":
        return (
            int(f.get("low_risk_count", 0) or 0) == 0
            and int(f.get("profiles_count", 0) or 0) == 0
            and int(f.get("habits_count", 0) or 0) == 0
            and not str(f.get("low_risk_names") or "").strip()
            and not str(f.get("habit_labels") or "").strip()
            and not str(f.get("profile_names") or "").strip()
        )
    # No numeric signal at all
    elevated = f.get("elevated_share")
    healthy = f.get("healthy_share")
    if elevated is None and healthy is None and not f.get("display_name"):
        return True
    return False


def _insufficient_structured(plan: InsightPlan) -> StructuredInsight:
    observation, explanation, recommendation = ensure_structured_field_punctuation(
        INSUFFICIENT_DATA_MESSAGE, "", ""
    )
    return StructuredInsight(
        section_id=plan.section_id,
        tone="neutral",
        severity_band=plan.severity_band,
        confidence=plan.confidence,
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
        effect_ids=[],
        lever_ids=[],
        related_metrics=[plan.observation.metric_id],
    )


def _soften_explanation(text: str) -> str:
    """Hedge confident verbs for low-confidence insights — keep full sentences."""
    replacements = [
        (r"^This indicates ", "This may indicate "),
        (r"^This suggests ", "This may suggest "),
        (r"^This highlights ", "This may highlight "),
        (r"^This shows ", "This may show "),
        (r"^This increases ", "This may increase "),
        (r"^This raises ", "This may raise "),
        (r"^This affects ", "This may affect "),
        (r"^This contributes ", "This may contribute "),
        (r"^This influences ", "This may influence "),
        (r"^This reflects ", "This may reflect "),
        (r"^This supports ", "This may support "),
        (r"^This identifies ", "This may identify "),
        # Legacy participial openings (pre-promotion)
        (r"^indicating ", "which may indicate "),
        (r"^signalling ", "which may signal "),
        (r"^signaling ", "which may signal "),
        (r"^highlighting ", "which may highlight "),
        (r"^pointing ", "which may point "),
        (r"^increasing ", "which may increase "),
        (r"^raising ", "which may raise "),
        (r"^showing ", "which may show "),
        (r"^impairing ", "which may impair "),
        (r"^an upstream", "potentially an upstream"),
        (r"^a primary", "potentially a primary"),
        (r"^a screenable", "potentially a screenable"),
    ]
    out = text
    for pattern, replacement in replacements:
        out = re.sub(pattern, replacement, out, count=1, flags=re.IGNORECASE)
    return out


def _soften_recommendation(text: str) -> str:
    replacements = [
        (r"^Prioritise ", "Consider prioritising "),
        (r"^Prioritize ", "Consider prioritizing "),
        (r"^Strengthen ", "Consider strengthening "),
        (r"^Promote ", "Consider promoting "),
        (r"^Support ", "Consider supporting "),
        (r"^Scale ", "Consider scaling "),
        (r"^Focus ", "Consider focusing "),
        (r"^Include ", "Consider including "),
        (r"^Introduce ", "Consider introducing "),
        (r"^Expand ", "Consider expanding "),
        (r"^Make ", "Consider making "),
        (r"^Reduce ", "Consider reducing "),
        (r"^Sustain ", "Consider sustaining "),
        (r"^Reinforce ", "Consider reinforcing "),
        (r"^Maintain ", "Consider maintaining "),
        (r"^Continue ", "Consider continuing "),
    ]
    out = text
    for pattern, replacement in replacements:
        out = re.sub(pattern, replacement, out, count=1, flags=re.IGNORECASE)
    return out


def _polish_punctuation(text: str) -> str:
    out = re.sub(r"\s+", " ", text)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    # Fix composition artifacts from joining punctuated phrases
    out = re.sub(r"\.\s*,", ".", out)
    out = re.sub(r",\s*\.", ".", out)
    out = re.sub(r",\s*,", ",", out)
    out = re.sub(r"\.\s*\.", ".", out)
    out = re.sub(r"([.!?]){2,}", r"\1", out)
    # Capitalise the start of a new sentence after a terminator
    out = re.sub(r"([.!?])\s+([a-z])", lambda m: f"{m.group(1)} {m.group(2).upper()}", out)
    out = re.sub(r"\(\s+", "(", out)
    out = re.sub(r"\s+\)", ")", out)
    # keep "). " as valid when sentence ends after paren — join logic avoids "). indicating"
    out = re.sub(r"\)\.", ").", out)
    return out.strip()


def _strip_trailing_punct(s: str) -> str:
    return re.sub(r"[.!,;:]+$", "", s.strip()).strip()


def _soften(phrase: str) -> str:
    return re.sub(r"\.$", "", phrase)


def _trim_period(s: str) -> str:
    return re.sub(r"\.$", "", s)


def _ensure_sentence(text: str) -> str:
    trimmed = text.strip()
    if not trimmed:
        return trimmed
    return trimmed if re.search(r"[.!?]$", trimmed) else f"{trimmed}."


def _capitalize(text: str) -> str:
    t = text.strip()
    if not t:
        return t
    return t[0].upper() + t[1:]


def _fmt(n: float) -> str:
    """Mirror JS `String(Math.round(n * 10) / 10)` — no trailing .0 for whole numbers."""
    value = round1(n)
    if value == int(value):
        return str(int(value))
    return str(value)

SHORT_WHY: dict[str, str] = {
    "overall_risk": "This indicates that a meaningful proportion of employees are at elevated overall health risk.",
    "overall_risk_healthy": "This reflects a healthy overall workforce risk profile.",

    "physical_activity": "This increases the risk of future metabolic and cardiovascular conditions.",
    "physical_activity_healthy": "This supports long-term metabolic and cardiovascular health.",

    "sleep": "This affects recovery, concentration, and metabolic health.",
    "sleep_healthy": "This supports healthy recovery and day-to-day wellbeing.",

    "nutrition": "This influences metabolic health, blood sugar regulation, and heart health.",
    "nutrition_healthy": "This helps maintain healthy metabolic and cardiovascular function.",

    "oxidative_stress": "This indicates increased cellular stress and slower recovery.",
    "oxidative_stress_healthy": "This reflects healthy recovery and good cellular resilience.",

    "type_2_diabetes": "This indicates an increased risk of developing Type 2 diabetes.",
    "hypertension": "This increases the risk of cardiovascular disease and stroke.",
    "obesity": "This contributes to several long-term metabolic and cardiovascular conditions.",
    "dyslipidemia": "This indicates an increased long-term risk of cardiovascular disease.",
    "cardiac_health": "This highlights increased cardiovascular risk across the workforce.",
    "nafld": "This suggests early metabolic changes that may affect liver health.",
    "thyroid_health": "This may contribute to changes in energy levels and metabolism.",
    "pcos_pcod": "This highlights the need for targeted women's metabolic and hormonal health support.",
    "metabolic_syndrome": "This indicates that multiple metabolic risk factors are occurring together.",

    "metabolic_age": "This suggests accelerated metabolic ageing in part of the workforce.",
    "bmi_waist": "This increases the risk of metabolic and cardiovascular disease.",

    "positive_wins": "This highlights areas where workforce health is performing well.",
    "participation": "This helps identify opportunities to improve programme participation.",
    "maintain": "This supports continued investment in preventive health initiatives.",

    "recovery_strain": "This suggests that employee recovery may be inadequate.",
    "cardio_nutrition": "This highlights the importance of heart-healthy nutrition.",
    "movement_priority": "This identifies physical activity as the most impactful lifestyle intervention.",
    "metabolic_cluster": "This indicates that multiple metabolic risk factors are occurring together.",
    "cardio_cluster": "This shows that cardiovascular risk factors are reinforcing each other.",

    "workforce_resilience": "This supports long-term workforce health and resilience.",
    "disease_generic": "This supports preventive care through targeted screening and lifestyle intervention.",
}

EFFECT_CLAUSES: dict[str, str] = {
    "recovery_strain": "Sleep quality and recovery require greater attention across the workforce.",
    "cardio_nutrition": "Nutrition should be a key focus for improving cardiovascular health.",
    "movement_priority": "Increasing daily physical activity should be a priority for the workforce.",
    "metabolic_cluster": "Several metabolic risk factors are occurring together, increasing the likelihood of future chronic disease.",
    "cardio_cluster": "Blood pressure and lipid abnormalities are occurring together, increasing overall cardiovascular risk.",
    "workforce_resilience": "Healthy sleep and regular physical activity are supporting long-term workforce wellbeing.",
}
SHORT_ACTIONS: dict[str, str] = {
    "metabolic_screening": "Include annual metabolic screening and lifestyle support for at-risk employees",
    "lipid_screening": "Include routine lipid screening with heart-healthy nutrition support",
    "bp_screening": "Promote regular blood pressure screening and stress management support",
    "thyroid_screening": "Include thyroid function screening in routine health checks",
    "liver_screening": "Include liver function screening alongside metabolic assessments",
    "cardiac_screening": "Prioritise cardiovascular screening for employees at elevated risk",
    "nutrition_refined_carb": "Promote nutrition programmes that reduce refined carbohydrate intake",
    "nutrition_heart_healthy": "Promote heart-healthy nutrition programmes across the workforce",
    "nutrition_sodium": "Promote nutrition programmes that encourage lower sodium intake",
    "nutrition_whole_food": "Promote balanced eating with whole foods and healthy portions",
    "movement_programme": "Promote daily movement programmes and active work breaks",
    "weight_management": "Support weight management programmes for at-risk employees",
    "sleep_health": "Promote healthy sleep habits and recovery-focused wellbeing initiatives",
    "stress_management": "Promote stress management and healthy workload practices",
    "recovery_programme": "Support recovery through healthy sleep, stress, and restorative habits",
    "womens_health": "Strengthen women's health awareness and confidential support services",
    "smoking_cessation": "Provide smoking cessation support for interested employees",
    "alcohol_moderation": "Promote responsible alcohol use through health education",
    "maintain_wellness": "Continue preventive wellness initiatives and routine health monitoring",
    "target_high_risk": "Focus coaching and clinical follow-up on employees at elevated risk",
    "scale_preventive_care": "Expand preventive care across screening, nutrition, and physical activity",
    "clinical_review": "Recommend clinical review for employees with elevated health risk",
}

# Company-level "single most appropriate action" phrasing (uses profile cluster).
ORG_PRIMARY_ACTIONS: dict[str, str] = {
    "metabolic_screening": "Make annual metabolic screening a key response to this {cluster} risk profile",
    "lipid_screening": "Make routine lipid screening with heart-healthy nutrition a core prevention programme",
    "bp_screening": "Make blood pressure screening and stress management a key prevention programme",
    "thyroid_screening": "Make thyroid screening a routine part of the organisation's preventive health programme",
    "liver_screening": "Make liver function and metabolic screening a key clinical response",
    "cardiac_screening": "Make cardiovascular screening a key response for this {cluster} risk profile",
    "nutrition_refined_carb": "Make reducing refined carbohydrates the organisation's primary nutrition initiative",
    "nutrition_heart_healthy": "Make heart-healthy nutrition the primary prevention programme for this {cluster} profile",
    "nutrition_sodium": "Make sodium reduction a key workplace nutrition initiative",
    "nutrition_whole_food": "Make whole-food, balanced eating the organisation's primary lifestyle programme",
    "movement_programme": "Make daily workplace movement the organisation's primary physical activity initiative",
    "weight_management": "Make weight management programmes the primary response for higher-risk employees",
    "sleep_health": "Make healthy sleep a key workplace recovery initiative",
    "stress_management": "Make stress management and healthy workload practices a core wellbeing programme",
    "recovery_programme": "Make sleep and stress recovery the organisation's primary recovery initiative",
    "target_high_risk": "Make targeted coaching for higher-risk employees the organisation's primary intervention",
    "scale_preventive_care": "Expand preventive care across screening, nutrition, and physical activity",
    "clinical_review": "Make clinical review for higher-risk employees the organisation's primary follow-up action",
}

# Section-specific supporting actions — avoid repeating the org-primary sentence.
SECTION_SUPPORT_ACTIONS: dict[tuple[str, str], str] = {
    ("movement_programme", "physical_activity"): "Reduce the activity gap through daily movement programmes and active work breaks",
    ("movement_programme", "oxidative_stress"): "Use daily movement programmes to support recovery and reduce oxidative stress",
    ("movement_programme", "bmi_waist"): "Combine body composition support with daily workplace movement programmes",

    ("sleep_health", "sleep"): "Promote healthy sleep habits through recovery-focused wellbeing initiatives",
    ("sleep_health", "oxidative_stress"): "Strengthen healthy sleep habits to reduce oxidative stress across the workforce",

    ("recovery_programme", "oxidative_stress"): "Support recovery through sleep, stress management, and restorative habits",
    ("recovery_programme", "sleep"): "Develop an integrated recovery programme covering sleep and stress",

    ("nutrition_heart_healthy", "nutrition"): "Promote heart-healthy nutrition programmes across the workforce",
    ("nutrition_whole_food", "nutrition"): "Promote balanced whole-food eating patterns across the organisation",

    ("weight_management", "bmi_waist"): "Support weight management programmes for employees with elevated BMI and waist circumference",

    ("target_high_risk", "overall_risk"): "Focus coaching and clinical follow-up on employees at elevated risk",
    ("scale_preventive_care", "overall_risk"): "Expand preventive care across screening, nutrition, and physical activity",

    ("lipid_screening", "disease_risks"): "Prioritise routine lipid screening for employees with the leading disease risk",
    ("bp_screening", "disease_risks"): "Prioritise blood pressure screening and stress management for the leading disease risk",
    ("metabolic_screening", "disease_risks"): "Prioritise metabolic screening for employees within the leading disease risk group",
}

MAINTAIN_ACTIONS: dict[str, str] = {
    "positive_highlights": "Continue the initiatives that are supporting these positive health outcomes",
    "participation": "Maintain strong participation while encouraging more engagement where needed",
    "overall_risk": "Continue preventive wellness initiatives and routine workforce health monitoring",
    "physical_activity": "Continue promoting healthy physical activity through existing movement programmes",
    "sleep": "Continue reinforcing healthy sleep habits through existing wellbeing initiatives",
    "oxidative_stress": "Continue promoting recovery habits that help maintain healthy oxidative stress levels",
    "disease_risks": "Continue preventive screening and lifestyle programmes where disease risk remains low",
    "disease_deep_dive": "Continue routine monitoring while reinforcing healthy behaviours in this disease area",
}

# ---------------------------------------------------------------------------
# compose/compose_insight.py
# ---------------------------------------------------------------------------


def compose_insight(
    plan: InsightPlan,
    *,
    used_phrases: frozenset[str] | None = None,
    used_why_stems: frozenset[str] | None = None,
) -> StructuredInsight:
    return _generate_structured_insight_from_plan(
        plan, used_phrases=used_phrases, used_why_stems=used_why_stems
    )


# ---------------------------------------------------------------------------
# insight/generate_leadership.py
# ---------------------------------------------------------------------------

BODY_MAX_WORDS = 30


def generate_leadership_cards(profile: CompanyHealthProfile) -> list[LeadershipTakeawayCard]:
    return [
        _workforce_status_card(profile),
        _lifestyle_priority_card(profile),
        _disease_focus_card(profile),
        _strategic_next_step_card(profile),
    ]


def _workforce_status_card(profile: CompanyHealthProfile) -> LeadershipTakeawayCard:
    overall_risk_finding = profile.findings.get("overall_risk")
    elevated = (
        overall_risk_finding.elevated_share if overall_risk_finding else None
    )
    if elevated is None:
        elevated = profile.overall_burden
    healthy_share = (
        overall_risk_finding.healthy_share
        if overall_risk_finding and overall_risk_finding.healthy_share is not None
        else max(0.0, 100 - elevated)
    )
    healthy = profile.overall_severity in ("very_low", "low")

    if healthy:
        headline = "Healthy Workforce"
        body = (
            f"{_fmt(healthy_share)}% of employees sit in healthy risk bands. Maintain annual "
            "assessments and reinforce preventive habits."
        )
    elif profile.overall_severity == "moderate":
        headline = "Emerging Health Risks"
        body = (
            f"{_fmt(elevated)}% of employees sit in elevated risk bands. Early lifestyle "
            "programmes can reverse this trajectory."
        )
    elif profile.overall_severity == "high":
        headline = "Growing Disease Burden"
        body = (
            f"{_fmt(elevated)}% of employees are at elevated risk. Organisation-wide prevention "
            "should become a leadership priority."
        )
    else:
        headline = "Immediate Attention Needed"
        body = (
            f"{_fmt(elevated)}% of employees are at elevated risk. A coordinated preventive "
            "strategy is required now."
        )

    return _card("workforce-health", "Workforce Health Status", headline, body, profile, healthy)


def _lifestyle_priority_card(profile: CompanyHealthProfile) -> LeadershipTakeawayCard:
    priority = profile.lifestyle_priority
    activity_finding = profile.findings.get("physical_activity")
    sleep_finding = profile.findings.get("sleep")
    nutrition_finding = profile.findings.get("nutrition")
    activity = activity_finding.elevated_share if activity_finding else 0
    sleep = sleep_finding.elevated_share if sleep_finding else 0
    nutrition = nutrition_finding.elevated_share if nutrition_finding else 0

    if priority == "recovery":
        headline = "Recovery Opportunity"
        body = (
            f"Poor sleep ({_fmt(sleep)}%) and elevated oxidative stress point to a recovery gap. "
            "Improve sleep hygiene before adding new wellness programmes."
        )
    elif priority == "sleep":
        headline = "Sleep Opportunity"
        body = (
            f"{_fmt(sleep)}% of employees fall outside healthy sleep ranges. Recovery-focused "
            "initiatives will lift energy and metabolic health."
        )
    elif priority == "nutrition":
        headline = "Nutrition Opportunity"
        body = (
            f"Nutrition scores leave {_fmt(nutrition)}% below a healthy range. Heart-healthy "
            "eating is the clearest lifestyle lever."
        )
    elif priority == "strong":
        headline = "Healthy Lifestyle Foundation"
        body = (
            "Movement and recovery habits are strong. Keep reinforcing them while supporting "
            "higher-risk individuals."
        )
    else:
        headline = "Movement Opportunity"
        body = (
            f"{_fmt(activity)}% of employees report low activity. Daily movement programmes "
            "offer the largest lifestyle gain."
        )

    return _card(
        "lifestyle-priority",
        "Lifestyle Priority",
        headline,
        body,
        profile,
        priority == "strong",
    )


def _disease_focus_card(profile: CompanyHealthProfile) -> LeadershipTakeawayCard:
    # Same ranking the Top Disease Risk chart displays
    lead = profile.chart_top_diseases[0] if profile.chart_top_diseases else None
    top_disease_id: MetricId | None = None
    if lead is not None:
        top_disease_id = lead.metric_id
    else:
        top_disease_id = next(
            (r.metric_id for r in profile.top_risks if is_disease_metric(r.metric_id)),
            profile.top_risks[0].metric_id if profile.top_risks else None,
        )

    if lead is not None:
        elevated = lead.high_risk_percent
    elif top_disease_id:
        finding = profile.findings.get(top_disease_id)
        elevated = finding.elevated_share if finding else 0
    else:
        elevated = 0

    if lead is not None:
        disease_name = lead.name
    elif top_disease_id:
        disease_name = human_metric_name(top_disease_id)
    else:
        disease_name = None

    cluster = profile.dominant_cluster

    if cluster == "healthy" or not disease_name or elevated < 20:
        headline = "Preventive Healthcare Focus"
        body = (
            "No single disease dominates. Broad screening and prevention will deliver the best "
            "organisational return."
        )
    elif cluster == "cardiovascular":
        headline = "Heart Health Priority"
        body = (
            f"{disease_name} leads at {_fmt(elevated)}% elevated risk. Focus screening on blood "
            "pressure, lipids and cardiac markers."
        )
    elif cluster == "hormonal":
        headline = "Hormonal Health Opportunity"
        body = (
            f"{disease_name} affects {_fmt(elevated)}% in elevated bands. Offer targeted "
            "screening and specialist support pathways."
        )
    else:
        headline = "Metabolic Health Priority"
        body = (
            f"{disease_name} leads at {_fmt(elevated)}% elevated risk. Prioritise metabolic "
            "screening and lifestyle support for this group."
        )

    # Prefer a curated leadership takeaway for the identified disease when it fits
    # the body budget. The disease, headline, number and card identity are all
    # unchanged — only the supporting sentence gains medically-authored phrasing.
    if disease_name and cluster != "healthy" and elevated >= 20 and top_disease_id:
        takeaways = disease_leadership_takeaways(top_disease_id)
        if takeaways:
            pick = select_variant(
                takeaways, top_disease_id, cluster, profile.overall_severity
            )
            candidate = f"{disease_name} leads at {_fmt(elevated)}% elevated risk. {pick}"
            if pick and count_words(candidate) <= BODY_MAX_WORDS:
                body = candidate

    return _card(
        "disease-focus",
        "Primary Disease Focus",
        headline,
        body,
        profile,
        cluster == "healthy",
    )


def _strategic_next_step_card(profile: CompanyHealthProfile) -> LeadershipTakeawayCard:
    severity = profile.overall_severity
    one_thing = profile.one_thing
    lever_label = _lever_to_plain_action(one_thing.lever) if one_thing else None

    if severity in ("very_low", "low"):
        headline = "Maintain Momentum"
        body = "Keep annual health assessments and recognise healthy behaviours to protect current gains."
    elif severity == "moderate":
        headline = "Target Elevated-Risk Groups"
        body = (
            f"Concentrate resources on elevated-risk employees. Start with {lever_label}."
            if lever_label
            else "Concentrate coaching and clinical follow-up on employees in elevated risk bands."
        )
    elif severity == "high":
        headline = "Scale Preventive Care"
        body = (
            f"Expand screening and lifestyle programmes company-wide, led by {lever_label}."
            if lever_label
            else "Expand screening, nutrition and fitness programmes across the organisation."
        )
    else:
        headline = "Build a Long-Term Health Strategy"
        body = (
            "Integrate regular screenings, leadership accountability and outcome tracking into "
            "organisational health planning."
        )

    return _card("strategic-next-step", "Strategic Next Step", headline, body, profile, False)


def _card(
    id_: str,
    title: str,
    headline: str,
    body: str,
    profile: CompanyHealthProfile,
    positive: bool,
) -> LeadershipTakeawayCard:
    confidence = _profile_confidence_as_insight(profile)
    clean_body = sanitize_insight_text(body)
    if confidence.band == "low":
        clean_body = _soften_leadership_body(clean_body)
    clean_body = limit_words(clean_body, BODY_MAX_WORDS)

    observation, explanation, recommendation = _leadership_structured_fields(
        headline, clean_body
    )

    structured = StructuredInsight(
        section_id="leadership",
        tone="positive" if positive else "concern",
        severity_band=profile.overall_severity,
        confidence=confidence,
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
        headline=headline,
        effect_ids=[],
        lever_ids=[],
    )

    return LeadershipTakeawayCard(
        id=id_,
        title=title,
        headline=headline,
        body=clean_body,  # UI unchanged — not cloned from structured fields
        confidence=confidence,
        structured=structured,
    )


def _leadership_structured_fields(headline: str, body: str) -> tuple[str, str, str]:
    """Split leadership body into distinct Observation / Explanation / Recommendation.

    Headline and body for the card UI are unchanged; only structured.* differs.
    """
    sentences = [
        s.strip()
        for s in re.split(r"(?<=[.!?])\s+", (body or "").strip())
        if s and s.strip()
    ]
    if not sentences:
        return ensure_structured_field_punctuation(
            f"{headline} requires structured leadership attention",
            "Workforce health patterns in this area carry organisational implications.",
            "Align leadership follow-up with the organisation's preventive health priorities.",
        )

    observation = sentences[0]
    if len(sentences) == 1:
        explanation = (
            f"{headline} reflects a workforce pattern with material implications "
            "for preventive planning."
        )
        recommendation = (
            "Translate this priority into a clear ownership plan with measurable follow-up."
        )
    else:
        recommendation = sentences[-1]
        mid = sentences[1:-1]
        if mid:
            explanation = " ".join(mid)
        else:
            explanation = (
                "Early, coordinated preventive action reduces long-term health "
                "and productivity risk for the workforce."
            )
        if _normalize_compare(explanation) == _normalize_compare(recommendation):
            explanation = (
                "This pattern benefits from structured preventive investment "
                "and clear leadership ownership."
            )
        if _normalize_compare(observation) == _normalize_compare(recommendation):
            recommendation = (
                "Convert this finding into a time-bound preventive action owned by HR and leadership."
            )

    return ensure_structured_field_punctuation(observation, explanation, recommendation)


def _soften_leadership_body(text: str) -> str:
    out = text
    out = re.sub(r"\bshould become\b", "may need to become", out, flags=re.IGNORECASE)
    out = re.sub(r"\bis required\b", "may be needed", out, flags=re.IGNORECASE)
    out = re.sub(r"\bwill deliver\b", "can help deliver", out, flags=re.IGNORECASE)
    out = re.sub(r"\bwill lift\b", "may lift", out, flags=re.IGNORECASE)
    out = re.sub(r"\boffer the largest\b", "may offer a strong", out, flags=re.IGNORECASE)
    out = re.sub(r"\bPrioritise\b", "Consider prioritising", out)
    out = re.sub(r"\bFocus screening\b", "Consider focusing screening", out)
    out = re.sub(r"\bExpand screening\b", "Consider expanding screening", out)
    out = re.sub(r"\bConcentrate resources\b", "Consider concentrating resources", out)
    return out


def _profile_confidence_as_insight(profile: CompanyHealthProfile) -> InsightConfidence:
    score = profile.profile_confidence
    band = "high" if score >= 0.75 else "moderate" if score >= 0.5 else "low"
    return InsightConfidence(
        score=score,
        band=band,
        factors=ConfidenceFactors(
            data_quality=score,
            signal_strength=0.7,
            graph_support=0.8 if len(profile.emergent_priorities) > 0 else 0.45,
            knowledge_coverage=1,
        ),
        notes=["leadership"],
    )


_LEVER_TO_PLAIN_ACTION: dict[str, str] = {
    "movement_programme": "daily movement programmes",
    "sleep_health": "sleep-health initiatives",
    "recovery_programme": "recovery programmes",
    "nutrition_heart_healthy": "heart-healthy nutrition",
    "nutrition_refined_carb": "nutrition counselling",
    "weight_management": "weight-management support",
    "metabolic_screening": "metabolic screening",
    "lipid_screening": "lipid screening",
    "bp_screening": "blood-pressure screening",
    "cardiac_screening": "cardiovascular screening",
    "target_high_risk": "targeted high-risk support",
    "scale_preventive_care": "scaled preventive care",
}


def _lever_to_plain_action(lever: str) -> str | None:
    return _LEVER_TO_PLAIN_ACTION.get(lever)


def top_disease_metric(profile: CompanyHealthProfile) -> MetricId | None:
    """Exported for tests — ensure disease metric lookup stays typed."""
    return next(
        (r.metric_id for r in profile.top_risks if is_disease_metric(r.metric_id)),
        None,
    )


# ---------------------------------------------------------------------------
# generate.py — public API
# ---------------------------------------------------------------------------

# @deprecated Prefer compose_company_profile
build_profile_from_slices = compose_company_profile

_EMPTY_CONFIDENCE = InsightConfidence(
    score=0,
    band="low",
    factors=ConfidenceFactors(
        data_quality=0,
        signal_strength=0,
        graph_support=0,
        knowledge_coverage=0,
    ),
    notes=["insufficient_data"],
)


def _insufficient_narrative(section_id: SectionId) -> ChartNarrative:
    observation, explanation, recommendation = ensure_structured_field_punctuation(
        INSUFFICIENT_DATA_MESSAGE, "", ""
    )
    structured = StructuredInsight(
        section_id=section_id,
        tone="neutral",
        severity_band="low",
        confidence=_EMPTY_CONFIDENCE,
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
    )
    return ChartNarrative(
        tone="neutral",
        text=INSUFFICIENT_DATA_MESSAGE,
        structured=structured,
        confidence=_EMPTY_CONFIDENCE,
    )


ChartDataInput = MetricFinding | None
GenerateInsightOptions = dict[str, Any]


@dataclass
class RecommendationLedger:
    """Tracks levers/phrases already used on a report so sections vary deterministically."""

    used_levers: list[str] = field(default_factory=list)
    used_phrases: list[str] = field(default_factory=list)
    used_why_stems: list[str] = field(default_factory=list)

    def note(
        self,
        lever: str | None,
        phrase: str | None,
        *,
        explanation: str | None = None,
    ) -> None:
        if lever:
            self.used_levers.append(lever)
        if phrase:
            # Compare bare text so trailing punctuation does not defeat dedupe.
            cleaned = re.sub(r"[.!,;:]+$", "", phrase.strip()).strip()
            if cleaned:
                self.used_phrases.append(cleaned)
        if explanation:
            stem = " ".join(_strip_trailing_punct(explanation).split()[:2]).lower()
            if stem:
                self.used_why_stems.append(stem)


def generate_insight(
    section_id: SectionId,
    chart_data: ChartDataInput,
    profile: CompanyHealthProfile,
    options: GenerateInsightOptions | None = None,
    *,
    ledger: RecommendationLedger | None = None,
) -> ChartNarrative:
    """Generate a chart/report narrative for one section.

    Args:
        section_id: Section key (e.g. 'overall_risk', 'disease_risks').
        chart_data: Finding for the chart currently displayed — single source
            of truth for facts. Pass ``None`` to reason from the company
            profile alone.
        profile: Company health profile from ``compose_company_profile(data)``.
        options: Optional dict with an ``active_metric_id`` override.
        ledger: Optional cross-section ledger to avoid repeating actions.
    """
    from .formatter import format_chart_footer

    options = options or {}

    if len(profile.coverage) == 0 and not chart_data:
        return _insufficient_narrative(section_id)

    working_profile = (
        rebuild_profile_with_finding(profile, chart_data) if chart_data else profile
    )

    active_metric_id = options.get("active_metric_id") or (
        chart_data.metric_id if chart_data else None
    )
    plan = reason_about_section(
        section_id,
        working_profile,
        ReasonOptions(
            active_metric_id=active_metric_id,
            chart_finding=chart_data or None,
            used_levers=list(ledger.used_levers) if ledger else None,
        ),
    )

    used_phrases = frozenset(ledger.used_phrases) if ledger else None
    used_why = frozenset(ledger.used_why_stems) if ledger else None
    if isinstance(plan, list):
        structured = compose_insight(
            plan[0], used_phrases=used_phrases, used_why_stems=used_why
        )
    else:
        structured = compose_insight(
            plan, used_phrases=used_phrases, used_why_stems=used_why
        )

    narrative = format_chart_footer(structured)
    if ledger is not None:
        primary = (structured.lever_ids or [None])[0]
        ledger.note(
            primary,
            structured.recommendation,
            explanation=structured.explanation,
        )
    return narrative


def generate_structured_insight(
    section_id: SectionId,
    chart_data: ChartDataInput,
    profile: CompanyHealthProfile,
    options: GenerateInsightOptions | None = None,
) -> StructuredInsight:
    """Structured Observation -> Why -> Action (no footer join)."""
    return generate_insight(section_id, chart_data, profile, options).structured


def generate_leadership_takeaways(profile: CompanyHealthProfile) -> list[LeadershipTakeawayCard]:
    from .formatter import format_leadership_cards

    if len(profile.coverage) == 0:
        return []
    return format_leadership_cards(profile)


def generate_positive_insights(profile: CompanyHealthProfile) -> ChartNarrative:
    """Positive organisational strengths narrative.

    Reusable by dashboard Positive Wins panel, PDF reports, and APIs.
    """
    chart_data = profile.findings.get("positive_wins")
    if not chart_data and len(profile.coverage) == 0:
        return _insufficient_narrative("positive_highlights")
    return generate_insight("positive_highlights", chart_data, profile)


def generate_insight_from_data(
    section_id: SectionId,
    data: DashboardInput,
    options: GenerateInsightOptions | None = None,
) -> ChartNarrative:
    """Convenience: profile from data then insight (no local chart override)."""
    return generate_insight(section_id, None, compose_company_profile(data), options)


def generate_insight_from_slices(
    section_id: SectionId,
    slices: DashboardInput,
    options: GenerateInsightOptions | None = None,
) -> ChartNarrative:
    """@deprecated Prefer generate_insight_from_data."""
    return generate_insight_from_data(section_id, slices, options)


def generate_insight_with_local_finding(
    section_id: SectionId,
    profile: CompanyHealthProfile,
    local_finding: MetricFinding,
    options: GenerateInsightOptions | None = None,
) -> ChartNarrative:
    """@deprecated Prefer generate_insight(section_id, chart_data, profile)."""
    return generate_insight(section_id, local_finding, profile, options)


def plan_insight(
    section_id: SectionId,
    profile: CompanyHealthProfile,
    options: ReasonOptions | None = None,
) -> InsightPlan | list[InsightPlan]:
    return reason_about_section(section_id, profile, options)


def validate_insight_structured(structured: StructuredInsight) -> list[str]:
    """Return a list of quality violations for one structured insight (empty = OK)."""
    errors: list[str] = []
    obs = (structured.observation or "").strip()
    why = (structured.explanation or "").strip()
    rec = (structured.recommendation or "").strip()

    if structured.section_id == "leadership":
        fields = [("observation", obs), ("explanation", why), ("recommendation", rec)]
    else:
        # Chart insights must have all three when data was sufficient.
        if obs == INSUFFICIENT_DATA_MESSAGE or not obs:
            return errors
        fields = [("observation", obs), ("explanation", why), ("recommendation", rec)]

    for name, text in fields:
        if not text:
            errors.append(f"{name} empty")
            continue
        if not (text[0].isupper() or text[0].isdigit()):
            errors.append(f"{name} must start with a capital letter")
        if text.endswith(","):
            errors.append(f"{name} must not end with a comma")
        if not re.search(r"[.!?]$", text):
            errors.append(f"{name} must end with sentence punctuation")
        if _is_clause(text):
            errors.append(f"{name} is a sentence fragment")

    if why and rec and _normalize_compare(why) == _normalize_compare(rec):
        errors.append("explanation equals recommendation")
    if obs and why and _normalize_compare(obs) == _normalize_compare(why):
        errors.append("observation equals explanation")
    if obs and rec and _normalize_compare(obs) == _normalize_compare(rec):
        errors.append("observation equals recommendation")

    if structured.effect_ids is not None:
        if len(structured.effect_ids) != len(set(structured.effect_ids)):
            errors.append("duplicate effect_ids")
    if structured.lever_ids is not None:
        if len(structured.lever_ids) != len(set(structured.lever_ids)):
            errors.append("duplicate lever_ids")

    if (
        structured.section_id in ("disease_risks", "disease_deep_dive")
        and structured.tone == "concern"
        and rec
    ):
        # Disease-first: should not be pure company-cluster replacement copy.
        if re.match(r"^Focus the \w+ prevention response\b", rec, re.I):
            errors.append("recommendation replaced by company-wide cluster template")

    return errors


def validate_report_insights(payload: dict) -> list[str]:
    """Validate a full generate_report_insights payload; return violation messages."""
    errors: list[str] = []
    concerns = payload.get("concerns") or {}

    def check_narrative(label: str, narrative: dict | None) -> None:
        if not narrative or not isinstance(narrative, dict):
            return
        structured = narrative.get("structured") or {}
        if not isinstance(structured, dict):
            return
        # Build a lightweight duck object
        class _S:
            pass

        s = _S()
        s.section_id = structured.get("section_id") or label
        s.observation = structured.get("observation") or ""
        s.explanation = structured.get("explanation") or ""
        s.recommendation = structured.get("recommendation") or ""
        s.tone = narrative.get("tone") or structured.get("tone")
        s.effect_ids = structured.get("effect_ids")
        s.lever_ids = structured.get("lever_ids")
        for err in validate_insight_structured(s):  # type: ignore[arg-type]
            errors.append(f"{label}: {err}")

    for key, value in concerns.items():
        if key in ("physical_activity", "sleep") and isinstance(value, dict) and "both" in value:
            for view, narr in value.items():
                check_narrative(f"{key}.{view}", narr)
        elif key == "disease_deep_dive" and isinstance(value, dict):
            for disease, narr in value.items():
                check_narrative(f"disease_deep_dive.{disease}", narr)
        elif isinstance(value, dict) and "text" in value:
            check_narrative(key, value)

    check_narrative("positives", payload.get("positives"))

    for i, card in enumerate(payload.get("leadership_cards") or []):
        structured = (card or {}).get("structured") or {}
        if not structured:
            continue

        class _S:
            pass

        s = _S()
        s.section_id = "leadership"
        s.observation = structured.get("observation") or ""
        s.explanation = structured.get("explanation") or ""
        s.recommendation = structured.get("recommendation") or ""
        s.tone = structured.get("tone")
        s.effect_ids = structured.get("effect_ids") or []
        s.lever_ids = structured.get("lever_ids") or []
        for err in validate_insight_structured(s):  # type: ignore[arg-type]
            errors.append(f"leadership[{i}]: {err}")
        if s.explanation and s.recommendation and _normalize_compare(s.explanation) == _normalize_compare(s.recommendation):
            errors.append(f"leadership[{i}]: structured explanation equals recommendation")

    return errors


__all__ = [
    "compose_company_profile",
    "rebuild_profile_with_finding",
    "build_profile_from_slices",
    "generate_insight",
    "generate_structured_insight",
    "generate_leadership_takeaways",
    "generate_positive_insights",
    "generate_insight_from_data",
    "generate_insight_from_slices",
    "generate_insight_with_local_finding",
    "plan_insight",
    "compose_insight",
    "reason_about_section",
    "ChartDataInput",
    "GenerateInsightOptions",
    "human_metric_name",
    "human_cluster_name",
    "sanitize_insight_text",
    "compose_footer_text",
    "generate_leadership_cards",
    "validate_insight_structured",
    "validate_report_insights",
    "INSUFFICIENT_DATA_MESSAGE",
]
