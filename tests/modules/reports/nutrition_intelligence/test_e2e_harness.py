"""Temporary end-to-end harness for Nutrition Intelligence Engine (Phases 1–10).

Runs the real pipeline only — no duplicated scoring logic.
Not production integration; not Phase 11.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from modules.reports.nutrition_intelligence.scoring import calculate_goal_alignment
from modules.reports.nutrition_intelligence.behaviour import evaluate_behaviour_indicators
from modules.reports.nutrition_intelligence.goals import combine_goal_profiles
from modules.reports.nutrition_intelligence.config_loader import (
    NutritionEngineConfig,
    load_nutrition_engine_config,
)
from modules.reports.nutrition_intelligence.goals import load_goal_profiles
from modules.reports.nutrition_intelligence.goals import resolve_goals
from modules.reports.nutrition_intelligence.models import (
    IndicatorScore,
    NormalizedAnswers,
)
from modules.reports.nutrition_intelligence.scoring import calculate_general_quality
from modules.reports.nutrition_intelligence.scoring import calculate_final_score

# Stable report order (matches product indicator naming in YAML).
_INDICATOR_ORDER = (
    "fruit_intake",
    "vegetable_intake",
    "food_diversity",
    "protein_supporting_foods",
    "meal_regularity",
    "baked_goods_control",
    "dessert_sugar_control",
    "hydration",
    "sodium_control",
)

_GOAL_COMPARE_CODES = (
    ("weight_loss", "0"),
    ("muscle_gain", "1"),
    ("metabolic_health", "2"),
)


@pytest.fixture(scope="module")
def config() -> NutritionEngineConfig:
    return load_nutrition_engine_config()


def _answers(**kwargs: Any) -> NormalizedAnswers:
    base: dict[str, Any] = {"health_priority_codes": ()}
    base.update(kwargs)
    return NormalizedAnswers(**base)


# ---------------------------------------------------------------------------
# Representative questionnaire snapshots
# ---------------------------------------------------------------------------


def fixture_healthy_user() -> NormalizedAnswers:
    """High-quality nutrition behaviour; single metabolic_health goal."""
    return _answers(
        health_priority_codes=("2",),  # metabolic_health
        diet_preference="1",  # non-veg
        fresh_fruit_frequency="0",  # daily
        fresh_vegetable_frequency="0",
        food_groups=("0", "1", "2", "3", "4", "5", "6", "7", "9"),
        healthy_breakfast_frequency="2",
        baked_goods_frequency="5",
        dessert_frequency="5",
        water_intake_frequency="4",  # 8 glasses
        extra_salt_frequency="0",
        # Activity / body fields present but must not affect scoring phases 7–10.
        exercise_frequency_week="3",
        exercise_level="2",
        gender="1",
        weight=None,
        height=None,
        sickness_frequency="0",
    )


def fixture_poor_nutrition_user() -> NormalizedAnswers:
    """Poor nutrition behaviour; weight_loss goal."""
    return _answers(
        health_priority_codes=("0",),  # weight_loss
        diet_preference="1",
        fresh_fruit_frequency="5",  # rarely
        fresh_vegetable_frequency="5",
        food_groups=("8",),  # non-quality group only if present; else empty-ish
        healthy_breakfast_frequency="0",
        baked_goods_frequency="1",
        dessert_frequency="1",
        water_intake_frequency="0",
        extra_salt_frequency="2",
    )


def fixture_mixed_user(*, goal_codes: tuple[str, ...] = ("0",)) -> NormalizedAnswers:
    """Mixed behaviours: strong protein/hydration, weak fruit/veg/sugar control."""
    return _answers(
        health_priority_codes=goal_codes,
        diet_preference="1",
        fresh_fruit_frequency="5",  # poor
        fresh_vegetable_frequency="3",  # mid-poor
        food_groups=("1", "2", "5", "6", "7"),  # protein-heavy, low produce diversity
        healthy_breakfast_frequency="2",  # good
        baked_goods_frequency="2",  # poor-ish
        dessert_frequency="2",
        water_intake_frequency="4",  # good
        extra_salt_frequency="1",  # mid
    )


def fixture_no_goal_user() -> NormalizedAnswers:
    """Same behavioural answers as MIXED_USER, zero health priorities."""
    return fixture_mixed_user(goal_codes=())


def fixture_two_goal_user() -> NormalizedAnswers:
    """Realistic two-goal user: muscle_gain + endurance."""
    return _answers(
        health_priority_codes=("1", "5"),  # muscle_gain, endurance
        diet_preference="1",
        fresh_fruit_frequency="2",
        fresh_vegetable_frequency="2",
        food_groups=("0", "1", "2", "5", "6", "7"),
        healthy_breakfast_frequency="1",
        baked_goods_frequency="3",
        dessert_frequency="4",
        water_intake_frequency="3",
        extra_salt_frequency="1",
        exercise_frequency_week="4",
        exercise_level="2",
        physical_activity_frequency="2",
        daily_active_duration="3",
    )


# ---------------------------------------------------------------------------
# Pipeline (existing engine only)
# ---------------------------------------------------------------------------


def run_engine(answers: NormalizedAnswers, *, config: NutritionEngineConfig) -> dict[str, Any]:
    """Execute Phases 4–10 using existing public functions only."""
    goals = resolve_goals(answers)
    profiles = load_goal_profiles(goals, config=config)
    combined = combine_goal_profiles(profiles, config=config)
    indicators = evaluate_behaviour_indicators(answers, config=config)
    quality = calculate_general_quality(indicators, config=config)
    alignment = calculate_goal_alignment(indicators, combined, config=config)
    final = calculate_final_score(
        quality.general_quality,
        alignment.goal_alignment,
        config=config,
    )
    return {
        "goals": tuple(goals),
        "combined_goals": combined.goals,
        "indicators": indicators,
        "general_quality": quality.general_quality,
        "goal_alignment": alignment.goal_alignment,
        "final_score": final.final_score,
        "quality_result": quality,
        "alignment_result": alignment,
        "final_result": final,
        "combined": combined,
    }


def _fmt(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "None"
    return f"{value:.{digits}f}"


def format_scenario_report(name: str, result: dict[str, Any]) -> str:
    lines = [
        "=" * 64,
        f"User / Scenario: {name}",
        "=" * 64,
        "Goals:",
    ]
    if result["goals"]:
        for goal in result["goals"]:
            lines.append(f"  - {goal}")
    else:
        lines.append("  - (none)")

    lines.append("")
    lines.append("Behaviour Indicators:")
    indicators: dict[str, IndicatorScore] = result["indicators"]
    for indicator_id in _INDICATOR_ORDER:
        item = indicators.get(indicator_id)
        score = None if item is None else item.score
        lines.append(f"  - {indicator_id}: {_fmt(score)}")
    # Any unexpected extras
    for indicator_id in sorted(indicators):
        if indicator_id not in _INDICATOR_ORDER:
            lines.append(f"  - {indicator_id}: {_fmt(indicators[indicator_id].score)}")

    lines.extend(
        [
            "",
            f"General Quality: {_fmt(result['general_quality'])}",
            f"Goal Alignment: {_fmt(result['goal_alignment'])}",
            f"Final Nutrition Score: {_fmt(result['final_score'])}",
            "",
        ]
    )
    return "\n".join(lines)


def format_goal_comparison(rows: dict[str, dict[str, Any]]) -> str:
    labels = [label for label, _ in _GOAL_COMPARE_CODES]
    width = max(14, max(len(label) for label in labels) + 2)
    header = f"{'':18}" + "".join(f"{label:>{width}}" for label in labels)
    sep = "-" * len(header)
    lines = [
        "",
        "Same behaviour under different goals",
        header,
        sep,
    ]
    for metric, key in (
        ("General Quality", "general_quality"),
        ("Goal Alignment", "goal_alignment"),
        ("Final Score", "final_score"),
    ):
        cells = "".join(f"{_fmt(rows[label][key]):>{width}}" for label in labels)
        lines.append(f"{metric:18}{cells}")
    lines.append("")
    return "\n".join(lines)


def assert_score_bounds(value: float | None, *, label: str) -> None:
    if value is None:
        return
    assert 0.0 <= value <= 100.0, f"{label} out of range: {value}"


def assert_pipeline_invariants(result: dict[str, Any]) -> None:
    for indicator_id, item in result["indicators"].items():
        assert isinstance(item, IndicatorScore)
        assert_score_bounds(item.score, label=indicator_id)
    assert_score_bounds(result["general_quality"], label="general_quality")
    assert_score_bounds(result["goal_alignment"], label="goal_alignment")
    assert_score_bounds(result["final_score"], label="final_score")
    # Scoring path must not attach target adherence fields.
    assert not hasattr(result["final_result"], "protein_g_per_kg")
    assert not hasattr(result["final_result"], "targets")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_e2e_healthy_user(config, capsys):
    answers = fixture_healthy_user()
    result = run_engine(answers, config=config)
    assert_pipeline_invariants(result)
    assert result["goals"] == ("metabolic_health",)
    assert result["general_quality"] is not None
    assert result["goal_alignment"] is not None
    assert result["final_score"] is not None
    # Healthy profile should land clearly above mid-scale.
    assert result["general_quality"] >= 80.0
    assert result["final_score"] >= 75.0
    print(format_scenario_report("HEALTHY_USER", result))


def test_e2e_poor_nutrition_user(config, capsys):
    answers = fixture_poor_nutrition_user()
    result = run_engine(answers, config=config)
    assert_pipeline_invariants(result)
    assert result["goals"] == ("weight_loss",)
    assert result["general_quality"] is not None
    assert result["final_score"] is not None
    # Poor profile should land clearly below mid-scale.
    assert result["general_quality"] <= 40.0
    assert result["final_score"] <= 45.0
    print(format_scenario_report("POOR_NUTRITION_USER", result))


def test_e2e_mixed_user(config, capsys):
    answers = fixture_mixed_user(goal_codes=("0",))
    result = run_engine(answers, config=config)
    assert_pipeline_invariants(result)
    assert result["goals"] == ("weight_loss",)
    assert result["general_quality"] is not None
    print(format_scenario_report("MIXED_USER", result))


def test_e2e_no_goal_user(config, capsys):
    answers = fixture_no_goal_user()
    result = run_engine(answers, config=config)
    assert_pipeline_invariants(result)

    assert result["goals"] == ()
    assert result["goal_alignment"] is None
    assert result["general_quality"] is not None
    assert result["final_score"] == pytest.approx(result["general_quality"])
    # Must not treat missing alignment as zero under 70/30.
    assert result["final_score"] != pytest.approx(
        0.70 * float(result["general_quality"]) + 0.30 * 0.0
    )
    print(format_scenario_report("NO_GOAL_USER", result))


def test_e2e_two_goal_user(config, capsys):
    answers = fixture_two_goal_user()
    result = run_engine(answers, config=config)
    assert_pipeline_invariants(result)
    assert result["goals"] == ("muscle_gain", "endurance")
    assert result["goal_alignment"] is not None
    assert result["final_score"] is not None
    print(format_scenario_report("TWO_GOAL_USER", result))


def test_e2e_same_behaviour_different_goals(config, capsys):
    """Critical: goals must not mutate indicator scores or General Quality."""
    base = fixture_mixed_user(goal_codes=())
    rows: dict[str, dict[str, Any]] = {}
    indicator_snapshots: dict[str, dict[str, float | None]] = {}

    for label, code in _GOAL_COMPARE_CODES:
        answers = replace(base, health_priority_codes=(code,))
        result = run_engine(answers, config=config)
        assert_pipeline_invariants(result)
        rows[label] = result
        indicator_snapshots[label] = {
            iid: item.score for iid, item in result["indicators"].items()
        }
        print(format_scenario_report(f"GOAL_COMPARE / {label}", result))

    # Indicator scores identical across goals.
    ref = indicator_snapshots["weight_loss"]
    for label in ("muscle_gain", "metabolic_health"):
        assert indicator_snapshots[label] == ref

    # General Quality identical.
    q_ref = rows["weight_loss"]["general_quality"]
    assert rows["muscle_gain"]["general_quality"] == pytest.approx(q_ref)
    assert rows["metabolic_health"]["general_quality"] == pytest.approx(q_ref)

    # Alignment / final may differ because priorities differ.
    alignments = {label: rows[label]["goal_alignment"] for label in rows}
    assert all(v is not None for v in alignments.values())
    # At least one pair differs for this mixed profile (protein-heavy, weak produce).
    assert len(set(alignments.values())) >= 2

    print(format_goal_comparison(rows))

    # Goal selection must not itself increase any indicator score vs another goal.
    for iid in ref:
        scores = [indicator_snapshots[label][iid] for label in indicator_snapshots]
        assert len(set(scores)) == 1


def test_e2e_goal_selection_does_not_mutate_behaviour_objects(config):
    answers = fixture_mixed_user(goal_codes=("1",))
    before = deepcopy(answers)
    run_engine(answers, config=config)
    assert answers == before


def test_e2e_full_suite_readable_dump(config, capsys):
    """Single entry that prints every scenario for manual review (`pytest -s`)."""
    scenarios = [
        ("HEALTHY_USER", fixture_healthy_user()),
        ("POOR_NUTRITION_USER", fixture_poor_nutrition_user()),
        ("MIXED_USER", fixture_mixed_user(goal_codes=("0",))),
        ("NO_GOAL_USER", fixture_no_goal_user()),
        ("TWO_GOAL_USER", fixture_two_goal_user()),
    ]
    for name, answers in scenarios:
        result = run_engine(answers, config=config)
        assert_pipeline_invariants(result)
        print(format_scenario_report(name, result))

    base = fixture_mixed_user(goal_codes=())
    rows: dict[str, dict[str, Any]] = {}
    for label, code in _GOAL_COMPARE_CODES:
        result = run_engine(replace(base, health_priority_codes=(code,)), config=config)
        rows[label] = result
    print(format_goal_comparison(rows))

    # Zero-goal invariant restated in the dump path.
    no_goal = run_engine(fixture_no_goal_user(), config=config)
    assert no_goal["goal_alignment"] is None
    assert no_goal["final_score"] == pytest.approx(no_goal["general_quality"])
