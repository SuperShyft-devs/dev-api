"""Phase 10 tests: Final Nutrition Score."""

from __future__ import annotations

from dataclasses import replace

import pytest

from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.scoring import calculate_final_score


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


def test_standard_70_30_calculation(config):
    q = 80.0
    a = 40.0
    wq = config.scoring.general_quality_weight
    wa = config.scoring.goal_alignment_weight
    result = calculate_final_score(q, a, config=config)
    expected = wq * q + wa * a
    assert result.final_score == pytest.approx(expected)
    assert result.final_score == pytest.approx(0.70 * 80.0 + 0.30 * 40.0)
    assert result.general_quality == q
    assert result.goal_alignment == a


def test_configured_weights_read_from_config(config):
    assert config.scoring.general_quality_weight == pytest.approx(0.70)
    assert config.scoring.goal_alignment_weight == pytest.approx(0.30)
    result = calculate_final_score(100.0, 0.0, config=config)
    assert result.general_quality_weight_used == pytest.approx(
        config.scoring.general_quality_weight
    )
    assert result.goal_alignment_weight_used == pytest.approx(
        config.scoring.goal_alignment_weight
    )
    assert result.final_score == pytest.approx(70.0)


def test_changing_weights_changes_result(config):
    scoring = replace(
        config.scoring,
        general_quality_weight=0.50,
        goal_alignment_weight=0.50,
    )
    alt = replace(config, scoring=scoring)
    result_default = calculate_final_score(100.0, 0.0, config=config)
    result_alt = calculate_final_score(100.0, 0.0, config=alt)
    assert result_default.final_score == pytest.approx(70.0)
    assert result_alt.final_score == pytest.approx(50.0)
    assert result_default.final_score != result_alt.final_score


def test_zero_goals_alignment_none_equals_general_quality(config):
    assert config.scoring.zero_goal_mode == "renormalize_quality_only"
    q = 72.5
    result = calculate_final_score(q, None, config=config)
    assert result.final_score == pytest.approx(q)
    assert result.general_quality_weight_used == pytest.approx(1.0)
    assert result.goal_alignment_weight_used == pytest.approx(0.0)


def test_q_exists_a_none_no_artificial_penalty(config):
    q = 90.0
    result = calculate_final_score(q, None, config=config)
    # Must not apply 0.70 × Q (would be 63).
    assert result.final_score == pytest.approx(90.0)
    assert result.final_score != pytest.approx(0.70 * 90.0)


def test_q_none_a_exists_renormalizes_to_alignment(config):
    a = 55.0
    result = calculate_final_score(None, a, config=config)
    assert result.final_score == pytest.approx(55.0)
    assert result.final_score != pytest.approx(0.30 * 55.0)
    assert result.general_quality_weight_used == pytest.approx(0.0)
    assert result.goal_alignment_weight_used == pytest.approx(1.0)


def test_q_none_and_a_none_returns_none(config):
    result = calculate_final_score(None, None, config=config)
    assert result.final_score is None
    assert result.general_quality_weight_used == 0.0
    assert result.goal_alignment_weight_used == 0.0


def test_none_never_treated_as_zero(config):
    # If A=None were treated as 0 with 70/30 → 56; must be full Q.
    result = calculate_final_score(80.0, None, config=config)
    assert result.final_score == pytest.approx(80.0)
    assert result.final_score != pytest.approx(0.70 * 80.0 + 0.30 * 0.0)

    # If Q=None were treated as 0 with 70/30 → 24; must be full A.
    result_a = calculate_final_score(None, 80.0, config=config)
    assert result_a.final_score == pytest.approx(80.0)
    assert result_a.final_score != pytest.approx(0.70 * 0.0 + 0.30 * 80.0)


def test_score_between_0_and_100(config):
    cases = [
        (0.0, 0.0),
        (100.0, 100.0),
        (12.3, 98.7),
        (100.0, 0.0),
        (0.0, 100.0),
    ]
    for q, a in cases:
        result = calculate_final_score(q, a, config=config)
        assert result.final_score is not None
        assert 0.0 <= result.final_score <= 100.0


def test_no_intermediate_rounding(config):
    q = 1.0 / 3.0 * 100.0  # 33.333...
    a = 2.0 / 3.0 * 100.0  # 66.666...
    result = calculate_final_score(q, a, config=config)
    wq = config.scoring.general_quality_weight
    wa = config.scoring.goal_alignment_weight
    expected = wq * q + wa * a
    assert result.final_score == expected  # exact float equality of same expression


def test_final_score_does_not_use_targets_or_questionnaire(config):
    result = calculate_final_score(70.0, 50.0, config=config)
    assert not hasattr(result, "protein_g_per_kg")
    assert not hasattr(result, "targets")
    assert not hasattr(result, "health_priority_codes")
    assert not hasattr(result, "NormalizedAnswers")
    # Signature is Q/A only — no questionnaire kwargs accepted.
    with pytest.raises(TypeError):
        calculate_final_score(70.0, 50.0, config=config, answers=None)  # type: ignore[call-arg]


def test_higher_component_raises_final_when_other_held_constant(config):
    base_q = calculate_final_score(50.0, 50.0, config=config)
    higher_q = calculate_final_score(80.0, 50.0, config=config)
    higher_a = calculate_final_score(50.0, 80.0, config=config)
    assert higher_q.final_score > base_q.final_score
    assert higher_a.final_score > base_q.final_score


def test_use_neutral_alignment_mode(config):
    scoring = replace(config.scoring, zero_goal_mode="use_neutral_alignment")
    alt = replace(config, scoring=scoring)
    q = 80.0
    result = calculate_final_score(q, None, config=alt)
    neutral = alt.scoring.neutral_alignment_score
    expected = (
        alt.scoring.general_quality_weight * q
        + alt.scoring.goal_alignment_weight * neutral
    )
    assert result.final_score == pytest.approx(expected)
    # Original input A remains None in the result echo.
    assert result.goal_alignment is None


def test_echoes_input_components(config):
    result = calculate_final_score(61.0, 44.0, config=config)
    assert result.general_quality == 61.0
    assert result.goal_alignment == 44.0


def test_repeated_evaluation_is_deterministic(config):
    assert calculate_final_score(77.0, 33.0, config=config) == calculate_final_score(
        77.0, 33.0, config=config
    )
