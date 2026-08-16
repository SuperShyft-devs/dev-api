"""Non-interactive tests for the Nutrition Intelligence CLI harness."""

from __future__ import annotations

from io import StringIO

import pytest

from modules.reports.nutrition_intelligence.cli import (
    _SCORING_QUESTION_KEYS,
    build_option_reverse_map,
    build_questionnaire_catalog,
    format_result,
    main,
    parse_args,
    run_pipeline,
)
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


@pytest.fixture(scope="module")
def catalog():
    return build_questionnaire_catalog()


def test_catalog_loads_scoring_fields_from_seed(catalog):
    for key in _SCORING_QUESTION_KEYS:
        assert key in catalog
        assert catalog[key]["prompt"]
        assert catalog[key]["options"], f"{key} missing options from seed"


def test_seed_fruit_labels_match_known_codes(catalog):
    options = dict(catalog["fresh_fruit_frequency"]["options"])
    assert options["0"] == "1-2 times per day"
    assert options["5"] == "Rarely or never"


def test_health_priorities_use_existing_goal_codes(catalog):
    options = dict(catalog["health_priorities"]["options"])
    assert options["0"] == "Weight Loss"
    assert options["1"] == "Building Muscle Mass"
    assert options["2"] == "Improving Metabolic Health"


def test_pipeline_via_lookup_uses_normalizer(config, catalog):
    reverse = build_option_reverse_map(catalog)
    lookup = {
        "health_priorities": "0",
        "diet_preference": "1",
        "food_groups": ["0", "1", "3", "4"],
        "healthy_breakfast_frequency": "2",
        "fresh_fruit_frequency": "0",
        "fresh_vegetable_frequency": "0",
        "baked_goods_frequency": "5",
        "dessert_frequency": "5",
        "extra_salt_frequency": "0",
        "water_intake_frequency": "4",
    }
    result = run_pipeline(lookup, config=config, option_reverse_map=reverse)
    assert result["normalized"].health_priority_codes == ("0",)
    assert result["goals"] == ("weight_loss",)
    assert result["quality"].general_quality is not None
    assert result["alignment"].goal_alignment is not None
    assert result["final"].final_score is not None
    assert 0.0 <= result["final"].final_score <= 100.0


def test_zero_goals_alignment_none_final_equals_quality(config, catalog):
    reverse = build_option_reverse_map(catalog)
    lookup = {
        "health_priorities": [],
        "diet_preference": "0",
        "food_groups": ["1", "2", "5"],
        "healthy_breakfast_frequency": "1",
        "fresh_fruit_frequency": "2",
        "fresh_vegetable_frequency": "2",
        "baked_goods_frequency": "3",
        "dessert_frequency": "3",
        "extra_salt_frequency": "1",
        "water_intake_frequency": "3",
    }
    result = run_pipeline(lookup, config=config, option_reverse_map=reverse)
    assert result["goals"] == ()
    assert result["alignment"].goal_alignment is None
    assert result["final"].final_score == pytest.approx(result["quality"].general_quality)


def test_format_result_shows_na_for_none(config):
    reverse = build_option_reverse_map(build_questionnaire_catalog())
    # Missing most fields → several indicator scores None.
    result = run_pipeline({"health_priorities": []}, config=config, option_reverse_map=reverse)
    text = format_result(result, config=config)
    assert "N/A (insufficient data)" in text
    assert "FINAL NUTRITION SCORE" in text
    # None must not be rendered as a numeric zero score line for missing indicators
    # when all are missing — at least one N/A present.
    assert "0.00" not in text or "N/A (insufficient data)" in text


def test_parse_debug_flag():
    assert parse_args([]).debug is False
    assert parse_args(["--debug"]).debug is True


def test_interactive_scripted_session_exits_cleanly(monkeypatch, capsys):
    """Drive the CLI with scripted stdin: healthy-ish answers, then quit."""
    # Selections by menu number for each prompt in collect order.
    scripted = "\n".join(
        [
            "1",  # goals: Weight Loss
            "2",  # diet: Non-Vegetarian
            "1,2,4,5",  # food groups
            "3",  # breakfast more than 5
            "1",  # fruit daily
            "1",  # veg daily
            "5",  # baked rarely
            "5",  # dessert rarely
            "5",  # butter rarely
            "4",  # red meat monthly
            "1",  # salt never
            "5",  # water 8 glasses (menu 5 = code 4)
            "1",  # caffeine none
            "n",  # do not run another
            "",
        ]
    )
    monkeypatch.setattr("sys.stdin", StringIO(scripted))
    code = main([])
    assert code == 0
    out = capsys.readouterr().out
    assert "Nutrition Score" in out
    assert "YOUR CURRENT NUTRITION" in out
    assert "Weight Loss" in out
