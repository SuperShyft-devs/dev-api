"""Unit tests for Metsights pull/push strategy transforms."""

from __future__ import annotations

from modules.metsights.strategies import apply_pull_strategy, pull_passthrough, pull_string_boolean


def test_pull_string_boolean_accepts_yes_no_labels():
    cfg = {"pull": {"enabled": True, "strategy": "string_boolean"}}
    assert apply_pull_strategy("iodized_salt_status", {"iodized_salt_status": "Yes"}, cfg) == "true"
    assert apply_pull_strategy("iodized_salt_status", {"iodized_salt_status": "No"}, cfg) == "false"
    assert apply_pull_strategy("iodized_salt_status", {"iodized_salt_status": "yes"}, cfg) == "true"
    assert apply_pull_strategy("iodized_salt_status", {"iodized_salt_status": True}, cfg) == "true"
    assert apply_pull_strategy("iodized_salt_status", {"iodized_salt_status": False}, cfg) == "false"
    assert apply_pull_strategy("iodized_salt_status", {"iodized_salt_status": "maybe"}, cfg) is None


def test_pull_string_boolean_direct():
    assert pull_string_boolean("iodized_salt_status", {"iodized_salt_status": "Y"}, {}) == "true"
    assert pull_string_boolean("iodized_salt_status", {"iodized_salt_status": "n"}, {}) == "false"


def test_pull_passthrough_empty_list_maps_to_none_for_disease_fields():
    cfg = {"pull": {"enabled": True, "strategy": "passthrough"}}
    assert apply_pull_strategy("diagnosed_diseases", {"diagnosed_diseases": []}, cfg) == ["none"]
    assert apply_pull_strategy("family_health_history", {"family_health_history": []}, cfg) == ["none"]
    assert apply_pull_strategy("diagnosed_diseases_medications", {"diagnosed_diseases_medications": []}, cfg) == [
        "none"
    ]
    # Unrelated empty lists still mean "no value"
    assert apply_pull_strategy("food_groups", {"food_groups": []}, cfg) is None
    assert pull_passthrough("food_groups", {"food_groups": []}, {}) is None


def test_pull_passthrough_respects_empty_list_as_param():
    cfg = {
        "pull": {
            "enabled": True,
            "strategy": "passthrough",
            "empty_list_as": ["none"],
        }
    }
    assert apply_pull_strategy("diagnosed_diseases", {"diagnosed_diseases": []}, cfg) == ["none"]
    cfg_custom = {
        "pull": {
            "enabled": True,
            "strategy": "passthrough",
            "empty_list_as": ["none_selected"],
        }
    }
    assert apply_pull_strategy("food_groups", {"food_groups": []}, cfg_custom) == ["none_selected"]


def test_pull_passthrough_preserves_populated_lists():
    cfg = {"pull": {"enabled": True, "strategy": "passthrough"}}
    assert apply_pull_strategy(
        "family_health_history",
        {"family_health_history": ["Type 2 diabetes"]},
        cfg,
    ) == ["Type 2 diabetes"]
