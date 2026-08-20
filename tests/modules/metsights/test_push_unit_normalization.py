"""Unit tests for Metsights push unit/label normalization."""

from __future__ import annotations

from modules.metsights.strategies import normalize_metsights_unit_code, push_scale_emit
from modules.metsights.sync_service import (
    _FieldMeta,
    _extract_field_metadata_from_options,
    _validate_payload_against_options,
)


def test_normalize_metsights_unit_code_maps_labels():
    assert normalize_metsights_unit_code("ft/in") == "2"
    assert normalize_metsights_unit_code("kg") == "0"
    assert normalize_metsights_unit_code("in") == "1"
    assert normalize_metsights_unit_code("cm") == "0"
    assert normalize_metsights_unit_code("lb") == "1"
    assert normalize_metsights_unit_code("2") == "2"
    assert normalize_metsights_unit_code("0") == "0"


def test_push_scale_emit_normalizes_unit_labels():
    assert push_scale_emit("height", {"value": 5.166666666666667, "unit": "ft/in"}, {}) == {
        "height": 5.166666666666667,
        "height_unit": "2",
    }
    assert push_scale_emit("weight", {"value": 69.0, "unit": "kg"}, {}) == {
        "weight": 69.0,
        "weight_unit": "0",
    }
    assert push_scale_emit("waist_circumference", {"value": 30.0, "unit": "in"}, {}) == {
        "waist_circumference": 30.0,
        "waist_circumference_unit": "1",
    }


def test_validate_payload_resolves_choice_labels_from_options():
    envelope = {
        "actions": {
            "POST": {
                "diet_preference": {
                    "required": True,
                    "choices": [
                        {"value": "0", "label": "Vegetarian"},
                        {"value": "1", "label": "Non-Vegetarian"},
                        {"value": "2", "label": "Eggetarian"},
                    ],
                },
                "height_unit": {
                    "required": False,
                    "choices": [
                        {"value": "0", "label": "cm"},
                        {"value": "2", "label": "ft/in"},
                    ],
                },
            }
        }
    }
    meta = _extract_field_metadata_from_options(envelope)
    cleaned = _validate_payload_against_options(
        {
            "diet_preference": "Non-Vegetarian",
            "height_unit": "ft/in",
            "height": 5.16,
        },
        meta,
    )
    assert cleaned["diet_preference"] == "1"
    assert cleaned["height_unit"] == "2"
    assert cleaned["height"] == 5.16


def test_validate_payload_does_not_blindly_remap_labels_to_zero():
    meta = {
        "diet_preference": _FieldMeta(valid_choices={"0", "1", "2"}, required=True),
    }
    cleaned = _validate_payload_against_options({"diet_preference": "Non-Vegetarian"}, meta)
    assert "diet_preference" not in cleaned


def test_validate_payload_keeps_required_scale_fields_without_choices():
    """Metsights OPTIONS often marks height/weight required but omits a choices list."""
    meta = {
        "height": _FieldMeta(valid_choices=set(), required=True),
        "weight": _FieldMeta(valid_choices=set(), required=True),
        "waist_circumference": _FieldMeta(valid_choices=set(), required=True),
    }
    payload = {
        "height": 170.0,
        "height_unit": "0",
        "weight": 70.0,
        "weight_unit": "0",
        "waist_circumference": 80.0,
        "waist_circumference_unit": "0",
    }
    cleaned = _validate_payload_against_options(payload, meta)
    assert cleaned == payload


def test_validate_measurement_ranges_rejects_tiny_weight():
    from modules.metsights.anthropometry_validation import validate_metsights_payload_ranges
    from core.exceptions import AppError
    import pytest

    with pytest.raises(AppError) as ei:
        validate_metsights_payload_ranges({"weight": 5.0, "weight_unit": "0"})
    assert ei.value.error_code == "INVALID_INPUT"
    assert "20" in ei.value.message


def test_validate_measurement_ranges_allows_normal_weight():
    from modules.metsights.anthropometry_validation import validate_metsights_payload_ranges

    validate_metsights_payload_ranges({"weight": 70.0, "weight_unit": "0", "height": 185.0, "height_unit": "0"})


def test_normalize_hip_inches_to_cm():
    from modules.metsights.anthropometry_validation import normalize_anthropometry_for_metsights

    out = normalize_anthropometry_for_metsights(
        {"hip_circumference": 21.0, "hip_circumference_unit": "1"}
    )
    assert out["hip_circumference"] == 53.34
    assert out["hip_circumference_unit"] == "0"


def test_drop_invalid_optional_hip():
    from modules.metsights.anthropometry_validation import drop_invalid_optional_hip

    kept = drop_invalid_optional_hip({"hip_circumference": 21.0, "hip_circumference_unit": "1"})
    assert kept["hip_circumference"] == 21.0

    dropped = drop_invalid_optional_hip({"hip_circumference": 10.0, "hip_circumference_unit": "0"})
    assert "hip_circumference" not in dropped


def test_validate_measurement_ranges_rejects_small_waist_inches():
    from modules.metsights.anthropometry_validation import validate_scale_answer
    from core.exceptions import AppError
    import pytest

    with pytest.raises(AppError) as ei:
        validate_scale_answer("waist_circumference", {"value": 21.0, "unit": "1"})
    assert ei.value.error_code == "INVALID_INPUT"
    assert "Waist" in ei.value.message


def test_prepare_anthropometry_payload_converts_hip_and_validates_waist():
    from modules.metsights.anthropometry_validation import prepare_anthropometry_payload
    from core.exceptions import AppError
    import pytest

    with pytest.raises(AppError):
        prepare_anthropometry_payload(
            {
                "waist_circumference": 21.0,
                "waist_circumference_unit": "1",
                "hip_circumference": 21.0,
                "hip_circumference_unit": "1",
            }
        )

    prepared = prepare_anthropometry_payload(
        {
            "waist_circumference": 30.0,
            "waist_circumference_unit": "1",
            "hip_circumference": 21.0,
            "hip_circumference_unit": "1",
        }
    )
    assert prepared["hip_circumference"] == 53.34
    assert prepared["waist_circumference"] == 30.0
