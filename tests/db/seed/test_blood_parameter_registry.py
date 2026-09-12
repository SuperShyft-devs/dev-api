"""Tests for blood parameter registry required flags and placeholders."""

from __future__ import annotations

from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_FIELDS,
    BLOOD_PARAMETER_FIELDS,
    BLOOD_PARAMETER_INTERNAL_FALLBACKS,
    FIELD_BY_KEY,
    PRO_FEMALE_HORMONE_PLACEHOLDERS,
)


def test_hormones_not_required_on_basic_blood_category():
    for key in ("lh_value", "fsh_value", "testosterone"):
        field = FIELD_BY_KEY[key]
        assert field.required is False


def test_hormones_required_on_advanced_category():
    advanced_by_key = {f.question_key: f for f in ADVANCED_BLOOD_PARAMETER_FIELDS}
    for key in ("lh_value", "fsh_value", "testosterone"):
        assert advanced_by_key[key].required is True


def test_hormone_placeholders_use_metsights_suggested_units():
    assert PRO_FEMALE_HORMONE_PLACEHOLDERS["lh_value"] == (5.0, "1")
    assert PRO_FEMALE_HORMONE_PLACEHOLDERS["fsh_value"] == (5.0, "1")
    assert PRO_FEMALE_HORMONE_PLACEHOLDERS["testosterone"] == (0.5, "2")
    assert "lh_value" not in BLOOD_PARAMETER_INTERNAL_FALLBACKS
    assert "testosterone" not in BLOOD_PARAMETER_INTERNAL_FALLBACKS
