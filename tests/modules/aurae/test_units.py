"""Unit tests for Aurae anthropometry unit converters."""

from __future__ import annotations

import pytest

from modules.aurae.units import (
    extract_scale_answer,
    height_to_cm,
    normalize_aurae_gender,
    waist_to_inches,
    weight_to_kg,
)


def test_extract_scale_answer():
    assert extract_scale_answer({"value": 170, "unit": "0"}) == (170.0, "0")
    assert extract_scale_answer(None) == (None, None)
    assert extract_scale_answer("bad") == (None, None)


def test_height_to_cm_from_cm():
    assert height_to_cm(175.4, "0") == 175
    assert height_to_cm(170, "cm") == 170


def test_height_to_cm_from_ft_in():
    # 5.10 / 5.1 → 5′10″ → 177.8 cm ≈ 178
    assert height_to_cm(5.1, "2") == 178
    # 5.9 → 5′9″ → 175.26 ≈ 175
    assert height_to_cm(5.9, "2") == 175
    assert height_to_cm(6.0, "ft/in") == 183


def test_weight_to_kg():
    assert weight_to_kg(70.4, "0") == 70
    assert weight_to_kg(220, "1") == 100  # 220 lb ≈ 99.79 → 100


def test_waist_to_inches():
    assert waist_to_inches(32, "1") == 32
    assert waist_to_inches(81.28, "0") == 32  # 81.28 cm / 2.54


def test_normalize_aurae_gender():
    assert normalize_aurae_gender("male") == "male"
    assert normalize_aurae_gender("Female") == "female"
    assert normalize_aurae_gender("1") == "male"
    assert normalize_aurae_gender("2") == "female"
    assert normalize_aurae_gender("m") == "male"
    assert normalize_aurae_gender(None) is None
    assert normalize_aurae_gender("") is None


def test_unsupported_units_raise():
    with pytest.raises(ValueError):
        height_to_cm(170, "yards")
    with pytest.raises(ValueError):
        weight_to_kg(70, "stone")
    with pytest.raises(ValueError):
        waist_to_inches(32, "mm")
