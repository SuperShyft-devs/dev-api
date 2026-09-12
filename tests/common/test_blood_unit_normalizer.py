"""Tests for blood unit normalization."""

from __future__ import annotations

from types import SimpleNamespace

from common.blood_unit_normalizer import (
    fingerprint_unit_label,
    map_unit_to_metsights_option_value,
    normalize_blood_unit_code_for_push,
    units_equivalent,
)


def _opt(value: str, label: str) -> SimpleNamespace:
    return SimpleNamespace(option_value=value, display_name=label)


def test_fingerprint_fixes_healthians_mlU_typo():
    assert fingerprint_unit_label("mlU/ml") == fingerprint_unit_label("mIU/mL")


def test_units_equivalent_esr_and_egfr():
    assert units_equivalent("mm/1st hour", "mm/hr")
    assert units_equivalent("mL/min/1.73m2", "ml/min/1.73 m²")


def test_map_unit_to_option_value_miu_ml():
    options = [
        _opt("0", "mIU/L"),
        _opt("1", "IU/L"),
        _opt("3", "mIU/mL"),
    ]
    assert map_unit_to_metsights_option_value("mIU/mL", options) == "3"
    assert map_unit_to_metsights_option_value("mlU/ml", options) == "3"


def test_map_unit_to_option_value_testosterone_ng_ml():
    options = [_opt("0", "ng/dl"), _opt("2", "ng/mL")]
    assert map_unit_to_metsights_option_value("ng/mL", options) == "2"


def test_normalize_blood_unit_code_for_push_labels():
    assert normalize_blood_unit_code_for_push("mg/dL") == "0"
    assert normalize_blood_unit_code_for_push("IU/L") == "1"
    assert normalize_blood_unit_code_for_push("3") == "3"
