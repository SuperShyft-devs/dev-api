"""Tests for Healthians→Metsights parameter key aliases."""

from __future__ import annotations

from db.seed.blood_parameter_key_aliases import (
    HEALTHIANS_TO_METSIGHTS_PARAMETER_KEY,
    resolve_metsights_parameter_key,
)
from modules.reports.blood_parameters_read_service import build_parameter_value_map


def test_resolve_metsights_parameter_key_aliases():
    assert resolve_metsights_parameter_key("lh") == "lh_value"
    assert resolve_metsights_parameter_key("hemoglobin") == "haemoglobin"
    assert resolve_metsights_parameter_key("haemoglobin") == "haemoglobin"


def test_build_parameter_value_map_applies_aliases():
    grouped = [
        {
            "group_name": "Hormones",
            "test_count": 2,
            "tests": [
                {"parameter_key": "lh", "value": 4.43, "unit": "mlU/ml"},
                {"parameter_key": "fsh", "value": 4.91, "unit": "mIU/mL"},
                {"parameter_key": "hemoglobin", "value": 14.0, "unit": "g/dL"},
            ],
        }
    ]
    result = build_parameter_value_map(grouped)
    assert result["lh_value"] == (4.43, "mlU/ml")
    assert result["fsh_value"] == (4.91, "mIU/mL")
    assert result["haemoglobin"] == (14.0, "g/dL")
    assert "lh" not in result
    assert len(HEALTHIANS_TO_METSIGHTS_PARAMETER_KEY) >= 30
