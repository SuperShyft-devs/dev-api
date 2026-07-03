"""Unit tests for Healthians blood parameter normalizer."""

from __future__ import annotations

from dataclasses import dataclass

from modules.reports.blood_parameters_normalizer import normalize_from_healthians
from modules.reports.blood_parameters_schemas import is_canonical_blood_parameters


@dataclass
class _CatalogRow:
    parameter_key: str | None
    healthians_parameter_id: int | None
    test_name: str | None = None


def test_normalize_from_healthians_builds_canonical_parameters():
    raw = {
        "customer_name": "Jane Doe",
        "customer_age": "32",
        "customer_gender": "F",
        "digital_data": [
            {
                "parameter_id": "1018",
                "value": "13.2",
                "unit": "g/dL",
                "min_range": "12",
                "max_range": "15",
                "test_name": "Hemoglobin Hb",
            }
        ],
    }
    catalog = [
        _CatalogRow(parameter_key="haemoglobin", healthians_parameter_id=1018, test_name="Hemoglobin Hb"),
    ]

    canonical, stored_raw = normalize_from_healthians(raw, catalog=catalog)

    assert is_canonical_blood_parameters(canonical)
    assert canonical["source"] == "healthians"
    assert canonical["customer"]["name"] == "Jane Doe"
    assert canonical["parameters"]["haemoglobin"]["value"] == 13.2
    assert canonical["parameters"]["haemoglobin"]["unit"] == "g/dL"
    assert canonical["parameters"]["haemoglobin"]["lower_range"] == 12.0
    assert stored_raw is raw


def test_normalize_maps_hemoglobin_alias_to_haemoglobin():
    raw = {
        "customer_name": "John",
        "digital_data": [
            {"parameter_id": "55", "value": "14.0", "unit": "g/dL", "min_range": "13", "max_range": "17"},
        ],
    }
    catalog = [_CatalogRow(parameter_key="hemoglobin", healthians_parameter_id=55)]

    canonical, _ = normalize_from_healthians(raw, catalog=catalog)

    assert "haemoglobin" in canonical["parameters"]
    assert "hemoglobin" not in canonical["parameters"]
