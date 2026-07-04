"""Unit tests for Healthians blood parameter normalizer."""

from __future__ import annotations

from dataclasses import dataclass

from modules.reports.blood_parameters_normalizer import build_grouped_from_healthians
from modules.reports.blood_parameters_schemas import (
    has_usable_provider_blood_parameters,
    is_grouped_blood_parameters,
)


@dataclass
class _TestRow:
    test_id: int
    parameter_type: str
    test_name: str
    parameter_key: str | None
    unit: str | None
    external_parameter_id: int | None


@dataclass
class _GroupRow:
    group_name: str
    tests: list[_TestRow]


def test_build_grouped_from_healthians_matches_package_structure():
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
    package_groups = [
        _GroupRow(
            group_name="CBC",
            tests=[
                _TestRow(
                    test_id=12,
                    parameter_type="test",
                    test_name="Haemoglobin",
                    parameter_key="haemoglobin",
                    unit="g/dL",
                    external_parameter_id=1018,
                )
            ],
        )
    ]

    grouped, stored_raw = build_grouped_from_healthians(raw, package_groups=package_groups)

    assert is_grouped_blood_parameters(grouped)
    assert has_usable_provider_blood_parameters(grouped)
    assert stored_raw is raw
    assert grouped[0]["group_name"] == "CBC"
    assert grouped[0]["test_count"] == 1
    test = grouped[0]["tests"][0]
    assert test["test_id"] == 12
    assert test["parameter_type"] == "test"
    assert test["test_name"] == "Haemoglobin"
    assert test["parameter_key"] == "haemoglobin"
    assert test["value"] == 13.2
    assert test["unit"] == "g/dL"
    assert test["lower_range"] == 12.0
    assert test["higher_range"] == 15.0
    assert test["provider_test_name"] == "Hemoglobin Hb"
    assert "external_parameter_id" not in test
    assert "healthians_parameter_id" not in test


def test_build_grouped_includes_unmatched_tests_with_null_values():
    raw = {"digital_data": []}
    package_groups = [
        _GroupRow(
            group_name="CBC",
            tests=[
                _TestRow(
                    test_id=1,
                    parameter_type="test",
                    test_name="Haemoglobin",
                    parameter_key="haemoglobin",
                    unit="g/dL",
                    external_parameter_id=55,
                )
            ],
        )
    ]

    grouped, _ = build_grouped_from_healthians(raw, package_groups=package_groups)

    assert is_grouped_blood_parameters(grouped)
    assert not has_usable_provider_blood_parameters(grouped)
    assert grouped[0]["tests"][0]["value"] is None
