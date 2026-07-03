"""Tests for blood_parameters schema helpers."""

from modules.reports.blood_parameters_schemas import (
    is_canonical_blood_parameters,
    is_legacy_healthians_format,
    is_legacy_metsights_flat_format,
)


def test_canonical_detection():
    blob = {"source": "healthians", "parameters": {"haemoglobin": {"value": 1.0}}}
    assert is_canonical_blood_parameters(blob) is True
    assert is_legacy_healthians_format(blob) is False
    assert is_legacy_metsights_flat_format(blob) is False


def test_legacy_healthians_detection():
    blob = {"customer_name": "A", "digital_data": []}
    assert is_legacy_healthians_format(blob) is True
    assert is_canonical_blood_parameters(blob) is False


def test_legacy_metsights_flat_detection():
    blob = {"haemoglobin": 12.0, "haemoglobin_unit": "g/dL"}
    assert is_legacy_metsights_flat_format(blob) is True
    assert is_canonical_blood_parameters(blob) is False
