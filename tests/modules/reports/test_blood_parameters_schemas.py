"""Tests for blood_parameters schema helpers."""

from modules.reports.blood_parameters_schemas import (
    describe_blood_parameters_blob,
    extract_healthians_customer_blob,
    is_canonical_blood_parameters,
    is_empty_blood_parameters,
    is_legacy_healthians_format,
    is_legacy_metsights_flat_format,
    is_metsights_metadata_only,
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


def test_empty_and_metadata_detection():
    assert is_empty_blood_parameters({}) is True
    assert is_empty_blood_parameters(None) is True
    assert is_metsights_metadata_only({"id": "x", "is_complete": False}) is True
    assert is_metsights_metadata_only({"haemoglobin": 12.0}) is False


def test_healthians_envelope_extraction():
    customer = {
        "customer_name": "Jane",
        "digital_data": [{"parameter_id": "1", "value": "12"}],
    }
    envelope = {"status": True, "data": [customer]}
    assert extract_healthians_customer_blob(envelope) == customer
