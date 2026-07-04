"""Tests for blood_parameters schema helpers."""

from modules.reports.blood_parameters_schemas import (
    extract_healthians_customer_blob,
    has_usable_provider_blood_parameters,
    is_canonical_blood_parameters,
    is_empty_blood_parameters,
    is_grouped_blood_parameters,
    is_legacy_healthians_format,
    is_legacy_metsights_flat_format,
    is_metsights_metadata_only,
    provider_code_from_field,
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


def test_grouped_detection():
    blob = [
        {
            "group_name": "CBC",
            "test_count": 1,
            "tests": [
                {
                    "test_id": 1,
                    "parameter_type": "test",
                    "test_name": "Haemoglobin",
                    "parameter_key": "haemoglobin",
                    "value": 13.2,
                }
            ],
        }
    ]
    assert is_grouped_blood_parameters(blob) is True
    assert is_canonical_blood_parameters(blob) is False


def test_has_usable_provider_blood_parameters():
    assert has_usable_provider_blood_parameters(None) is False
    assert has_usable_provider_blood_parameters({}) is False
    assert has_usable_provider_blood_parameters({"id": "x", "is_complete": False}) is False
    assert has_usable_provider_blood_parameters(
        {"source": "healthians", "parameters": {}}
    ) is False
    assert has_usable_provider_blood_parameters(
        {"source": "healthians", "parameters": {"haemoglobin": {"value": 13.0}}}
    ) is True
    assert has_usable_provider_blood_parameters(
        {"customer_name": "A", "digital_data": []}
    ) is False
    assert has_usable_provider_blood_parameters(
        {"customer_name": "A", "digital_data": [{"parameter_id": "1"}]}
    ) is True
    assert has_usable_provider_blood_parameters(
        {"haemoglobin": 12.0, "haemoglobin_unit": "g/dL"}
    ) is True
    assert has_usable_provider_blood_parameters(
        [
            {
                "group_name": "CBC",
                "test_count": 1,
                "tests": [{"test_id": 1, "test_name": "Hb", "value": None}],
            }
        ]
    ) is False
    assert has_usable_provider_blood_parameters(
        [
            {
                "group_name": "CBC",
                "test_count": 1,
                "tests": [{"test_id": 1, "test_name": "Hb", "value": 13.2}],
            }
        ]
    ) is True


def test_provider_code_from_field():
    assert provider_code_from_field({"code": "healthians"}) == "healthians"
    assert provider_code_from_field(
        {"name": "Healthians (No Package)", "lab_provider": {"code": "Healthians"}}
    ) == "Healthians"
    assert provider_code_from_field(
        {"code": "A", "lab_provider": {"code": "B"}}
    ) == "A"
    assert provider_code_from_field({"lab_provider": {}}) == ""
    assert provider_code_from_field({"name": "Healthians (No Package)"}) == ""
    assert provider_code_from_field(None) == ""
    assert provider_code_from_field("other") == "other"
