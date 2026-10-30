"""Tests for diagnostic external code helpers."""

from types import SimpleNamespace

from modules.diagnostics.external_codes import (
    healthians_package_deal_id,
    healthians_product_deal_type_id,
    provider_parameter_key,
)


def test_healthians_package_deal_id_from_string_code():
    assert healthians_package_deal_id("101") == "package_101"


def test_healthians_package_deal_id_empty_defaults_to_zero():
    assert healthians_package_deal_id(None) == "package_0"
    assert healthians_package_deal_id("") == "package_0"


def test_healthians_product_deal_type_id():
    assert healthians_product_deal_type_id("42") == 42


def test_healthians_profile_deal_type_id():
    from modules.diagnostics.external_codes import healthians_profile_deal_type_id

    assert healthians_profile_deal_type_id("7") == 7


def test_provider_parameter_key_healthians():
    param = SimpleNamespace(healthians_parameter_key="135", orangehealth_parameter_key="oh-1")
    assert provider_parameter_key(param, "Healthians") == "135"
    assert provider_parameter_key(param, "healthians") == "135"


def test_provider_parameter_key_orange_health():
    param = SimpleNamespace(healthians_parameter_key="135", orangehealth_parameter_key="oh-1")
    assert provider_parameter_key(param, "orange_health") == "oh-1"


def test_provider_parameter_key_empty_string_is_none():
    param = SimpleNamespace(healthians_parameter_key="  ", orangehealth_parameter_key=None)
    assert provider_parameter_key(param, "healthians") is None
