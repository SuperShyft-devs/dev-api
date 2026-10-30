"""Helpers for diagnostic provider external package/parameter codes."""

from __future__ import annotations


def healthians_package_deal_id(external_package_code: str | None) -> str:
    """Build Healthians deal id string from stored external package code."""
    raw = (external_package_code or "").strip()
    if not raw:
        return "package_0"
    return f"package_{int(raw)}"


def healthians_product_deal_type_id(external_package_code: str | None) -> int:
    raw = (external_package_code or "").strip()
    if not raw:
        return 0
    return int(raw)


def healthians_profile_deal_type_id(external_test_group_code: str | None) -> int:
    raw = (external_test_group_code or "").strip()
    if not raw:
        return 0
    return int(raw)


def _normalized_provider(diagnostic_provider: str | None) -> str:
    return (diagnostic_provider or "").strip().lower()


def is_healthians_diagnostic_provider(diagnostic_provider: str | None) -> bool:
    """True for Healthians packages (including legacy ``Healthians`` stored value)."""
    return _normalized_provider(diagnostic_provider) == "healthians"


def provider_parameter_key(
    parameter: object,
    diagnostic_provider: str | None,
) -> str | None:
    """Return the provider-specific external parameter key for mapping/booking context."""
    provider = _normalized_provider(diagnostic_provider)
    if provider == "orange_health":
        raw = getattr(parameter, "orangehealth_parameter_key", None)
    elif provider in ("healthians", "healthlabs"):
        raw = getattr(parameter, "healthians_parameter_key", None)
    else:
        raw = getattr(parameter, "healthians_parameter_key", None)
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None
