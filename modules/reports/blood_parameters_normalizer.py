"""Normalize Healthians lab payloads into canonical ``blood_parameters`` storage."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from modules.reports.blood_parameters_schemas import (
    CanonicalBloodParameterValue,
    CanonicalBloodParameters,
    CanonicalBloodParametersCustomer,
    is_canonical_blood_parameters,
    is_legacy_healthians_format,
)


class HealthParameterCatalogRow(Protocol):
    parameter_key: str | None
    healthians_parameter_id: int | None
    test_name: str | None


PARAMETER_KEY_ALIASES: dict[str, str] = {
    "hemoglobin": "haemoglobin",
}


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_digital_data_lookup(raw_customer: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    digital_data = raw_customer.get("digital_data")
    if not isinstance(digital_data, list):
        return lookup
    for entry in digital_data:
        if not isinstance(entry, dict):
            continue
        pid = str(entry.get("parameter_id") or "").strip()
        if pid:
            lookup[pid] = entry
    return lookup


def normalize_from_healthians(
    raw_customer: dict[str, Any],
    *,
    catalog: list[HealthParameterCatalogRow],
    ingested_at: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(canonical_blood_parameters_dict, raw_for_blood_report_raw_column)``."""
    when = ingested_at or datetime.now(timezone.utc)
    dd_lookup = _build_digital_data_lookup(raw_customer)

    parameters: dict[str, CanonicalBloodParameterValue] = {}
    for row in catalog:
        parameter_key = (row.parameter_key or "").strip()
        if not parameter_key:
            continue
        canonical_key = PARAMETER_KEY_ALIASES.get(parameter_key, parameter_key)
        healthians_pid = row.healthians_parameter_id
        if healthians_pid is None:
            continue
        entry = dd_lookup.get(str(healthians_pid))
        if entry is None:
            continue

        parameters[canonical_key] = CanonicalBloodParameterValue(
            value=_parse_float(entry.get("value")),
            unit=(str(entry.get("unit")).strip() if entry.get("unit") is not None else None) or None,
            machine_value=_parse_float(entry.get("machine_value")),
            lower_range=_parse_float(entry.get("min_range")),
            higher_range=_parse_float(entry.get("max_range")),
            healthians_parameter_id=int(healthians_pid),
            provider_test_name=(
                str(entry.get("test_name")).strip()
                if entry.get("test_name") is not None
                else (row.test_name or None)
            ),
        )

    customer = CanonicalBloodParametersCustomer(
        name=(str(raw_customer.get("customer_name") or "").strip() or None),
        age=(str(raw_customer.get("customer_age") or "").strip() or None),
        gender=(str(raw_customer.get("customer_gender") or "").strip() or None),
    )
    canonical = CanonicalBloodParameters(
        source="healthians",
        ingested_at=when.isoformat(),
        customer=customer,
        parameters=parameters,
    )
    return canonical.model_dump(), raw_customer


def read_canonical_parameters(blob: Any) -> dict[str, dict[str, Any]]:
    """Extract ``parameter_key -> value dict`` from canonical or legacy Healthians storage."""
    if not isinstance(blob, dict):
        return {}

    if is_canonical_blood_parameters(blob):
        raw_params = blob.get("parameters")
        if not isinstance(raw_params, dict):
            return {}
        return {str(k): v for k, v in raw_params.items() if isinstance(v, dict)}

    if is_legacy_healthians_format(blob):
        # Normalize on read during migration window (catalog not available here — return empty;
        # callers should use full normalize_from_healthians when catalog is available).
        return {}

    return {}
