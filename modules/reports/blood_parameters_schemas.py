"""Canonical blood parameters storage schemas (provider / lab results only)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CanonicalBloodParameterValue(BaseModel):
    value: float | None = None
    unit: str | None = None
    machine_value: float | None = None
    lower_range: float | None = None
    higher_range: float | None = None
    healthians_parameter_id: int | None = None
    provider_test_name: str | None = None


class CanonicalBloodParametersCustomer(BaseModel):
    name: str | None = None
    age: str | None = None
    gender: str | None = None


class CanonicalBloodParameters(BaseModel):
    source: str
    ingested_at: str
    customer: CanonicalBloodParametersCustomer | None = None
    parameters: dict[str, CanonicalBloodParameterValue] = Field(default_factory=dict)


def is_canonical_blood_parameters(blob: Any) -> bool:
    """Return True when ``blob`` is normalized provider storage (top-level ``parameters`` dict)."""
    if not isinstance(blob, dict):
        return False
    params = blob.get("parameters")
    return isinstance(params, dict)


def is_legacy_healthians_format(blob: Any) -> bool:
    """Return True when ``blob`` is a raw Healthians customer object."""
    return isinstance(blob, dict) and isinstance(blob.get("digital_data"), list)


def is_legacy_metsights_flat_format(blob: Any) -> bool:
    """Return True when ``blob`` is a legacy Metsights flat dict stored in ``blood_parameters``."""
    if not isinstance(blob, dict):
        return False
    if is_canonical_blood_parameters(blob) or is_legacy_healthians_format(blob):
        return False
    return bool(blob)


_METSIGHTS_METADATA_KEYS = frozenset({"id", "is_complete", "created_at", "updated_at"})


def is_empty_blood_parameters(blob: Any) -> bool:
    """Return True when ``blob`` is null-like or carries no usable blood data."""
    if blob is None:
        return True
    if isinstance(blob, dict):
        return len(blob) == 0
    if isinstance(blob, list):
        return len(blob) == 0
    if isinstance(blob, str):
        return not blob.strip()
    return False


def has_usable_provider_blood_parameters(blob: Any) -> bool:
    """Return True when ``blob`` has provider/lab values worth serving from cache."""
    if is_empty_blood_parameters(blob) or is_metsights_metadata_only(blob):
        return False
    if is_canonical_blood_parameters(blob):
        params = blob.get("parameters")
        return isinstance(params, dict) and len(params) > 0
    if is_legacy_healthians_format(blob):
        digital_data = blob.get("digital_data")
        return isinstance(digital_data, list) and len(digital_data) > 0
    if is_legacy_metsights_flat_format(blob):
        return True
    return False


def provider_code_from_field(provider_field: Any) -> str:
    """Extract provider code from Metsights fetch-collections ``provider`` field."""
    if isinstance(provider_field, dict):
        return str(provider_field.get("code") or "").strip()
    if provider_field is None:
        return ""
    return str(provider_field).strip()


def is_metsights_metadata_only(blob: Any) -> bool:
    """Metsights API wrapper with only record metadata and no parameter values."""
    if not isinstance(blob, dict) or not blob:
        return False
    value_keys = {
        key
        for key in blob
        if key not in _METSIGHTS_METADATA_KEYS and not str(key).endswith("_unit")
    }
    return not value_keys


def extract_healthians_customer_blob(blob: Any) -> dict[str, Any] | None:
    """Return a Healthians customer object from raw or API-envelope JSON."""
    if not isinstance(blob, dict):
        return None
    if is_legacy_healthians_format(blob):
        return blob
    data = blob.get("data")
    if not isinstance(data, list):
        return None
    for entry in data:
        if isinstance(entry, dict) and is_legacy_healthians_format(entry):
            return entry
    return None


def describe_blood_parameters_blob(blob: Any) -> str:
    """Short description for migration error reporting."""
    if blob is None:
        return "null"
    if isinstance(blob, dict):
        keys = sorted(str(k) for k in blob.keys())
        preview = ", ".join(keys[:8])
        if len(keys) > 8:
            preview += ", ..."
        return f"dict(keys=[{preview}])"
    return type(blob).__name__

