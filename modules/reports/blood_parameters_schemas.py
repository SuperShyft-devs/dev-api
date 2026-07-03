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
