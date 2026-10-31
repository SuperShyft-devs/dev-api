"""Schemas for public diagnostic provider APIs (no auth)."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, model_validator

from common.validation import (
    AddressText,
    CityStateCountry,
    OptionalLandmarkText,
    PinCode,
    PositiveIntId,
)

_SUPPORTED_SLOT_PROVIDERS = frozenset({"healthians", "orange_health"})
_SUPPORTED_ADDRESS_SERVICEABILITY_PROVIDERS = frozenset({"healthians", "orange_health", "redcliffe"})


def _normalize_provider_key(provider: str | None) -> str | None:
    if provider is None:
        return None
    key = provider.strip().lower()
    return key or None


class PublicPincodeServiceabilityRequest(BaseModel):
    provider: str | None = Field(default=None, max_length=50)
    pincode: PinCode

    @model_validator(mode="after")
    def validate_provider_when_set(self) -> PublicPincodeServiceabilityRequest:
        key = _normalize_provider_key(self.provider)
        if key is not None and key not in _SUPPORTED_SLOT_PROVIDERS:
            raise ValueError("Unsupported diagnostic provider")
        return self


class PublicServiceabilityRequest(BaseModel):
    provider: str | None = Field(default=None, max_length=50)
    address_line: AddressText
    landmark: OptionalLandmarkText = None
    city: CityStateCountry
    pincode: PinCode
    latitude: float | None = None
    longitude: float | None = None
    diagnostic_package_id: PositiveIntId | None = None

    @model_validator(mode="after")
    def validate_provider_when_set(self) -> PublicServiceabilityRequest:
        key = _normalize_provider_key(self.provider)
        if key is not None and key not in _SUPPORTED_ADDRESS_SERVICEABILITY_PROVIDERS:
            raise ValueError("Unsupported diagnostic provider")
        return self


class PublicAvailableSlotsRequest(BaseModel):
    provider: str = Field(min_length=1, max_length=50)
    slot_date: date
    diagnostic_package_id: PositiveIntId | None = None
    zone_id: str | None = Field(default=None, max_length=50)
    latitude: float
    longitude: float
    pincode: PinCode | None = None
    has_female_patient: bool = False

    @model_validator(mode="after")
    def validate_provider_fields(self) -> PublicAvailableSlotsRequest:
        prov = _normalize_provider_key(self.provider)
        if prov not in _SUPPORTED_SLOT_PROVIDERS:
            raise ValueError("Unsupported diagnostic provider")
        if prov == "healthians":
            if self.diagnostic_package_id is None:
                raise ValueError("diagnostic_package_id is required for healthians")
            zone = (self.zone_id or "").strip()
            if not zone:
                raise ValueError("zone_id is required for healthians")
        if prov == "orange_health" and self.diagnostic_package_id is None:
            pass
        return self
