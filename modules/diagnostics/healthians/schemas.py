"""Pydantic schemas for Healthians integration."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, model_validator


class HealthiansConstituentsRequest(BaseModel):
    external_code: str | None = Field(default=None, min_length=1)
    external_package_code: str | None = Field(default=None, min_length=1)
    deal_type: str = Field(default="package", pattern="^(package|profile)$")

    @model_validator(mode="after")
    def _resolve_external_code(self) -> HealthiansConstituentsRequest:
        code = (self.external_code or self.external_package_code or "").strip()
        if not code:
            raise ValueError("external_code is required")
        object.__setattr__(self, "external_code", code)
        return self


class HealthiansProfileSearchResponse(BaseModel):
    profiles: list[HealthiansConstituent]


class HealthiansConstituent(BaseModel):
    id: str
    name: str


class HealthiansConstituentsResponse(BaseModel):
    constituents: list[HealthiansConstituent]
    package_name: Optional[str] = None
