"""Pydantic schemas for Healthians integration."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class HealthiansConstituentsRequest(BaseModel):
    healthians_camp_id: int = Field(..., gt=0)


class HealthiansConstituent(BaseModel):
    id: str
    name: str


class HealthiansConstituentsResponse(BaseModel):
    constituents: list[HealthiansConstituent]
    package_name: Optional[str] = None
