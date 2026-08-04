"""Pydantic schemas for engagement types API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class EngagementTypeCreateRequest(BaseModel):
    code: str = Field(..., min_length=1, max_length=100)
    display_name: str = Field(..., min_length=1, max_length=200)
    is_active: bool = True


class EngagementTypeUpdateRequest(BaseModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=200)
    is_active: bool | None = None


class EngagementTypeResponse(BaseModel):
    id: int
    code: str
    display_name: str
    is_active: bool
