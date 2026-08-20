"""Pydantic schemas for engagement types API."""

from __future__ import annotations

from pydantic import BaseModel

from common.validation import OptionalSafeDisplayName, SafeDisplayName, SlugKey


class EngagementTypeCreateRequest(BaseModel):
    code: SlugKey
    display_name: SafeDisplayName
    is_active: bool = True


class EngagementTypeUpdateRequest(BaseModel):
    display_name: OptionalSafeDisplayName = None
    is_active: bool | None = None


class EngagementTypeResponse(BaseModel):
    id: int
    code: str
    display_name: str
    is_active: bool
