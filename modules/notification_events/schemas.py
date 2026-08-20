"""Pydantic schemas for auto notification events API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from common.validation import OptionalSafeDisplayName, OptionalSafeText, PositiveIntId, SafeDisplayName, SlugKey


class NotificationEventCreateRequest(BaseModel):
    engagement_type_ids: list[PositiveIntId] = Field(..., min_length=1)
    event_code: SlugKey
    display_name: SafeDisplayName
    description: OptionalSafeText = None


class NotificationEventUpdateRequest(BaseModel):
    engagement_type_ids: list[PositiveIntId] | None = Field(default=None, min_length=1)
    display_name: OptionalSafeDisplayName = None
    description: OptionalSafeText = None


class EngagementTypeRef(BaseModel):
    engagement_type_id: int
    display_name: str


class NotificationEventResponse(BaseModel):
    notification_event_id: int
    engagement_type_ids: list[int]
    engagement_types: list[EngagementTypeRef] = []
    event_code: str
    display_name: str
    description: str | None = None
    created_at: datetime | None = None
