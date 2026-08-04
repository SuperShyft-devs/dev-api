"""Pydantic schemas for auto notification events API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class NotificationEventCreateRequest(BaseModel):
    engagement_type_ids: list[int] = Field(..., min_length=1)
    event_code: str = Field(..., min_length=1, max_length=100)
    display_name: str = Field(..., min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=1000)


class NotificationEventUpdateRequest(BaseModel):
    engagement_type_ids: list[int] | None = Field(default=None, min_length=1)
    display_name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=1000)


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
