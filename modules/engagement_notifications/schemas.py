"""Pydantic schemas for engagement notifications API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class EngagementNotificationItem(BaseModel):
    notification_event_id: int
    notification_services: list[str] = Field(default_factory=list)


class EngagementNotificationResponse(BaseModel):
    id: int
    engagement_id: int
    notification_event_id: int
    notification_services: list[str]
    event_code: str | None = None
    event_display_name: str | None = None


class EngagementNotificationsUpsertRequest(BaseModel):
    notifications: list[EngagementNotificationItem] = Field(default_factory=list)


class NotificationDefaultItem(BaseModel):
    notification_event_id: int
    notification_services: list[str] = Field(default_factory=list)


class NotificationDefaultResponse(BaseModel):
    id: int
    engagement_type_id: int
    notification_event_id: int
    notification_services: list[str]
    event_code: str | None = None
    event_display_name: str | None = None


class NotificationDefaultsUpsertRequest(BaseModel):
    engagement_type_id: int
    defaults: list[NotificationDefaultItem] = Field(default_factory=list)
