"""Pydantic schemas for notification event bindings API."""

from __future__ import annotations

from pydantic import BaseModel, Field

from modules.engagement_notifications.service_config import NotificationServiceConfigItem


class NotificationBindingCreate(BaseModel):
    event_code: str = Field(min_length=1)
    event_name: str = Field(min_length=1)
    recipient_type: str = Field(min_length=1)
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)
    engagement_type_id: int | None = None


class NotificationBindingUpdate(BaseModel):
    event_name: str | None = None
    recipient_type: str | None = None
    notification_services: list[NotificationServiceConfigItem] | None = None
    engagement_type_id: int | None = None


class NotificationBindingResponse(BaseModel):
    id: int
    event_code: str
    event_name: str
    recipient_type: str
    notification_services: list[dict]
    engagement_type_id: int | None = None


class CatalogEventResponse(BaseModel):
    code: str
    name: str
    status: str
    variable_groups: list[str]
    default_recipients: list[str]
    trigger_notes: str
    legacy_event_code: str | None = None
