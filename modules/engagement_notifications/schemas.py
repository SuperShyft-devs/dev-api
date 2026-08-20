"""Pydantic schemas for engagement notifications API."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from common.validation import PositiveIntId

from modules.engagement_notifications.service_config import (
    NotificationServiceConfigItem,
    configs_to_api,
    normalize_notification_services,
    serialize_for_db,
)


class EngagementNotificationItem(BaseModel):
    notification_event_id: PositiveIntId
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        return normalize_notification_services(value if isinstance(value, list) else [])


class EngagementNotificationResponse(BaseModel):
    id: int
    engagement_id: int
    notification_event_id: int
    notification_services: list[NotificationServiceConfigItem]
    event_code: str | None = None
    event_display_name: str | None = None

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        return normalize_notification_services(value if isinstance(value, list) else [])


class EngagementNotificationsUpsertRequest(BaseModel):
    notifications: list[EngagementNotificationItem] = Field(default_factory=list)


class NotificationDefaultItem(BaseModel):
    notification_event_id: PositiveIntId
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        return normalize_notification_services(value if isinstance(value, list) else [])


class NotificationDefaultResponse(BaseModel):
    id: int
    engagement_type_id: int
    notification_event_id: int
    notification_services: list[NotificationServiceConfigItem]
    event_code: str | None = None
    event_display_name: str | None = None

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        return normalize_notification_services(value if isinstance(value, list) else [])


class NotificationDefaultsUpsertRequest(BaseModel):
    engagement_type_id: PositiveIntId
    defaults: list[NotificationDefaultItem] = Field(default_factory=list)


__all__ = [
    "EngagementNotificationItem",
    "EngagementNotificationResponse",
    "EngagementNotificationsUpsertRequest",
    "NotificationDefaultItem",
    "NotificationDefaultResponse",
    "NotificationDefaultsUpsertRequest",
    "NotificationServiceConfigItem",
    "configs_to_api",
    "normalize_notification_services",
    "serialize_for_db",
]
