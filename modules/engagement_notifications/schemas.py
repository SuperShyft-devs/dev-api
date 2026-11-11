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
    event_code: str = Field(min_length=1)
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        return normalize_notification_services(value if isinstance(value, list) else [])


class EngagementNotificationResponse(BaseModel):
    id: int
    engagement_id: int
    event_code: str
    notification_services: list[NotificationServiceConfigItem]
    event_display_name: str | None = None
    recipient_type: str | None = None
    delivery_channel: str | None = None
    recipient_config: dict | None = None
    condition_key: str | None = None
    emit_condition: dict | None = None

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        return normalize_notification_services(value if isinstance(value, list) else [])


class EngagementNotificationSetupEvent(BaseModel):
    event_code: str
    name: str
    is_applicable: bool = True
    is_extra: bool = False
    global_bindings: list[dict] = Field(default_factory=list)
    engagement_bindings: list[dict] = Field(default_factory=list)


class EngagementNotificationSetupResponse(BaseModel):
    profile_key: str = ""
    profile_label: str = ""
    engagement_id: int | None = None
    applicable_event_count: int = 0
    events: list[EngagementNotificationSetupEvent] = Field(default_factory=list)


class EngagementNotificationsUpsertRequest(BaseModel):
    notifications: list[EngagementNotificationItem] = Field(default_factory=list)


class NotificationDefaultItem(BaseModel):
    event_code: str = Field(min_length=1)
    recipient_type: str | None = None
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        return normalize_notification_services(value if isinstance(value, list) else [])


class NotificationDefaultResponse(BaseModel):
    id: int
    engagement_type_id: int | None
    event_code: str
    recipient_type: str
    notification_services: list[NotificationServiceConfigItem]
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
    "EngagementNotificationSetupEvent",
    "EngagementNotificationSetupResponse",
    "EngagementNotificationsUpsertRequest",
    "NotificationDefaultItem",
    "NotificationDefaultResponse",
    "NotificationDefaultsUpsertRequest",
    "NotificationServiceConfigItem",
    "configs_to_api",
    "normalize_notification_services",
    "serialize_for_db",
]
