"""Notifications module Pydantic schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class DispatchRequest(BaseModel):
    service_key: str = Field(..., min_length=1)
    user_ids: list[int] = Field(..., min_length=1)
    engagement_id: int | None = None
    record_id: str | None = None
    participant_details: dict | None = None
    otp: str | None = None


class CallbackRequest(BaseModel):
    notification_id: int
    status: str = Field(..., pattern=r"^(sent|failed)$")
    message: str | None = None


class NotificationServiceCreate(BaseModel):
    service_key: str = Field(..., min_length=1)
    display_name: str = Field(..., min_length=1)
    channel: str = Field(..., pattern=r"^(email|whatsapp)$")
    webhook_path: str = Field(..., min_length=1)
    is_active: bool = True
    require_record_id: bool = True
    require_participant_detail: bool = False
    require_otp: bool = False


class NotificationServiceUpdate(BaseModel):
    display_name: str | None = None
    channel: str | None = Field(None, pattern=r"^(email|whatsapp)$")
    webhook_path: str | None = None
    is_active: bool | None = None
    require_record_id: bool | None = None
    require_participant_detail: bool | None = None
    require_otp: bool | None = None
