"""Audit module request/response schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class IntegrationSyncLogItem(BaseModel):
    sync_log_id: int
    engagement_id: int | None = None
    user_id: int | None = None
    provider: str
    api_endpoint_url: str
    request_payload: dict[str, Any] | list[Any] | None = None
    response_payload: dict[str, Any] | list[Any] | None = None
    status: str | None = None
    error_message: str | None = None
    created_at: datetime
