"""Pydantic schemas for inbound webhooks."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HealthiansWebhookPayload(BaseModel):
    """Healthians B2B webhook body (status updates, reports, phlebo events, etc.)."""

    model_config = ConfigDict(extra="allow")

    type: str
    booking_id: str
    data: dict[str, Any] = Field(default_factory=dict)
