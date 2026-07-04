"""Console module request/response schemas."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class ConsoleParticipantBookRequest(BaseModel):
    barcode: str = Field(min_length=1)


class ConsoleParticipantBookResponse(BaseModel):
    status: bool
    message: Optional[str] = None
    lead_id: Optional[int] = None
    booking_id: Optional[str] = None
    resCode: Optional[str] = None
    tatDetail: Optional[dict[str, Any]] = None
    barcode: Optional[str] = None
    engagement_participant_id: Optional[int] = None
    user_id: Optional[int] = None
