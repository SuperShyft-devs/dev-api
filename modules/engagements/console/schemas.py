"""Console module request/response schemas."""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

from pydantic import BaseModel, Field


class ConsoleParticipantBookRequest(BaseModel):
    barcode: str = Field(min_length=1)


class HomeCollectionCheckServiceabilityRequest(BaseModel):
    address_line: str = Field(min_length=1, max_length=500)
    landmark: Optional[str] = Field(default=None, max_length=200)
    city: str = Field(min_length=1, max_length=100)
    pincode: str = Field(min_length=1, max_length=20)


class HomeCollectionAvailableSlotsRequest(BaseModel):
    blood_collection_date: date


class HomeCollectionLockRequest(BaseModel):
    blood_collection_date: date
    blood_collection_time_slot_id: str = Field(min_length=1, max_length=50)
    blood_collection_time_slot: str = Field(min_length=1, max_length=50)


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
