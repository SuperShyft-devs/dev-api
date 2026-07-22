"""Pydantic schemas for booking flow APIs."""

from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class CheckServiceabilityMember(BaseModel):
    user_id: int = Field(gt=0)
    address_line: str = Field(min_length=1, max_length=500)
    landmark: Optional[str] = Field(default=None, max_length=200)
    city: str = Field(min_length=1, max_length=100)
    pincode: str = Field(min_length=1, max_length=20)
    diagnostic_package_id: int = Field(gt=0)


class CheckServiceabilityRequest(BaseModel):
    members: list[CheckServiceabilityMember] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "CheckServiceabilityRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


class AvailableSlotsMember(BaseModel):
    user_id: int = Field(gt=0)
    engagement_id: int = Field(gt=0)
    blood_collection_date: date


class AvailableSlotsRequest(BaseModel):
    members: list[AvailableSlotsMember] = Field(..., min_length=1, max_length=10)


class LockSlotMember(BaseModel):
    user_id: int = Field(gt=0)
    engagement_id: int = Field(gt=0)
    blood_collection_date: date
    blood_collection_time_slot_id: str = Field(min_length=1, max_length=50)
    blood_collection_time_slot: str = Field(min_length=1, max_length=50)


class LockSlotRequest(BaseModel):
    members: list[LockSlotMember] = Field(..., min_length=1, max_length=10)


class BookPayMember(BaseModel):
    user_id: int = Field(gt=0)
    engagement_id: int = Field(gt=0)


class BookPayRequest(BaseModel):
    members: list[BookPayMember] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "BookPayRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


class VerifyAndBookRequest(BaseModel):
    razorpay_payment_id: str = Field(min_length=1)
    razorpay_order_id: str = Field(min_length=1)
    razorpay_signature: str = Field(min_length=1)


class BookFromDraftMember(BaseModel):
    user_id: int = Field(gt=0)
    engagement_id: int = Field(gt=0)


class BookFromDraftRequest(BaseModel):
    members: list[BookFromDraftMember] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "BookFromDraftRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


class CancelBookingMember(BaseModel):
    user_id: int = Field(gt=0)
    engagement_id: int = Field(gt=0)
    remarks: str = Field(min_length=1, max_length=500)


class CancelBookingRequest(BaseModel):
    members: list[CancelBookingMember] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "CancelBookingRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self
