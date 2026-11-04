"""Pydantic schemas for booking flow APIs."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, model_validator

from modules.discounts.schemas import CartLine, CheckoutContext, PricePreviewRequest

from common.validation import (
    AddressText,
    CityStateCountry,
    OptionalLandmarkText,
    PinCode,
    PositiveIntId,
    SafeText,
    ShortSafeText,
)


class CheckServiceabilityMember(BaseModel):
    user_id: PositiveIntId
    address_line: AddressText
    landmark: OptionalLandmarkText = None
    city: CityStateCountry
    pincode: PinCode
    diagnostic_package_id: PositiveIntId


class CheckServiceabilityRequest(BaseModel):
    members: list[CheckServiceabilityMember] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "CheckServiceabilityRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


class AvailableSlotsMember(BaseModel):
    user_id: PositiveIntId
    engagement_id: PositiveIntId
    blood_collection_date: date


class AvailableSlotsRequest(BaseModel):
    members: list[AvailableSlotsMember] = Field(..., min_length=1, max_length=10)


class LockSlotMember(BaseModel):
    user_id: PositiveIntId
    engagement_id: PositiveIntId
    blood_collection_date: date
    blood_collection_time_slot_id: str = Field(min_length=1, max_length=50)
    blood_collection_time_slot: ShortSafeText


class LockSlotRequest(BaseModel):
    members: list[LockSlotMember] = Field(..., min_length=1, max_length=10)


class BookPayMember(BaseModel):
    user_id: PositiveIntId
    engagement_id: PositiveIntId


class BookPayRequest(BaseModel):
    members: list[BookPayMember] = Field(..., min_length=1, max_length=10)
    discount_code: str | None = None

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


class ConsultationPayMember(BaseModel):
    user_id: PositiveIntId
    expert_type: ShortSafeText
    expert_id: PositiveIntId
    date: date
    slot: str = Field(min_length=1, max_length=20)


class ConsultationPayRequest(BaseModel):
    members: list[ConsultationPayMember] = Field(..., min_length=1, max_length=10)
    discount_code: str | None = None

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "ConsultationPayRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


class BookFromDraftMember(BaseModel):
    user_id: PositiveIntId
    engagement_id: PositiveIntId


class BookFromDraftRequest(BaseModel):
    members: list[BookFromDraftMember] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "BookFromDraftRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


class CancelBookingMember(BaseModel):
    user_id: PositiveIntId
    engagement_id: PositiveIntId
    remarks: SafeText


class CancelBookingRequest(BaseModel):
    members: list[CancelBookingMember] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "CancelBookingRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


class RescheduleBloodTestMember(BaseModel):
    user_id: PositiveIntId
    engagement_id: PositiveIntId
    blood_collection_date: date
    blood_collection_time_slot_id: str = Field(min_length=1, max_length=50)
    blood_collection_time_slot: ShortSafeText
    reschedule_reason: SafeText


class RescheduleBloodTestRequest(BaseModel):
    members: list[RescheduleBloodTestMember] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "RescheduleBloodTestRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self

