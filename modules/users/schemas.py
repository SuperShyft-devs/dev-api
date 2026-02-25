"""Pydantic schemas for users APIs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, Field


class UserProfileResponse(BaseModel):
    user_id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: str
    email: Optional[EmailStr] = None
    date_of_birth: Optional[date] = None
    gender: Optional[str] = None
    address: Optional[str] = None
    pin_code: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    referred_by: Optional[str] = None
    is_participant: Optional[bool] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class UpdateMyProfileRequest(BaseModel):
    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    email: Optional[EmailStr] = None
    date_of_birth: Optional[date] = None
    gender: Optional[str] = Field(default=None, max_length=30)
    address: Optional[str] = Field(default=None, max_length=500)
    pin_code: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)


class PublicUserOnboardRequest(BaseModel):
    """Payload for B2C onboarding."""

    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    email: Optional[EmailStr] = None
    phone: str = Field(min_length=5, max_length=30)
    gender: Optional[str] = Field(default=None, max_length=30)
    dob: Optional[date] = None
    address: Optional[str] = Field(default=None, max_length=500)
    pincode: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)

    blood_collection_date: date
    blood_collection_time_slot: str = Field(min_length=1, max_length=20)


class EngagementUserOnboardRequest(BaseModel):
    """Payload for B2B onboarding into an existing engagement.

    The canonical B2B flow uses `referred_by` to carry the engagement_code.
    The path param is supported for backward compatibility.
    """

    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    email: Optional[EmailStr] = None
    phone: str = Field(min_length=5, max_length=30)
    gender: Optional[str] = Field(default=None, max_length=30)
    dob: Optional[date] = None
    address: Optional[str] = Field(default=None, max_length=500)
    pincode: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)

    referred_by: Optional[str] = Field(default=None, max_length=200)

    blood_collection_date: date
    blood_collection_time_slot: str = Field(min_length=1, max_length=20)


class UserOnboardResponse(BaseModel):
    user_id: int
    created: bool
    is_participant: bool
    engagement_id: Optional[int] = None
    engagement_code: Optional[str] = None
    time_slot_id: Optional[int] = None


class UserStatusResponse(BaseModel):
    user_id: int
    status: str
    is_active: bool


class EmployeeCreateUserRequest(BaseModel):
    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    phone: str = Field(min_length=5, max_length=30)
    email: Optional[EmailStr] = None
    date_of_birth: Optional[date] = None
    gender: Optional[str] = Field(default=None, max_length=30)
    address: Optional[str] = Field(default=None, max_length=500)
    pin_code: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)
    referred_by: Optional[str] = Field(default=None, max_length=200)
    is_participant: Optional[bool] = None
    status: Optional[str] = Field(default="active", max_length=30)


class EmployeeUpdateUserRequest(BaseModel):
    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    phone: str = Field(min_length=5, max_length=30)
    email: Optional[EmailStr] = None
    date_of_birth: Optional[date] = None
    gender: Optional[str] = Field(default=None, max_length=30)
    address: Optional[str] = Field(default=None, max_length=500)
    pin_code: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)
    referred_by: Optional[str] = Field(default=None, max_length=200)
    is_participant: Optional[bool] = None
    status: str = Field(default="active", max_length=30)


class EmployeeUserListItem(BaseModel):
    user_id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: str
    email: Optional[EmailStr] = None
    city: Optional[str] = None
    status: Optional[str] = None
    is_participant: Optional[bool] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
