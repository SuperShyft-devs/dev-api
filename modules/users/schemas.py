"""Pydantic schemas for users APIs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, EmailStr, Field, validator

_ALLOWED_DIET_PREFERENCES = {"veg", "non_veg", "vegan", "jain", "eggetarian", "keto"}
_ALLOWED_ALLERGIES = {"peanuts", "dairy", "eggs", "fish", "soy", "wheat", "sesame", "mustard", "corn", "other"}


class UserProfileResponse(BaseModel):
    user_id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    age: int
    phone: str
    email: Optional[EmailStr] = None
    profile_photo: Optional[str] = None
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
    age: int
    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    phone: Optional[str] = Field(default=None, min_length=5, max_length=30)
    email: Optional[EmailStr] = None
    profile_photo: Optional[str] = Field(default=None, max_length=500)
    date_of_birth: Optional[date] = None
    gender: Optional[str] = Field(default=None, max_length=30)
    address: Optional[str] = Field(default=None, max_length=500)
    pin_code: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class UserPreferencesResponse(BaseModel):
    preference_id: int
    user_id: int
    push_enabled: bool
    email_enabled: bool
    sms_enabled: bool
    access_to_files: bool
    store_downloaded_files: bool
    diet_preference: str | None
    allergies: list[str]
    updated_at: datetime


class UserPreferencesUpdate(BaseModel):
    push_enabled: Optional[bool] = None
    email_enabled: Optional[bool] = None
    sms_enabled: Optional[bool] = None
    access_to_files: Optional[bool] = None
    store_downloaded_files: Optional[bool] = None
    diet_preference: str | None = None
    allergies: list[str] | None = None

    @validator("diet_preference")
    def validate_diet_preference(cls, value):
        if value is not None and value not in _ALLOWED_DIET_PREFERENCES:
            raise ValueError("Invalid diet preference. Allowed: veg, non_veg, vegan, jain, eggetarian, keto")
        return value

    @validator("allergies")
    def validate_allergies(cls, value):
        if value is None:
            return value
        for item in value:
            if item not in _ALLOWED_ALLERGIES:
                raise ValueError(f"Invalid allergy value: {item}")
        return value


class UpcomingSlotEngagement(BaseModel):
    engagement_type: Literal["b2b", "b2c"]
    organization_name: str | None = None


class UpcomingSlotTiming(BaseModel):
    slot_start_time: str
    slot_end_time: str
    engagement_date: date


class UpcomingSlotLocation(BaseModel):
    type: Literal["venue", "home_collection"]
    display: str


class UpcomingSlotItem(BaseModel):
    engagement: UpcomingSlotEngagement
    slot: UpcomingSlotTiming
    location: UpcomingSlotLocation


class UpcomingSlotResponse(BaseModel):
    has_scheduled_slot: bool
    slots: list[UpcomingSlotItem]


class SubProfileCreate(BaseModel):
    age: int
    first_name: str = Field(min_length=1, max_length=100)
    last_name: str = Field(min_length=1, max_length=100)
    date_of_birth: Optional[date] = None
    gender: str = Field(min_length=1, max_length=30)
    relationship: Literal["spouse", "child", "sibling", "parent", "grandparent", "other"]
    phone: Optional[str] = Field(default=None, min_length=5, max_length=30)
    email: Optional[EmailStr] = None
    city: Optional[str] = Field(default=None, max_length=100)

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class SubProfileUpdate(BaseModel):
    age: int
    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    date_of_birth: Optional[date] = None
    gender: Optional[str] = Field(default=None, max_length=30)
    relationship: Optional[Literal["spouse", "child", "sibling", "parent", "grandparent", "other"]] = None
    phone: Optional[str] = Field(default=None, min_length=5, max_length=30)
    email: Optional[EmailStr] = None
    city: Optional[str] = Field(default=None, max_length=100)
    address: Optional[str] = Field(default=None, max_length=500)

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class SubProfileResponse(BaseModel):
    user_id: int
    first_name: str
    last_name: str
    age: int
    date_of_birth: date | None
    gender: str
    relationship: str
    phone: str
    email: str
    parent_id: int | None
    status: str


class UnlinkRequest(BaseModel):
    """Unlink a sub-profile from its parent. Sub-profile phone must differ from the parent's."""

    child_user_id: Optional[int] = Field(default=None, gt=0)


class BookBioAiRequest(BaseModel):
    """Authenticated B2C booking: new engagement, slot, assessment instance, Metsights record (when configured)."""

    blood_collection_date: date
    blood_collection_time_slot: str = Field(min_length=1, max_length=20)
    diagnostic_package_id: Optional[int] = Field(default=None, gt=0)
    address: Optional[str] = Field(default=None, max_length=500)
    pincode: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)


class BookBioAiMemberPayload(BaseModel):
    """One member in a batch Bio AI or blood-test booking."""

    user_id: int = Field(gt=0)
    address: str = Field(min_length=1, max_length=500)
    pincode: str = Field(min_length=1, max_length=20)
    city: str = Field(min_length=1, max_length=100)
    blood_collection_date: date
    blood_collection_time_slot: str = Field(min_length=1, max_length=20)
    diagnostic_package_id: int = Field(gt=0)


class BookBioAiBatchRequest(BaseModel):
    members: list[BookBioAiMemberPayload] = Field(..., min_length=1)


class BookBloodTestBatchRequest(BaseModel):
    members: list[BookBioAiMemberPayload] = Field(..., min_length=1)


class PublicUserOnboardRequest(BaseModel):
    """Payload for B2C onboarding."""

    age: int
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

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class EngagementUserOnboardRequest(BaseModel):
    """Payload for B2B onboarding into an existing engagement.

    The canonical B2B flow uses `referred_by` to carry the engagement_code.
    The path param is supported for backward compatibility.
    """

    age: int
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

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class UserOnboardResponse(BaseModel):
    user_id: int
    created: bool
    is_participant: bool
    engagement_id: Optional[int] = None
    engagement_code: Optional[str] = None
    time_slot_id: Optional[int] = None
    assessment_instance_id: Optional[int] = None
    metsights_record_id: Optional[str] = None


class UserStatusResponse(BaseModel):
    user_id: int
    status: str
    is_active: bool


class EmployeeCreateUserRequest(BaseModel):
    age: int
    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    phone: str = Field(min_length=5, max_length=30)
    email: Optional[EmailStr] = None
    profile_photo: Optional[str] = Field(default=None, max_length=500)
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

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class EmployeeUpdateUserRequest(BaseModel):
    age: int
    first_name: Optional[str] = Field(default=None, max_length=100)
    last_name: Optional[str] = Field(default=None, max_length=100)
    phone: str = Field(min_length=5, max_length=30)
    email: Optional[EmailStr] = None
    profile_photo: Optional[str] = Field(default=None, max_length=500)
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

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class MetsightsSyncRecordsRequest(BaseModel):
    """Optional B2B engagement to attach synced Metsights records to."""

    engagement_code: Optional[str] = Field(default=None, max_length=200)


class EmployeeUserListItem(BaseModel):
    user_id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    age: int
    phone: str
    email: Optional[EmailStr] = None
    profile_photo: Optional[str] = None
    date_of_birth: Optional[date] = None
    city: Optional[str] = None
    status: Optional[str] = None
    is_participant: Optional[bool] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
