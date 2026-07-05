"""Pydantic schemas for users APIs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, EmailStr, Field, model_validator, validator

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


class SportsPlaylistPayload(BaseModel):
    sportIds: list[str] = Field(default_factory=list)
    otherSelected: bool = False
    otherNote: str = ""


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
    sports_playlists: SportsPlaylistPayload | None = None
    updated_at: datetime


class UserPreferencesUpdate(BaseModel):
    push_enabled: Optional[bool] = None
    email_enabled: Optional[bool] = None
    sms_enabled: Optional[bool] = None
    access_to_files: Optional[bool] = None
    store_downloaded_files: Optional[bool] = None
    diet_preference: str | None = None
    allergies: list[str] | None = None
    sports_playlists: SportsPlaylistPayload | None = None

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
    members: list[BookBioAiMemberPayload] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "BookBioAiBatchRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


class BookBloodTestBatchRequest(BaseModel):
    members: list[BookBioAiMemberPayload] = Field(..., min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_member_user_ids(self) -> "BookBloodTestBatchRequest":
        ids = [m.user_id for m in self.members]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in members")
        return self


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

    participants_employee_id: Optional[str] = Field(default=None, max_length=100)
    participant_department: Optional[str] = Field(default=None, max_length=100)
    participant_blood_group: Optional[str] = Field(default=None, max_length=20)
    want_doctor_consultation: Optional[bool] = None
    want_nutritionist_consultation: Optional[bool] = None
    want_doctor_and_nutritionist_consultation: Optional[bool] = None

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

    participants_employee_id: Optional[str] = Field(default=None, max_length=100)
    participant_department: Optional[str] = Field(default=None, max_length=100)
    participant_blood_group: Optional[str] = Field(default=None, max_length=20)
    want_doctor_consultation: Optional[bool] = None
    want_nutritionist_consultation: Optional[bool] = None
    want_doctor_and_nutritionist_consultation: Optional[bool] = None

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
    engagement_participant_id: Optional[int] = None
    assessment_instance_id: Optional[int] = None
    metsights_record_id: Optional[str] = None


class BookingPaymentResponse(BaseModel):
    """Returned by POST /book/bio-ai and POST /book/blood-test after creating bookings + Razorpay order."""

    booking_ids: list[int]
    booking_id: int
    razorpay_order_id: str
    amount_paise: int
    amount_rupees: float
    currency: str = "INR"
    key_id: str


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


class UpdateMetsightsProfileIdRequest(BaseModel):
    """Set or clear a user's Metsights profile id (employee admin)."""

    metsights_profile_id: str = ""


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


class ImportMetsightsProfilesRequest(BaseModel):
    """Bulk import Metsights profiles into local users + B2C engagements (employee tooling)."""

    metsights_profile_ids: list[str]

    @validator("metsights_profile_ids")
    def validate_profile_ids(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("metsights_profile_ids must not be empty")
        if len(v) > 50:
            raise ValueError("At most 50 metsights_profile_ids per request")
        seen: set[str] = set()
        out: list[str] = []
        for raw in v:
            s = (raw or "").strip()
            if not s:
                raise ValueError("metsights_profile_ids entries cannot be empty")
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        if not out:
            raise ValueError("metsights_profile_ids must not be empty")
        return out


class UserDeleteImpactEngagement(BaseModel):
    engagement_id: int
    engagement_code: str
    engagement_name: Optional[str] = None


class UserDeleteImpactResponse(BaseModel):
    engagements_to_orphan: list[UserDeleteImpactEngagement]


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
    metsights_profile_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
