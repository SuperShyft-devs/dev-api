"""Pydantic schemas for users APIs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, EmailStr, Field, model_validator, validator

from common.validation import (
    AddressText,
    CityStateCountry,
    EngagementCode,
    OptionalAddressText,
    OptionalCityStateCountry,
    OptionalEngagementCode,
    OptionalPersonName,
    OptionalPhoneStr,
    OptionalPinCode,
    OptionalSafeDisplayName,
    OptionalShortSafeText,
    PersonName,
    PhoneStr,
    PinCode,
    PositiveIntId,
    SafeDisplayName,
    ShortSafeText,
    SlugKey,
    StatusStr,
    validate_nested_strings,
)
from modules.questionnaire.schemas import ResponseItem

_ALLOWED_DIET_PREFERENCES = {"veg", "non_veg", "vegan", "jain", "eggetarian", "keto"}
_ALLOWED_ALLERGIES = {"peanuts", "dairy", "eggs", "fish", "soy", "wheat", "sesame", "mustard", "corn", "other"}


class OnboardCategoryQuestionnaire(BaseModel):
    """Per-category answers embedded in public onboard (same shape as PUT responses)."""

    responses: list[ResponseItem] = Field(default_factory=list, max_length=500)


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
    first_name: OptionalPersonName = None
    last_name: OptionalPersonName = None
    phone: OptionalPhoneStr = None
    email: Optional[EmailStr] = None
    profile_photo: Optional[str] = Field(default=None, max_length=500)
    date_of_birth: Optional[date] = None
    gender: OptionalSafeDisplayName = None
    address: OptionalAddressText = None
    pin_code: OptionalPinCode = None
    city: OptionalCityStateCountry = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None

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
    first_name: PersonName
    last_name: PersonName
    date_of_birth: Optional[date] = None
    gender: SafeDisplayName
    relationship: Literal["spouse", "child", "sibling", "parent", "grandparent", "other"]
    phone: OptionalPhoneStr = None
    email: Optional[EmailStr] = None
    city: OptionalCityStateCountry = None

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class SubProfileUpdate(BaseModel):
    age: int
    first_name: OptionalPersonName = None
    last_name: OptionalPersonName = None
    date_of_birth: Optional[date] = None
    gender: OptionalSafeDisplayName = None
    relationship: Optional[Literal["spouse", "child", "sibling", "parent", "grandparent", "other"]] = None
    phone: OptionalPhoneStr = None
    email: Optional[EmailStr] = None
    city: OptionalCityStateCountry = None
    address: OptionalAddressText = None

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

    child_user_id: PositiveIntId | None = None


class BookBioAiRequest(BaseModel):
    """Authenticated B2C booking: new engagement, slot, assessment instance, Metsights record (when configured)."""

    blood_collection_date: date
    blood_collection_time_slot: ShortSafeText
    diagnostic_package_id: PositiveIntId | None = None
    address: OptionalAddressText = None
    pincode: OptionalPinCode = None
    city: OptionalCityStateCountry = None


class BookBioAiMemberPayload(BaseModel):
    """One member in a batch Bio AI or blood-test booking."""

    user_id: PositiveIntId
    address: AddressText
    pincode: PinCode
    city: CityStateCountry
    blood_collection_date: date
    blood_collection_time_slot: ShortSafeText
    diagnostic_package_id: PositiveIntId


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
    """Payload for B2C onboarding.

    When ``user_id`` is provided, personal profile fields are optional and ignored;
    the existing user is loaded from the database. Without ``user_id``, ``age`` and
    ``phone`` remain required (create / get-or-create by phone).
    """

    user_id: PositiveIntId | None = None

    age: Optional[int] = None
    first_name: OptionalPersonName = None
    last_name: OptionalPersonName = None
    email: Optional[EmailStr] = None
    phone: OptionalPhoneStr = None
    gender: OptionalSafeDisplayName = None
    dob: Optional[date] = None
    address: OptionalAddressText = None
    pincode: OptionalPinCode = None
    city: OptionalCityStateCountry = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None

    # Must match an active row in engagement_types.code (validated in service).
    engagement_type: SlugKey = "bio_ai"

    # Optional only when engagement_type is "vifc"; required for all other types.
    blood_collection_date: Optional[date] = None
    blood_collection_time_slot: OptionalShortSafeText = None

    participants_employee_id: OptionalSafeDisplayName = None
    participant_department: OptionalSafeDisplayName = None
    participant_blood_group: OptionalSafeDisplayName = None
    # Legacy fields (deprecated, kept for backward compat)
    want_doctor_consultation: Optional[bool] = None
    want_nutritionist_consultation: Optional[bool] = None
    want_doctor_and_nutritionist_consultation: Optional[bool] = None
    # New field: { expert_type: { want, date, cabin, slot, expert_id } }
    consultations: Optional[dict[str, Any]] = None

    # Optional: category_key -> { responses: [{ question_id, answer }, ...] }
    questionnaire: Optional[dict[str, OnboardCategoryQuestionnaire]] = None

    @model_validator(mode="after")
    def require_personal_fields_without_user_id(self):
        if self.user_id is not None:
            return self
        if self.phone is None or not str(self.phone).strip():
            raise ValueError("phone is required when user_id is not provided")
        if self.age is None:
            raise ValueError("age is required when user_id is not provided")
        if self.age < 1 or self.age > 120:
            raise ValueError("Age must be between 1 and 120")
        return self

    @model_validator(mode="after")
    def require_blood_fields_unless_vifc(self):
        if (self.engagement_type or "").strip().lower() == "vifc":
            return self
        if self.blood_collection_date is None:
            raise ValueError("blood_collection_date is required")
        slot = (self.blood_collection_time_slot or "").strip()
        if not slot:
            raise ValueError("blood_collection_time_slot is required")
        self.blood_collection_time_slot = slot
        return self

    @model_validator(mode="after")
    def sanitize_nested_inputs(self):
        if self.questionnaire is not None:
            validate_nested_strings(self.questionnaire)
        if self.consultations is not None:
            validate_nested_strings(self.consultations)
        return self

    @model_validator(mode="after")
    def normalize_consultations(self):
        from modules.experts.consultations import empty_preference, normalize_consultations_map

        if self.consultations is None:
            doctor = bool(self.want_doctor_consultation or self.want_doctor_and_nutritionist_consultation)
            nutritionist = bool(self.want_nutritionist_consultation or self.want_doctor_and_nutritionist_consultation)
            built: dict[str, Any] = {}
            if doctor:
                built["doctor"] = empty_preference(want=True)
            if nutritionist:
                built["nutritionist"] = empty_preference(want=True)
            self.consultations = built
        else:
            self.consultations = normalize_consultations_map(self.consultations)
        return self


class EngagementUserOnboardRequest(BaseModel):
    """Payload for B2B onboarding into an existing engagement.

    The canonical B2B flow uses `referred_by` to carry the engagement_code.
    The path param is supported for backward compatibility.
    """

    age: int
    first_name: OptionalPersonName = None
    last_name: OptionalPersonName = None
    email: Optional[EmailStr] = None
    phone: PhoneStr
    gender: OptionalSafeDisplayName = None
    dob: Optional[date] = None
    address: OptionalAddressText = None
    pincode: OptionalPinCode = None
    city: OptionalCityStateCountry = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None

    referred_by: OptionalEngagementCode = None

    blood_collection_date: date
    blood_collection_time_slot: ShortSafeText
    blood_collection_cabin: OptionalSafeDisplayName = None

    participants_employee_id: OptionalSafeDisplayName = None
    participant_department: OptionalSafeDisplayName = None
    participant_blood_group: OptionalSafeDisplayName = None
    # Legacy fields (deprecated, kept for backward compat)
    want_doctor_consultation: Optional[bool] = None
    want_nutritionist_consultation: Optional[bool] = None
    want_doctor_and_nutritionist_consultation: Optional[bool] = None
    # New field: { expert_type: { want, date, cabin, slot, expert_id } }
    consultations: Optional[dict[str, Any]] = None

    @model_validator(mode="after")
    def sanitize_consultations(self):
        if self.consultations is not None:
            validate_nested_strings(self.consultations)
        return self

    @model_validator(mode="after")
    def normalize_consultations(self):
        from modules.experts.consultations import empty_preference, normalize_consultations_map

        if self.consultations is None:
            doctor = bool(self.want_doctor_consultation or self.want_doctor_and_nutritionist_consultation)
            nutritionist = bool(self.want_nutritionist_consultation or self.want_doctor_and_nutritionist_consultation)
            built: dict[str, Any] = {}
            if doctor:
                built["doctor"] = empty_preference(want=True)
            if nutritionist:
                built["nutritionist"] = empty_preference(want=True)
            self.consultations = built
        else:
            self.consultations = normalize_consultations_map(self.consultations)
        return self

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


class VifcQuickStartRequest(BaseModel):
    """Public VIFC quick-start: onboard with engagement_type=vifc then start face scan.

    When ``user_id`` is provided, personal profile fields are optional and ignored.
    Without ``user_id``, ``age`` and ``phone`` remain required.
    """

    user_id: PositiveIntId | None = None

    age: Optional[int] = None
    first_name: OptionalPersonName = None
    last_name: OptionalPersonName = None
    email: Optional[EmailStr] = None
    phone: OptionalPhoneStr = None
    gender: OptionalSafeDisplayName = None
    dob: Optional[date] = None
    address: OptionalAddressText = None
    pincode: OptionalPinCode = None
    city: OptionalCityStateCountry = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None

    questionnaire: Optional[dict[str, OnboardCategoryQuestionnaire]] = None

    @model_validator(mode="after")
    def require_personal_fields_without_user_id(self):
        if self.user_id is not None:
            return self
        if self.phone is None or not str(self.phone).strip():
            raise ValueError("phone is required when user_id is not provided")
        if self.age is None:
            raise ValueError("age is required when user_id is not provided")
        if self.age < 1 or self.age > 120:
            raise ValueError("Age must be between 1 and 120")
        return self

    @model_validator(mode="after")
    def sanitize_questionnaire(self):
        if self.questionnaire is not None:
            validate_nested_strings(self.questionnaire)
        return self


class VifcQuickStartResponse(BaseModel):
    user_id: int
    created: bool
    is_participant: bool
    engagement_id: Optional[int] = None
    engagement_code: Optional[str] = None
    engagement_participant_id: Optional[int] = None
    assessment_instance_id: Optional[int] = None
    metsights_record_id: Optional[str] = None
    face_scan_link: str


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
    first_name: OptionalPersonName = None
    last_name: OptionalPersonName = None
    phone: PhoneStr
    email: Optional[EmailStr] = None
    profile_photo: Optional[str] = Field(default=None, max_length=500)
    date_of_birth: Optional[date] = None
    gender: OptionalSafeDisplayName = None
    address: OptionalAddressText = None
    pin_code: OptionalPinCode = None
    city: OptionalCityStateCountry = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None
    referred_by: OptionalEngagementCode = None
    is_participant: Optional[bool] = None
    status: StatusStr | None = "active"

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
    first_name: OptionalPersonName = None
    last_name: OptionalPersonName = None
    phone: PhoneStr
    email: Optional[EmailStr] = None
    profile_photo: Optional[str] = Field(default=None, max_length=500)
    date_of_birth: Optional[date] = None
    gender: OptionalSafeDisplayName = None
    address: OptionalAddressText = None
    pin_code: OptionalPinCode = None
    city: OptionalCityStateCountry = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None
    referred_by: OptionalEngagementCode = None
    is_participant: Optional[bool] = None
    status: StatusStr = "active"

    @validator("age")
    def age_must_be_valid(cls, v):
        if v < 1 or v > 120:
            raise ValueError("Age must be between 1 and 120")
        return v


class MetsightsSyncRecordsRequest(BaseModel):
    """Optional B2B engagement to attach synced Metsights records to."""

    engagement_code: OptionalEngagementCode = None


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
