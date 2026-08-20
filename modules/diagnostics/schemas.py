"""Pydantic schemas for diagnostics APIs."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from common.validation import (
    OptionalSafeDisplayName,
    OptionalSafeText,
    OptionalSlugKey,
    SafeDisplayName,
    SlugKey,
    StatusStr,
)


class ParameterType(str, Enum):
    TEST = "test"
    METRIC = "metric"


class PackageListType(str, Enum):
    PUBLIC_PACKAGE = "public_package"
    CUSTOM_PACKAGE = "custom_package"


class PackageForType(str, Enum):
    PUBLIC = "public"
    CAMP = "camp"


class FilterChipForSchema(str, Enum):
    PUBLIC_PACKAGE = "public_package"
    CUSTOM_PACKAGE = "custom_package"


class DiagnosticPackageCreate(BaseModel):
    package_name: SafeDisplayName
    package_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    external_package_id: Optional[int] = None
    custom: bool = False
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    health_areas_covered: OptionalSafeText = None
    about_text: OptionalSafeText = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    complementary_consultation: Optional[dict[str, bool]] = None
    gender_suitability: Optional[str] = None
    reference_id: Optional[str] = None
    package_for: Optional[PackageForType] = None


class DiagnosticPackageUpdate(BaseModel):
    package_name: OptionalSafeDisplayName = None
    package_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    external_package_id: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    health_areas_covered: OptionalSafeText = None
    about_text: OptionalSafeText = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    complementary_consultation: Optional[dict[str, bool]] = None
    gender_suitability: Optional[str] = None
    reference_id: Optional[str] = None
    package_for: Optional[PackageForType] = None


class DiagnosticPackageStatusUpdate(BaseModel):
    status: StatusStr


class DiagnosticPackageResponse(BaseModel):
    diagnostic_package_id: int
    reference_id: Optional[str] = None
    package_name: str
    package_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    external_package_id: Optional[int] = None
    created_by_user_id: Optional[int] = None
    no_of_tests: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    health_areas_covered: Optional[str] = None
    about_text: Optional[str] = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    complementary_consultation: Optional[dict[str, bool]] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    discount_percent: Optional[int] = None


class FilterChipCreate(BaseModel):
    chip_key: SlugKey
    display_name: SafeDisplayName
    display_order: Optional[int] = None
    chip_for: FilterChipForSchema = FilterChipForSchema.PUBLIC_PACKAGE


class FilterChipUpdate(BaseModel):
    chip_key: OptionalSlugKey = None
    display_name: OptionalSafeDisplayName = None
    display_order: Optional[int] = None
    status: Optional[StatusStr] = None
    chip_for: Optional[FilterChipForSchema] = None


class FilterChipResponse(BaseModel):
    filter_chip_id: int
    chip_key: str
    display_name: str
    display_order: Optional[int] = None
    chip_for: str = "public_package"
    status: Optional[str] = None


class PackageFilterChipResponse(BaseModel):
    filter_chip_id: int
    chip_key: str
    display_name: str
    display_order: Optional[int] = None


class PackageFilterChipAssign(BaseModel):
    filter_chip_id: int
    display_order: Optional[int] = None


class ReasonCreate(BaseModel):
    reason_text: SafeDisplayName
    display_order: Optional[int] = None


class ReasonUpdate(BaseModel):
    reason_text: OptionalSafeDisplayName = None
    display_order: Optional[int] = None


class ReasonResponse(BaseModel):
    reason_id: int
    diagnostic_package_id: int
    reason_text: str
    display_order: Optional[int] = None


class TagCreate(BaseModel):
    tag_name: SafeDisplayName
    display_order: Optional[int] = None


class TagResponse(BaseModel):
    tag_id: int
    diagnostic_package_id: int
    tag_name: str
    display_order: Optional[int] = None


class HealthParameterCreate(BaseModel):
    parameter_type: ParameterType = ParameterType.TEST
    test_name: SafeDisplayName
    external_parameter_id: Optional[int] = None
    parameter_key: OptionalSlugKey = None
    unit: Optional[str] = None
    meaning: OptionalSafeText = None
    low_risk_lower_range_male: Optional[float] = None
    low_risk_higher_range_male: Optional[float] = None
    moderate_risk_lower_range_male: Optional[float] = None
    moderate_risk_higher_range_male: Optional[float] = None
    high_risk_lower_range_male: Optional[float] = None
    high_risk_higher_range_male: Optional[float] = None
    low_risk_lower_range_female: Optional[float] = None
    low_risk_higher_range_female: Optional[float] = None
    moderate_risk_lower_range_female: Optional[float] = None
    moderate_risk_higher_range_female: Optional[float] = None
    high_risk_lower_range_female: Optional[float] = None
    high_risk_higher_range_female: Optional[float] = None
    causes_when_high: OptionalSafeText = None
    causes_when_low: OptionalSafeText = None
    effects_when_high: OptionalSafeText = None
    effects_when_low: OptionalSafeText = None
    what_to_do_when_low: OptionalSafeText = None
    what_to_do_when_high: OptionalSafeText = None
    is_available: bool = True
    display_order: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None


class HealthParameterUpdate(BaseModel):
    parameter_type: Optional[ParameterType] = None
    test_name: OptionalSafeDisplayName = None
    external_parameter_id: Optional[int] = None
    parameter_key: OptionalSlugKey = None
    unit: Optional[str] = None
    meaning: OptionalSafeText = None
    low_risk_lower_range_male: Optional[float] = None
    low_risk_higher_range_male: Optional[float] = None
    moderate_risk_lower_range_male: Optional[float] = None
    moderate_risk_higher_range_male: Optional[float] = None
    high_risk_lower_range_male: Optional[float] = None
    high_risk_higher_range_male: Optional[float] = None
    low_risk_lower_range_female: Optional[float] = None
    low_risk_higher_range_female: Optional[float] = None
    moderate_risk_lower_range_female: Optional[float] = None
    moderate_risk_higher_range_female: Optional[float] = None
    high_risk_lower_range_female: Optional[float] = None
    high_risk_higher_range_female: Optional[float] = None
    causes_when_high: OptionalSafeText = None
    causes_when_low: OptionalSafeText = None
    effects_when_high: OptionalSafeText = None
    effects_when_low: OptionalSafeText = None
    what_to_do_when_low: OptionalSafeText = None
    what_to_do_when_high: OptionalSafeText = None
    display_order: Optional[int] = None
    is_available: Optional[bool] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None


class HealthParameterResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    test_id: int
    parameter_type: ParameterType
    test_name: str
    external_parameter_id: Optional[int] = None
    parameter_key: Optional[str] = None
    unit: Optional[str] = None
    meaning: Optional[str] = None
    low_risk_lower_range_male: Optional[float] = None
    low_risk_higher_range_male: Optional[float] = None
    moderate_risk_lower_range_male: Optional[float] = None
    moderate_risk_higher_range_male: Optional[float] = None
    high_risk_lower_range_male: Optional[float] = None
    high_risk_higher_range_male: Optional[float] = None
    low_risk_lower_range_female: Optional[float] = None
    low_risk_higher_range_female: Optional[float] = None
    moderate_risk_lower_range_female: Optional[float] = None
    moderate_risk_higher_range_female: Optional[float] = None
    high_risk_lower_range_female: Optional[float] = None
    high_risk_higher_range_female: Optional[float] = None
    causes_when_high: Optional[str] = None
    causes_when_low: Optional[str] = None
    effects_when_high: Optional[str] = None
    effects_when_low: Optional[str] = None
    what_to_do_when_low: Optional[str] = None
    what_to_do_when_high: Optional[str] = None
    is_available: bool
    display_order: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None


class TestGroupCreate(BaseModel):
    group_name: SafeDisplayName
    group_key: SlugKey
    display_order: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[PackageForType] = None


class TestGroupUpdate(BaseModel):
    group_name: OptionalSafeDisplayName = None
    group_key: OptionalSlugKey = None
    display_order: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[PackageForType] = None


class TestGroupResponse(BaseModel):
    group_id: int
    group_name: str
    group_key: str
    test_count: int
    display_order: Optional[int] = None
    price: Optional[float] = None
    discount: Optional[str] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[str] = None
    tests: list[HealthParameterResponse] = Field(default_factory=list)
    filter_chips: list[PackageFilterChipResponse] = Field(default_factory=list)


class AssignTestsToGroupRequest(BaseModel):
    test_ids: list[int] = Field(min_length=1)


class ReorderGroupTestsRequest(BaseModel):
    test_ids: list[int] = Field(min_length=1)


class AssignTestsToGroupResponse(BaseModel):
    group_id: int
    added_test_ids: list[int] = Field(default_factory=list)
    skipped_test_ids: list[int] = Field(default_factory=list)


class AssignGroupsToPackageRequest(BaseModel):
    group_ids: list[int] = Field(min_length=1)


class ReorderPackageGroupsRequest(BaseModel):
    group_ids: list[int] = Field(min_length=1)


class ReorderPackagesRequest(BaseModel):
    package_ids: list[int] = Field(min_length=1)


class AssignGroupsToPackageResponse(BaseModel):
    diagnostic_package_id: int
    added_group_ids: list[int] = Field(default_factory=list)
    skipped_group_ids: list[int] = Field(default_factory=list)


class PackageTestsResponse(BaseModel):
    diagnostic_package_id: int
    groups: list[TestGroupResponse] = Field(default_factory=list)


class SampleCreate(BaseModel):
    sample_type: SafeDisplayName
    description: OptionalSafeText = None
    display_order: Optional[int] = None


class SampleUpdate(BaseModel):
    sample_type: OptionalSafeDisplayName = None
    description: OptionalSafeText = None
    display_order: Optional[int] = None


class SampleResponse(BaseModel):
    sample_id: int
    diagnostic_package_id: int
    sample_type: str
    description: Optional[str] = None
    display_order: Optional[int] = None


class PreparationCreate(BaseModel):
    preparation_title: SafeDisplayName
    steps: Optional[list[str]] = None
    display_order: Optional[int] = None


class PreparationUpdate(BaseModel):
    preparation_title: OptionalSafeDisplayName = None
    steps: Optional[list[str]] = None
    display_order: Optional[int] = None


class PreparationResponse(BaseModel):
    preparation_id: int
    diagnostic_package_id: int
    preparation_title: str
    steps: Optional[list[str]] = None
    display_order: Optional[int] = None


class DiagnosticPackageDetailResponse(DiagnosticPackageResponse):
    reasons: list[ReasonResponse] = Field(default_factory=list)
    tags: list[TagResponse] = Field(default_factory=list)
    samples: list[SampleResponse] = Field(default_factory=list)
    preparations: list[PreparationResponse] = Field(default_factory=list)
    filter_chips: list[PackageFilterChipResponse] = Field(default_factory=list)


class DiagnosticPackageListItem(BaseModel):
    diagnostic_package_id: int
    package_name: str
    package_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    display_order: Optional[int] = None
    external_package_id: Optional[int] = None
    no_of_tests: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    discount_percent: Optional[int] = None
    is_most_popular: Optional[bool] = None
    complementary_consultation: Optional[dict[str, bool]] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[str] = None
    status: Optional[str] = None
    tags: list[TagResponse] = Field(default_factory=list)
    filter_chips: list[PackageFilterChipResponse] = Field(default_factory=list)
