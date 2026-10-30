"""Pydantic schemas for diagnostics APIs."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from common.validation import (
    DiagnosticPackageName,
    OptionalDiagnosticPackageMultilineText,
    OptionalDiagnosticPackageName,
    OptionalParameterKey,
    OptionalSafeDisplayName,
    OptionalSafeText,
    OptionalSlugKey,
    PositiveIntId,
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


class DiagnosticPackagePeerResponse(BaseModel):
    diagnostic_package_id: int
    package_name: str
    diagnostic_provider: Optional[str] = None
    price: Optional[float] = None
    status: Optional[str] = None
    is_default_for_group: Optional[bool] = None


class DiagnosticWarning(BaseModel):
    code: str
    message: str


class TestGroupPeerResponse(BaseModel):
    group_id: int
    group_name: str
    diagnostic_provider: Optional[str] = None
    price: Optional[float] = None
    is_default_for_group: Optional[bool] = None


class DiagnosticPackageCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    package_name: DiagnosticPackageName
    package_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    external_package_code: Optional[str] = None
    custom: bool = False
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    health_areas_covered: OptionalDiagnosticPackageMultilineText = None
    about_text: OptionalDiagnosticPackageMultilineText = None
    same_as_package_id: PositiveIntId | None = None
    copy_test_groups_from_package_id: PositiveIntId | None = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    min_price: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("min_price", "minimum_price"),
    )
    is_most_popular: Optional[bool] = None
    is_default_for_group: Optional[bool] = None
    complementary_consultation: Optional[dict[str, bool]] = None
    gender_suitability: Optional[str] = None
    reference_id: Optional[str] = None
    package_for: Optional[PackageForType] = None


class DiagnosticPackageUpdate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    package_name: OptionalDiagnosticPackageName = None
    package_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    external_package_code: Optional[str] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    health_areas_covered: OptionalDiagnosticPackageMultilineText = None
    about_text: OptionalDiagnosticPackageMultilineText = None
    same_as_package_id: int | None = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    min_price: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("min_price", "minimum_price"),
    )
    is_most_popular: Optional[bool] = None
    is_default_for_group: Optional[bool] = None
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
    external_package_code: Optional[str] = None
    created_by_user_id: Optional[int] = None
    no_of_tests: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    health_areas_covered: Optional[str] = None
    about_text: Optional[str] = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    min_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    is_default_for_group: Optional[bool] = None
    has_allergy_screening: Optional[bool] = None
    has_cancer_screening: Optional[bool] = None
    complementary_consultation: Optional[dict[str, bool]] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    discount_percent: Optional[int] = None
    package_group_id: Optional[int] = None
    same_packages: list[DiagnosticPackagePeerResponse] = Field(default_factory=list)


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
    healthians_parameter_key: Optional[str] = None
    orangehealth_parameter_key: Optional[str] = None
    parameter_key: OptionalParameterKey = None
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
    healthians_parameter_key: Optional[str] = None
    orangehealth_parameter_key: Optional[str] = None
    parameter_key: OptionalParameterKey = None
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
    healthians_parameter_key: Optional[str] = None
    orangehealth_parameter_key: Optional[str] = None
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
    test_group_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    external_test_group_code: Optional[str] = None
    same_as_group_id: PositiveIntId | None = None
    copy_tests_from_group_id: PositiveIntId | None = None
    display_order: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    has_allergy_screening: Optional[bool] = None
    has_cancer_screening: Optional[bool] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[PackageForType] = None
    is_default_for_group: Optional[bool] = None


class TestGroupUpdate(BaseModel):
    group_name: OptionalSafeDisplayName = None
    group_key: OptionalSlugKey = None
    test_group_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    external_test_group_code: Optional[str] = None
    same_as_group_id: int | None = None
    display_order: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    has_allergy_screening: Optional[bool] = None
    has_cancer_screening: Optional[bool] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[PackageForType] = None
    is_default_for_group: Optional[bool] = None


class TestGroupResponse(BaseModel):
    group_id: int
    group_name: str
    group_key: str
    profile_group_id: Optional[int] = None
    test_group_image: Optional[str] = None
    diagnostic_provider: Optional[str] = None
    external_test_group_code: Optional[str] = None
    is_default_for_group: Optional[bool] = None
    test_count: int
    display_order: Optional[int] = None
    price: Optional[float] = None
    discount: Optional[str] = None
    discount_percent: Optional[int] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    has_allergy_screening: Optional[bool] = None
    has_cancer_screening: Optional[bool] = None
    gender_suitability: Optional[str] = None
    package_for: Optional[str] = None
    forked_from_group_id: Optional[int] = None
    same_test_groups: list[TestGroupPeerResponse] = Field(default_factory=list)
    tests: list[HealthParameterResponse] = Field(default_factory=list)
    filter_chips: list[PackageFilterChipResponse] = Field(default_factory=list)


class TestGroupProfileGroupListItem(BaseModel):
    group_id: int
    group_name: str
    test_count: int
    price: Optional[float] = None
    discount: Optional[str] = None
    discount_percent: Optional[int] = None
    gender_suitability: Optional[str] = None
    profile_group_id: Optional[int] = None
    diagnostic_provider: Optional[str] = None
    profile_group: dict[str, int] = Field(default_factory=dict)


class TestGroupPackageListItem(BaseModel):
    diagnostic_package_id: int
    package_name: str
    diagnostic_provider: Optional[str] = None
    status: Optional[str] = None


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


class ReorderFilterChipsRequest(BaseModel):
    filter_chip_ids: list[int] = Field(min_length=1)


class ReorderPackageReasonsRequest(BaseModel):
    reason_ids: list[int] = Field(min_length=1)


class AssignGroupsToPackageResponse(BaseModel):
    diagnostic_package_id: int
    added_group_ids: list[int] = Field(default_factory=list)
    skipped_group_ids: list[int] = Field(default_factory=list)


class ForkTestGroupForPackageResponse(BaseModel):
    group_id: int
    forked: bool


class PackageTestsResponse(BaseModel):
    diagnostic_package_id: int
    groups: list[TestGroupResponse] = Field(default_factory=list)


class ProviderTestDiffCounts(BaseModel):
    matched: int
    only_in_package: int
    only_at_provider: int
    not_linked: int


class ProviderTestDiffMatchedItem(BaseModel):
    test_id: int
    test_name: str
    group_names: list[str] = Field(default_factory=list)
    provider_id: str
    provider_name: str
    shared_provider_id: bool = False


class ProviderTestDiffOnlyInPackageItem(BaseModel):
    test_id: int
    test_name: str
    group_names: list[str] = Field(default_factory=list)
    provider_id: str


class ProviderTestDiffNotLinkedItem(BaseModel):
    test_id: int
    test_name: str
    group_names: list[str] = Field(default_factory=list)


class ProviderTestDiffOnlyAtProviderItem(BaseModel):
    provider_id: str
    provider_name: str
    catalog_test_id: int | None = None
    catalog_test_name: str | None = None


class ProviderTestDiffResponse(BaseModel):
    status: str
    healthians_package_name: str | None = None
    external_package_code: str
    counts: ProviderTestDiffCounts
    matched: list[ProviderTestDiffMatchedItem] = Field(default_factory=list)
    only_in_package: list[ProviderTestDiffOnlyInPackageItem] = Field(default_factory=list)
    not_linked: list[ProviderTestDiffNotLinkedItem] = Field(default_factory=list)
    only_at_provider: list[ProviderTestDiffOnlyAtProviderItem] = Field(default_factory=list)


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
    no_of_tests: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    min_price: Optional[float] = None
    discount_percent: Optional[int] = None
    is_most_popular: Optional[bool] = None
    has_allergy_screening: Optional[bool] = None
    has_cancer_screening: Optional[bool] = None
    complementary_consultation: Optional[dict[str, bool]] = None
    gender_suitability: Optional[str] = None
    tags: list[TagResponse] = Field(default_factory=list)
    filter_chips: list[PackageFilterChipResponse] = Field(default_factory=list)
    package_group_id: Optional[int] = None


class DiagnosticPackageGroupListItem(DiagnosticPackageListItem):
    package_group: dict[str, int] = Field(default_factory=dict)
    has_unmapped_tests: bool = False
    warnings: list[DiagnosticWarning] = Field(default_factory=list)
