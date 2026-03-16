"""Pydantic schemas for diagnostics APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class DiagnosticPackageCreate(BaseModel):
    package_name: str = Field(min_length=1)
    diagnostic_provider: Optional[str] = None
    no_of_tests: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    about_text: Optional[str] = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None
    reference_id: Optional[str] = None


class DiagnosticPackageUpdate(BaseModel):
    package_name: Optional[str] = Field(default=None, min_length=1)
    diagnostic_provider: Optional[str] = None
    no_of_tests: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    about_text: Optional[str] = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None
    reference_id: Optional[str] = None


class DiagnosticPackageStatusUpdate(BaseModel):
    status: str = Field(min_length=1)


class DiagnosticPackageResponse(BaseModel):
    diagnostic_package_id: int
    reference_id: Optional[str] = None
    package_name: str
    diagnostic_provider: Optional[str] = None
    no_of_tests: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    about_text: Optional[str] = None
    bookings_count: Optional[int] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    discount_percent: Optional[int] = None


class FilterCreate(BaseModel):
    filter_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    display_order: Optional[int] = None
    filter_type: Optional[str] = None


class FilterUpdate(BaseModel):
    filter_key: Optional[str] = None
    display_name: Optional[str] = None
    display_order: Optional[int] = None
    filter_type: Optional[str] = None
    status: Optional[str] = None


class FilterResponse(BaseModel):
    filter_id: int
    filter_key: str
    display_name: str
    display_order: Optional[int] = None
    filter_type: Optional[str] = None
    status: Optional[str] = None


class ReasonCreate(BaseModel):
    reason_text: str = Field(min_length=1)
    display_order: Optional[int] = None


class ReasonUpdate(BaseModel):
    reason_text: Optional[str] = None
    display_order: Optional[int] = None


class ReasonResponse(BaseModel):
    reason_id: int
    diagnostic_package_id: int
    reason_text: str
    display_order: Optional[int] = None


class TagCreate(BaseModel):
    tag_name: str = Field(min_length=1)
    display_order: Optional[int] = None


class TagResponse(BaseModel):
    tag_id: int
    diagnostic_package_id: int
    tag_name: str
    display_order: Optional[int] = None


class TestCreate(BaseModel):
    test_name: str = Field(min_length=1)
    display_order: Optional[int] = None
    is_available: Optional[bool] = True


class TestUpdate(BaseModel):
    test_name: Optional[str] = None
    display_order: Optional[int] = None
    is_available: Optional[bool] = None


class TestResponse(BaseModel):
    test_id: int
    group_id: int
    test_name: str
    display_order: Optional[int] = None
    is_available: Optional[bool] = None


class TestGroupCreate(BaseModel):
    group_name: str = Field(min_length=1)
    test_count: Optional[int] = None
    display_order: Optional[int] = None


class TestGroupUpdate(BaseModel):
    group_name: Optional[str] = None
    test_count: Optional[int] = None
    display_order: Optional[int] = None


class TestGroupResponse(BaseModel):
    group_id: int
    diagnostic_package_id: int
    group_name: str
    test_count: Optional[int] = None
    display_order: Optional[int] = None
    tests: list[TestResponse] = Field(default_factory=list)


class SampleCreate(BaseModel):
    sample_type: str = Field(min_length=1)
    description: Optional[str] = None
    display_order: Optional[int] = None


class SampleUpdate(BaseModel):
    sample_type: Optional[str] = None
    description: Optional[str] = None
    display_order: Optional[int] = None


class SampleResponse(BaseModel):
    sample_id: int
    diagnostic_package_id: int
    sample_type: str
    description: Optional[str] = None
    display_order: Optional[int] = None


class PreparationCreate(BaseModel):
    preparation_title: str = Field(min_length=1)
    steps: Optional[list[str]] = None
    display_order: Optional[int] = None


class PreparationUpdate(BaseModel):
    preparation_title: Optional[str] = None
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


class DiagnosticPackageListItem(BaseModel):
    diagnostic_package_id: int
    package_name: str
    no_of_tests: Optional[int] = None
    report_duration_hours: Optional[int] = None
    collection_type: Optional[str] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    discount_percent: Optional[int] = None
    is_most_popular: Optional[bool] = None
    gender_suitability: Optional[str] = None
    status: Optional[str] = None
    tags: list[TagResponse] = Field(default_factory=list)
