"""Pydantic schemas for organizations APIs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from common.validation import (
    OptionalCityStateCountry,
    OptionalOrgAddressText,
    OptionalPinCode,
    OptionalSafeDisplayName,
    OptionalSlugKey,
    PositiveIntId,
    SafeDisplayName,
    StatusStr,
)


class IndustryItem(BaseModel):
    id: int
    industry_key: str
    industry: str


class IndustryCreateRequest(BaseModel):
    industry: SafeDisplayName


class IndustryUpdateRequest(BaseModel):
    industry: SafeDisplayName


class OrganizationDepartment(BaseModel):
    department: str
    slug: str


class OrganizationDepartmentInput(BaseModel):
    department: SafeDisplayName


class OrganizationCreateRequest(BaseModel):
    name: SafeDisplayName
    organization_type: Optional[str] = Field(default=None, max_length=50)
    logo: Optional[str] = Field(default=None, max_length=500)
    website_url: Optional[str] = Field(default=None, max_length=500)
    address: OptionalOrgAddressText = None
    pin_code: OptionalPinCode = None
    city: OptionalCityStateCountry = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None

    contact_person_user_ids: Optional[dict[str, Any]] = None
    bd_employee_id: Optional[PositiveIntId] = None
    departments: Optional[list[OrganizationDepartmentInput]] = None
    industry_key: OptionalSlugKey = None


class OrganizationUpdateRequest(BaseModel):
    name: SafeDisplayName
    organization_type: Optional[str] = Field(default=None, max_length=50)
    logo: Optional[str] = Field(default=None, max_length=500)
    website_url: Optional[str] = Field(default=None, max_length=500)
    address: OptionalOrgAddressText = None
    pin_code: OptionalPinCode = None
    city: OptionalCityStateCountry = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None

    contact_person_user_ids: Optional[dict[str, Any]] = None
    bd_employee_id: Optional[PositiveIntId] = None
    departments: Optional[list[OrganizationDepartmentInput]] = None
    industry_key: OptionalSlugKey = None


class OrganizationStatusUpdateRequest(BaseModel):
    status: StatusStr


class OrganizationListItem(BaseModel):
    organization_id: int
    name: Optional[str] = None
    organization_type: Optional[str] = None
    logo: Optional[str] = None
    website_url: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    industry_key: Optional[str] = None
    industry: Optional[str] = None
    status: Optional[str] = None


class OrganizationDetailsResponse(BaseModel):
    organization_id: int
    name: Optional[str] = None
    organization_type: Optional[str] = None
    logo: Optional[str] = None
    website_url: Optional[str] = None
    address: Optional[str] = None
    pin_code: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    industry_key: Optional[str] = None
    industry: Optional[str] = None

    contact_person_user_ids: Optional[dict[str, Any]] = None

    bd_employee_id: Optional[int] = None
    departments: Optional[list[OrganizationDepartment]] = None
    status: Optional[str] = None

    created_at: Optional[datetime] = None
    created_employee_id: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_employee_id: Optional[int] = None


class MyOrganizationListItem(OrganizationDetailsResponse):
    camp_cities: list[str] = []
    report_access: dict[str, Any] = {}


class OrganizationParticipantItem(BaseModel):
    """Single participant in an organization (user enrolled in any engagement)."""

    user_id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    city: Optional[str] = None
    status: Optional[str] = None


class CampDepartmentItem(BaseModel):
    name: str
    slug: str


class CampDepartmentsPayload(BaseModel):
    count: int
    departments: list[CampDepartmentItem]


class CampCitiesPayload(BaseModel):
    count: int
    cities: list[str]


class CampListItem(BaseModel):
    camp_no: int
    camp_name: str
    start_date: date
    year: int
    organization_id: int
    organization_name: str
    organization_logo: Optional[str] = None
    engagement_ids: list[int]
    departments: CampDepartmentsPayload
    cities: CampCitiesPayload


class CampRemapRequest(BaseModel):
    new_camp_no: PositiveIntId
