"""Pydantic schemas for organizations APIs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class OrganizationDepartment(BaseModel):
    department: str
    slug: str


class OrganizationDepartmentInput(BaseModel):
    department: str = Field(min_length=1, max_length=100)


class OrganizationCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    organization_type: Optional[str] = Field(default=None, max_length=50)
    logo: Optional[str] = Field(default=None, max_length=500)
    website_url: Optional[str] = Field(default=None, max_length=500)
    address: Optional[str] = Field(default=None, max_length=2000)
    pin_code: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)

    contact_person_user_id: Optional[int] = Field(default=None, gt=0)
    bd_employee_id: Optional[int] = Field(default=None, gt=0)
    departments: Optional[list[OrganizationDepartmentInput]] = None


class OrganizationUpdateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    organization_type: Optional[str] = Field(default=None, max_length=50)
    logo: Optional[str] = Field(default=None, max_length=500)
    website_url: Optional[str] = Field(default=None, max_length=500)
    address: Optional[str] = Field(default=None, max_length=2000)
    pin_code: Optional[str] = Field(default=None, max_length=20)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)

    contact_person_user_id: Optional[int] = Field(default=None, gt=0)
    bd_employee_id: Optional[int] = Field(default=None, gt=0)
    departments: Optional[list[OrganizationDepartmentInput]] = None


class OrganizationStatusUpdateRequest(BaseModel):
    status: str = Field(min_length=1, max_length=30)


class OrganizationListItem(BaseModel):
    organization_id: int
    name: Optional[str] = None
    organization_type: Optional[str] = None
    logo: Optional[str] = None
    website_url: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
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

    contact_person_user_id: Optional[int] = None

    bd_employee_id: Optional[int] = None
    departments: Optional[list[OrganizationDepartment]] = None
    status: Optional[str] = None

    created_at: Optional[datetime] = None
    created_employee_id: Optional[int] = None
    updated_at: Optional[datetime] = None
    updated_employee_id: Optional[int] = None


class OrganizationParticipantItem(BaseModel):
    """Single participant in an organization (user enrolled in any engagement)."""

    user_id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    city: Optional[str] = None
    status: Optional[str] = None


class CampListItem(BaseModel):
    camp_no: int
    camp_name: str
    organization_id: int
    organization_name: str
    start_date: date
    engagement_count: int
