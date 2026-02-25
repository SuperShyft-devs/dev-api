"""Pydantic schemas for organizations APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, Field


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

    contact_name: Optional[str] = Field(default=None, max_length=200)
    contact_email: Optional[EmailStr] = None
    contact_phone: Optional[str] = Field(default=None, max_length=30)
    contact_designation: Optional[str] = Field(default=None, max_length=100)

    bd_employee_id: Optional[int] = Field(default=None, gt=0)


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

    contact_name: Optional[str] = Field(default=None, max_length=200)
    contact_email: Optional[EmailStr] = None
    contact_phone: Optional[str] = Field(default=None, max_length=30)
    contact_designation: Optional[str] = Field(default=None, max_length=100)

    bd_employee_id: Optional[int] = Field(default=None, gt=0)


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

    contact_name: Optional[str] = None
    contact_email: Optional[EmailStr] = None
    contact_phone: Optional[str] = None
    contact_designation: Optional[str] = None

    bd_employee_id: Optional[int] = None
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
