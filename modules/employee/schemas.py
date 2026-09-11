"""Pydantic schemas for employee APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, EmailStr, Field, model_validator

from common.validation import (
    OptionalPhoneStr,
    OtpCode,
    SafeDisplayName,
    StatusStr,
)
from modules.employee.models import EmployeeRole


StaffEmployeeRole = Literal["admin", "inferior_admin"]


class TaskGrantRequest(BaseModel):
    task_key: str = Field(..., min_length=1, max_length=80)
    can_view: bool = False
    can_edit: bool = False


class CategoryGrantRequest(BaseModel):
    category_key: str = Field(..., min_length=1, max_length=50)
    can_view: bool = False
    can_edit: bool = False
    tasks: list[TaskGrantRequest] | None = None


class EmployeeCreateRequest(BaseModel):
    name: SafeDisplayName
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)
    role: StaffEmployeeRole
    status: Optional[StatusStr] = "active"
    permissions: list[CategoryGrantRequest] | None = None

    @model_validator(mode="after")
    def require_phone_or_email(self) -> EmployeeCreateRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if not has_phone and not has_email:
            raise ValueError("Provide at least one of phone or email")
        return self


class EmployeeUpdateRequest(BaseModel):
    name: SafeDisplayName
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)
    role: StaffEmployeeRole
    expected_version: int | None = Field(default=None, ge=1)
    permissions: list[CategoryGrantRequest] | None = None

    @model_validator(mode="after")
    def require_phone_or_email(self) -> EmployeeUpdateRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if not has_phone and not has_email:
            raise ValueError("Provide at least one of phone or email")
        return self


class EmployeeStatusUpdateRequest(BaseModel):
    status: StatusStr


class EmployeeListItem(BaseModel):
    employee_id: int
    name: str
    phone: str | None = None
    email: str | None = None
    role: Optional[EmployeeRole] = None
    status: Optional[str] = None


class EmployeeDetailsResponse(EmployeeListItem):
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ReplaceEmployeePermissionsRequest(BaseModel):
    expected_version: int = Field(..., ge=1)
    permissions: list[CategoryGrantRequest] = Field(default_factory=list)


class EmployeeSendOtpRequest(BaseModel):
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)

    @model_validator(mode="after")
    def exactly_one_identifier(self) -> EmployeeSendOtpRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if has_phone == has_email:
            raise ValueError("Provide exactly one of phone or email")
        return self


class EmployeeVerifyOtpRequest(BaseModel):
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)
    otp: OtpCode

    @model_validator(mode="after")
    def exactly_one_identifier(self) -> EmployeeVerifyOtpRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if has_phone == has_email:
            raise ValueError("Provide exactly one of phone or email")
        return self


class EmployeeRefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., min_length=10)


class EmployeeLogoutRequest(BaseModel):
    refresh_token: str = Field(..., min_length=10)
