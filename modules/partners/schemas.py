"""Pydantic schemas for partners APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, EmailStr, Field, model_validator

from common.validation import OptionalPhoneStr, OtpCode, PhoneStr, PositiveIntId, SafeDisplayName, StatusStr
from modules.partners.models import PartnerRole


class PartnerCreateRequest(BaseModel):
    name: SafeDisplayName
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)
    role: PartnerRole
    status: Optional[StatusStr] = "active"

    @model_validator(mode="after")
    def require_phone_or_email(self) -> PartnerCreateRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if not has_phone and not has_email:
            raise ValueError("Provide at least one of phone or email")
        return self


class PartnerUpdateRequest(BaseModel):
    name: SafeDisplayName
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)
    role: PartnerRole

    @model_validator(mode="after")
    def require_phone_or_email(self) -> PartnerUpdateRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if not has_phone and not has_email:
            raise ValueError("Provide at least one of phone or email")
        return self


class PartnerStatusUpdateRequest(BaseModel):
    status: StatusStr


class PartnerListItem(BaseModel):
    partner_id: int
    name: str
    phone: str | None = None
    email: str | None = None
    role: PartnerRole
    status: str


class PartnerDetailsResponse(PartnerListItem):
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class PartnerSendOtpRequest(BaseModel):
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)

    @model_validator(mode="after")
    def exactly_one_identifier(self) -> PartnerSendOtpRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if has_phone == has_email:
            raise ValueError("Provide exactly one of phone or email")
        return self


class PartnerVerifyOtpRequest(BaseModel):
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)
    otp: OtpCode

    @model_validator(mode="after")
    def exactly_one_identifier(self) -> PartnerVerifyOtpRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if has_phone == has_email:
            raise ValueError("Provide exactly one of phone or email")
        return self


class PartnerRefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., min_length=10)


class PartnerLogoutRequest(BaseModel):
    refresh_token: str = Field(..., min_length=10)


PartnerRoleFilter = Literal["phlebo", "expert", "organization_manager"]
