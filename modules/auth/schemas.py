"""Pydantic schemas for auth APIs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, EmailStr, Field, model_validator

from common.validation import OtpCode, OptionalPhoneStr, PhoneStr


class SendOtpRequest(BaseModel):
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)

    @model_validator(mode="after")
    def exactly_one_identifier(self) -> SendOtpRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if has_phone == has_email:
            raise ValueError("Provide exactly one of phone or email")
        return self


class SendOtpResponse(BaseModel):
    session_id: int


ResendOtpVia = Literal["email", "whatsapp"]


class ResendOtpRequest(BaseModel):
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)
    via: ResendOtpVia | None = None

    @model_validator(mode="after")
    def at_least_one_identifier(self) -> ResendOtpRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if not has_phone and not has_email:
            raise ValueError("Provide at least one of phone or email")
        return self


class ResendOtpResponse(BaseModel):
    session_id: int


class VerifyOtpRequest(BaseModel):
    phone: OptionalPhoneStr = None
    email: EmailStr | None = Field(default=None, max_length=254)
    otp: OtpCode

    @model_validator(mode="after")
    def exactly_one_identifier(self) -> VerifyOtpRequest:
        has_phone = self.phone is not None and str(self.phone).strip() != ""
        has_email = self.email is not None and str(self.email).strip() != ""
        if has_phone == has_email:
            raise ValueError("Provide exactly one of phone or email")
        return self


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class VerifyOtpResponse(BaseModel):
    user_id: int
    tokens: TokenPair


class RefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., min_length=10)


class RefreshTokenResponse(BaseModel):
    tokens: TokenPair


class LogoutRequest(BaseModel):
    refresh_token: str = Field(..., min_length=10)


class LogoutResponse(BaseModel):
    success: bool
