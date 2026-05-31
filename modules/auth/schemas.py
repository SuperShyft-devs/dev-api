"""Pydantic schemas for auth APIs."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class SendOtpRequest(BaseModel):
    phone: str | None = Field(None, min_length=10, max_length=32)
    email: str | None = Field(None, min_length=3, max_length=254)

    @model_validator(mode="after")
    def exactly_one_identifier(self) -> SendOtpRequest:
        has_phone = self.phone is not None and self.phone.strip() != ""
        has_email = self.email is not None and self.email.strip() != ""
        if has_phone == has_email:
            raise ValueError("Provide exactly one of phone or email")
        return self


class SendOtpResponse(BaseModel):
    session_id: int


class VerifyOtpRequest(BaseModel):
    phone: str | None = Field(None, min_length=5, max_length=32)
    email: str | None = Field(None, min_length=3, max_length=254)
    otp: str = Field(..., min_length=4, max_length=10)

    @model_validator(mode="after")
    def exactly_one_identifier(self) -> VerifyOtpRequest:
        has_phone = self.phone is not None and self.phone.strip() != ""
        has_email = self.email is not None and self.email.strip() != ""
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
