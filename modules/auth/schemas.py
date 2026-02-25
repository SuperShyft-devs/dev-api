"""Pydantic schemas for auth APIs."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SendOtpRequest(BaseModel):
    phone: str = Field(..., min_length=5, max_length=20)


class SendOtpResponse(BaseModel):
    session_id: int


class VerifyOtpRequest(BaseModel):
    phone: str = Field(..., min_length=5, max_length=20)
    otp: str = Field(..., min_length=4, max_length=10)


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
