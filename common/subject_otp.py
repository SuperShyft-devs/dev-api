"""Shared OTP hashing / bypass helpers for subject auth (user / employee / partner)."""

from __future__ import annotations

import hashlib
import hmac

from core.config import settings


def hash_otp(otp: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), otp.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_otp(otp: str, otp_hash: str, secret: str) -> bool:
    return hmac.compare_digest(hash_otp(otp, secret), otp_hash)


def per_phone_bypass_allowed(phone_candidates: list[str], otp: str) -> bool:
    index = settings.get_bypass_otp_by_phone()
    if not index:
        return False
    for candidate in phone_candidates:
        expected = index.get(candidate)
        if expected and hmac.compare_digest(otp, expected):
            return True
    return False


def global_bypass_allowed(otp: str) -> bool:
    return bool(
        settings.ALLOW_BYPASS_OTP
        and settings.BYPASS_OTP
        and hmac.compare_digest(otp, settings.BYPASS_OTP)
    )


def hash_refresh_token(refresh_token: str) -> str:
    secret = settings.get_refresh_token_secret()
    if not secret:
        raise ValueError("Refresh token secret is missing")
    return hmac.new(secret.encode("utf-8"), refresh_token.encode("utf-8"), hashlib.sha256).hexdigest()
