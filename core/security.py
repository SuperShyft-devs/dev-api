"""
Security utilities for tokens and hashing.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import secrets
from jose import jwt
from jose.exceptions import ExpiredSignatureError, JWTError

from core.config import settings


_ALLOWED_ALGORITHM = "HS256"


def _base64url_encode(data: bytes) -> str:
    """Encode bytes using base64url without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def _base64url_decode(data: str) -> bytes:
    """Decode base64url data with padding restored."""
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}")


def create_jwt_token(
    payload: Dict[str, Any],
    expires_delta: timedelta,
    secret_key: Optional[str] = None,
    algorithm: str = _ALLOWED_ALGORITHM,
) -> str:
    """Create a signed JWT using a standard library (python-jose)."""
    if algorithm != _ALLOWED_ALGORITHM:
        raise ValueError("Unsupported algorithm")

    secret = secret_key or settings.JWT_SECRET_KEY
    if not secret:
        raise ValueError("JWT secret key is missing")

    now = datetime.now(timezone.utc)
    payload_copy = dict(payload)
    payload_copy["iat"] = int(now.timestamp())
    payload_copy["exp"] = int((now + expires_delta).timestamp())

    return jwt.encode(payload_copy, secret, algorithm=_ALLOWED_ALGORITHM)


def decode_and_verify_jwt(
    token: str,
    secret_key: Optional[str] = None,
    algorithm: str = _ALLOWED_ALGORITHM,
) -> Dict[str, Any]:
    """Decode and verify a JWT using a standard library (python-jose)."""
    if algorithm != _ALLOWED_ALGORITHM:
        raise ValueError("Unsupported algorithm")

    secret = secret_key or settings.JWT_SECRET_KEY
    if not secret:
        raise ValueError("JWT secret key is missing")

    # Keep error messages stable for callers and tests.
    try:
        payload = jwt.decode(token, secret, algorithms=[_ALLOWED_ALGORITHM])
    except ExpiredSignatureError as exc:
        raise ValueError("Token has expired") from exc
    except JWTError as exc:
        message = str(exc).lower()

        # Format issues are usually "not enough segments".
        if "segments" in message or "format" in message:
            raise ValueError("Invalid token format") from exc

        # Signature / key problems.
        if "signature" in message or "verify" in message or "key" in message:
            raise ValueError("Invalid token signature") from exc

        raise ValueError("Invalid token format") from exc

    return payload


def generate_secure_token(length: int = 32) -> str:
    """Generate a secure random token."""
    if length < 16:
        raise ValueError("Token length must be at least 16")
    return secrets.token_urlsafe(length)


def hash_token(token: str, salt: Optional[bytes] = None) -> Tuple[str, str]:
    """Hash a token with a random salt using SHA-256."""
    if not token:
        raise ValueError("Token is required")

    salt_bytes = salt or secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", token.encode("utf-8"), salt_bytes, 100_000)
    return _base64url_encode(derived), _base64url_encode(salt_bytes)


def verify_token_hash(token: str, token_hash: str, salt: str) -> bool:
    """Verify a token against a stored hash and salt."""
    if not token:
        return False

    try:
        salt_bytes = _base64url_decode(salt)
    except ValueError:
        return False

    derived = hashlib.pbkdf2_hmac("sha256", token.encode("utf-8"), salt_bytes, 100_000)
    derived_hash = _base64url_encode(derived)
    return hmac.compare_digest(derived_hash, token_hash)
