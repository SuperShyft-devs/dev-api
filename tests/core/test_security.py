"""
Tests for security utilities.
"""

from datetime import timedelta

import pytest

from core.security import (
    create_jwt_token,
    decode_and_verify_jwt,
    generate_secure_token,
    hash_token,
    verify_token_hash,
)


def test_generate_secure_token_returns_unique_values():
    """Secure tokens should be random and long enough."""
    token_one = generate_secure_token()
    token_two = generate_secure_token()

    assert token_one != token_two
    assert isinstance(token_one, str)
    assert len(token_one) >= 16


def test_generate_secure_token_rejects_short_length():
    """Token length must be at least 16 bytes."""
    with pytest.raises(ValueError, match="Token length must be at least 16"):
        generate_secure_token(8)


def test_hash_token_and_verify_round_trip():
    """Hashed tokens should verify successfully."""
    token = "sample-token"
    token_hash, salt = hash_token(token)

    assert token_hash
    assert salt
    assert verify_token_hash(token, token_hash, salt)
    assert not verify_token_hash("wrong-token", token_hash, salt)


def test_hash_token_requires_value():
    """Hashing should reject empty tokens."""
    with pytest.raises(ValueError, match="Token is required"):
        hash_token("")


def test_verify_token_hash_rejects_invalid_salt():
    """Verification should fail when salt is invalid."""
    assert not verify_token_hash("token", "hash", "not-base64")


def test_create_and_verify_jwt_token():
    """JWT tokens should decode to the original payload."""
    payload = {"sub": "user-123"}
    token = create_jwt_token(payload, timedelta(minutes=5), secret_key="secret")

    decoded = decode_and_verify_jwt(token, secret_key="secret")
    assert decoded["sub"] == "user-123"
    assert "exp" in decoded
    assert "iat" in decoded


def test_jwt_rejects_expired_token():
    """Expired JWT tokens should be rejected."""
    payload = {"sub": "user-123"}
    token = create_jwt_token(payload, timedelta(seconds=-1), secret_key="secret")

    with pytest.raises(ValueError, match="Token has expired"):
        decode_and_verify_jwt(token, secret_key="secret")


def test_jwt_rejects_invalid_signature():
    """Tokens with invalid signatures should be rejected."""
    payload = {"sub": "user-123"}
    token = create_jwt_token(payload, timedelta(minutes=5), secret_key="secret")

    with pytest.raises(ValueError, match="Invalid token signature"):
        decode_and_verify_jwt(token, secret_key="other-secret")


def test_jwt_rejects_invalid_format():
    """Tokens with invalid format should be rejected."""
    with pytest.raises(ValueError, match="Invalid token format"):
        decode_and_verify_jwt("not.a.jwt", secret_key="secret")
