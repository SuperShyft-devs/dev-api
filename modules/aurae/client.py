"""HTTP client for the Aurae partner API."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from core.config import settings
from core.exceptions import AppError

logger = logging.getLogger(__name__)

_cached_token: str | None = None
_cached_token_at: float = 0.0
_TOKEN_TTL_SECONDS = 50 * 60  # Aurae tokens last ~1 hour; refresh early.


def clear_token_cache() -> None:
    """Drop the in-process Aurae token cache."""
    global _cached_token, _cached_token_at
    _cached_token = None
    _cached_token_at = 0.0


def _require_config() -> tuple[str, str, str]:
    base = (settings.AURAE_BASE_URL or "").rstrip("/")
    api_key = (settings.AURAE_API_KEY or "").strip()
    org_code = (settings.AURAE_ORG_CODE or "").strip()
    if not base or not api_key or not org_code:
        raise AppError(
            status_code=503,
            error_code="AURAE_NOT_CONFIGURED",
            message="Aurae integration is not configured",
        )
    return base, api_key, org_code


def token_url() -> str:
    base, _, org_code = _require_config()
    return f"{base}/token?org_code={org_code}"


def onboard_url() -> str:
    base, _, _ = _require_config()
    return f"{base}/onboard"


async def get_token(*, force_refresh: bool = False) -> str:
    """Fetch (and cache) an Aurae Bearer token."""
    global _cached_token, _cached_token_at

    if (
        not force_refresh
        and _cached_token
        and (time.monotonic() - _cached_token_at) < _TOKEN_TTL_SECONDS
    ):
        return _cached_token

    base, api_key, org_code = _require_config()
    url = f"{base}/token"
    timeout = settings.AURAE_TIMEOUT_SECONDS

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(
            url,
            params={"org_code": org_code},
            headers={"x-api-key": api_key},
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("Aurae token request failed: %s %s", resp.status_code, resp.text[:300])
            raise AppError(
                status_code=502,
                error_code="AURAE_TOKEN_FAILED",
                message="Failed to obtain Aurae auth token",
            ) from exc
        data = resp.json()

    token = (data.get("token") or "").strip() if isinstance(data, dict) else ""
    if not token:
        raise AppError(
            status_code=502,
            error_code="AURAE_TOKEN_FAILED",
            message="Aurae token response did not include a token",
        )
    _cached_token = token
    _cached_token_at = time.monotonic()
    return token


async def onboard_user(*, token: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Call Aurae POST /onboard and return the JSON body."""
    url = onboard_url()
    timeout = settings.AURAE_TIMEOUT_SECONDS

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("Aurae onboard failed: %s %s", resp.status_code, resp.text[:500])
            raise AppError(
                status_code=502,
                error_code="AURAE_ONBOARD_FAILED",
                message="Failed to onboard user with Aurae",
            ) from exc
        data = resp.json()

    if not isinstance(data, dict):
        raise AppError(
            status_code=502,
            error_code="AURAE_ONBOARD_FAILED",
            message="Unexpected Aurae onboard response",
        )
    return data
