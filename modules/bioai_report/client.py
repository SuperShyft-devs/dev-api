"""HTTP client for the bio-ai-reports PDF registration service."""

from __future__ import annotations

from typing import Any

import httpx

from core.config import settings
from core.exceptions import AppError


class BioAiReportsClient:
    """Register BioReport JSON payloads and receive permanent secret PDF links."""

    async def register_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        base = (settings.BIO_AI_REPORTS_BASE_URL or "").strip().rstrip("/")
        if not base:
            raise AppError(
                status_code=500,
                error_code="CONFIG_ERROR",
                message="BIO_AI_REPORTS_BASE_URL is not configured",
            )

        url = f"{base}/api/reports"
        try:
            async with httpx.AsyncClient(timeout=settings.BIO_AI_REPORTS_TIMEOUT_SECONDS) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                body = response.json()
        except httpx.HTTPStatusError as exc:
            raise AppError(
                status_code=502,
                error_code="BIO_AI_REPORTS_ERROR",
                message=f"bio-ai-reports returned HTTP {exc.response.status_code}",
            ) from exc
        except httpx.HTTPError as exc:
            raise AppError(
                status_code=502,
                error_code="BIO_AI_REPORTS_ERROR",
                message="bio-ai-reports request failed",
            ) from exc

        if not isinstance(body, dict):
            raise AppError(
                status_code=502,
                error_code="BIO_AI_REPORTS_ERROR",
                message="bio-ai-reports returned an invalid response",
            )
        return body
