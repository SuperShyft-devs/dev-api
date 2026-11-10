"""Legacy WhatAPI GET webhook fallback when template POST fails."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import httpx

from core.config import settings
from core.exceptions import AppError
from modules.notifications.whatapi_client import (
    format_whatapi_http_error,
    parse_http_error_body,
    whatapi_http_error_details,
)


def build_whatsapp_fallback_get_url(
    *,
    phone: str,
    message_csv: str,
    medialink: str | None = None,
    pdflink: str | None = None,
) -> str:
    base_url = (settings.WHATAPI_FALLBACK_WEBHOOK_URL or "").strip()
    if not base_url:
        raise RuntimeError("WHATAPI_FALLBACK_WEBHOOK_URL is not configured")
    params: dict[str, str] = {
        "number": phone,
        "message": message_csv,
    }
    if medialink:
        params["medialink"] = medialink
    if pdflink:
        params["pdflink"] = pdflink
    return f"{base_url}?{urlencode(params)}"


async def send_whatsapp_fallback_get(
    *,
    phone: str,
    message_csv: str,
    medialink: str | None = None,
    pdflink: str | None = None,
) -> dict[str, Any]:
    base_url = (settings.WHATAPI_FALLBACK_WEBHOOK_URL or "").strip()
    if not base_url:
        raise RuntimeError("WHATAPI_FALLBACK_WEBHOOK_URL is not configured")

    url = build_whatsapp_fallback_get_url(
        phone=phone,
        message_csv=message_csv,
        medialink=medialink,
        pdflink=pdflink,
    )
    timeout = settings.WHATAPI_FALLBACK_TIMEOUT_SECONDS
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            err_body = parse_http_error_body(exc.response)
            raise AppError(
                status_code=502,
                error_code="WHATAPI_FALLBACK_ERROR",
                message=format_whatapi_http_error(
                    http_method="GET",
                    url=url,
                    status_code=exc.response.status_code,
                    body=err_body,
                ),
                details=whatapi_http_error_details(
                    http_method="GET",
                    url=url,
                    status_code=exc.response.status_code,
                    body=err_body,
                ),
            ) from exc
        except httpx.HTTPError as exc:
            raise AppError(
                status_code=502,
                error_code="WHATAPI_FALLBACK_ERROR",
                message=f"GET {url} request failed",
            ) from exc
        try:
            body = response.json()
        except Exception:
            body = {"accepted": False, "raw": response.text}

    accepted = False
    if isinstance(body, dict):
        accepted = bool(body.get("accepted"))
    return {"accepted": accepted, "queue_id": None, "response": body, "url": url}
