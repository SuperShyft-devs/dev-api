"""Legacy WhatAPI GET webhook fallback when template POST fails."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import httpx

from core.config import settings


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
            raise RuntimeError(f"GET {url} → HTTP {exc.response.status_code}") from exc
        except httpx.HTTPError as exc:
            raise RuntimeError(f"GET {url} request failed") from exc
        try:
            body = response.json()
        except Exception:
            body = {"accepted": False, "raw": response.text}

    accepted = False
    if isinstance(body, dict):
        accepted = bool(body.get("accepted"))
    return {"accepted": accepted, "queue_id": None, "response": body, "url": url}
