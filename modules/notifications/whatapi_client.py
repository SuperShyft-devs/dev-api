"""HTTP client for WhatAPI Meta WhatsApp template APIs."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import httpx

from core.config import settings
from core.exceptions import AppError


def _whatapi_base() -> str:
    base = (settings.WHATAPI_DOMAIN or "").strip().rstrip("/")
    if not base:
        raise AppError(
            status_code=500,
            error_code="CONFIG_ERROR",
            message="WHATAPI_DOMAIN is not configured",
        )
    return base


def _auth_headers() -> dict[str, str]:
    """WhatAPI Meta routes accept Bearer access tokens; API-KEY is for other key types."""
    access = (settings.WHATAPI_ACCESS_TOKEN or "").strip()
    if access:
        return {"Authorization": f"Bearer {access}"}
    api_key = (settings.WHATAPI_API_KEY or "").strip()
    if api_key:
        return {"API-KEY": api_key}
    raise AppError(
        status_code=500,
        error_code="CONFIG_ERROR",
        message="WHATAPI_ACCESS_TOKEN or WHATAPI_API_KEY is not configured",
    )


def _meta_path(*parts: str) -> str:
    version = (settings.WHATAPI_VERSION or "v19.0").strip().strip("/")
    return f"{_whatapi_base()}/api/meta/{version}/{'/'.join(parts)}"


def message_templates_request_url(*, template_name: str) -> str:
    """Full GET URL for Meta message template lookup (for integration sync logs)."""
    waba_id = (settings.WHATAPI_WABA_ID or "").strip()
    if not waba_id:
        raise AppError(
            status_code=500,
            error_code="CONFIG_ERROR",
            message="WHATAPI_WABA_ID is not configured",
        )
    base = _meta_path(waba_id, "message_templates")
    return f"{base}?{urlencode({'name': (template_name or '').strip()})}"


def send_messages_request_url() -> str:
    """Full POST URL for sending a WhatsApp template message."""
    phone_number_id = (settings.WHATAPI_PHONE_NUMBER_ID or "").strip()
    if not phone_number_id:
        raise AppError(
            status_code=500,
            error_code="CONFIG_ERROR",
            message="WHATAPI_PHONE_NUMBER_ID is not configured",
        )
    return _meta_path(phone_number_id, "messages")


def whatapi_auth_descriptor() -> dict[str, str]:
    access = (settings.WHATAPI_ACCESS_TOKEN or "").strip()
    if access:
        return {"header": "Authorization", "scheme": "Bearer", "value": "(redacted)"}
    return {"header": "API-KEY", "value": "(redacted)"}


def parse_http_error_body(response: httpx.Response) -> dict[str, Any] | list[Any] | None:
    """Best-effort JSON (or short text) from a failed WhatAPI HTTP response."""
    try:
        parsed = response.json()
        if isinstance(parsed, (dict, list)):
            return parsed
    except Exception:
        pass
    text = (response.text or "").strip()
    if not text:
        return None
    return {"raw": text[:2000]}


def whatapi_error_summary(body: dict[str, Any] | list[Any] | None) -> str | None:
    if not isinstance(body, dict):
        return None
    for key in ("error", "message", "error_message", "detail"):
        value = body.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def format_whatapi_http_error(
    *,
    http_method: str,
    url: str,
    status_code: int,
    body: dict[str, Any] | list[Any] | None,
) -> str:
    summary = whatapi_error_summary(body) if isinstance(body, dict) else None
    base = f"{http_method} {url} → HTTP {status_code}"
    if summary:
        return f"{base}: {summary}"
    return base


def whatapi_http_error_details(
    *,
    http_method: str,
    url: str,
    status_code: int,
    body: dict[str, Any] | list[Any] | None,
) -> dict[str, Any]:
    return {
        "http_method": http_method,
        "url": url,
        "http_status": status_code,
        "response_body": body,
    }


class WhatApiClient:
    """Fetch templates and send WhatsApp template messages via WhatAPI."""

    async def fetch_message_template(self, *, template_name: str) -> dict[str, Any]:
        waba_id = (settings.WHATAPI_WABA_ID or "").strip()
        if not waba_id:
            raise AppError(
                status_code=500,
                error_code="CONFIG_ERROR",
                message="WHATAPI_WABA_ID is not configured",
            )
        url = message_templates_request_url(template_name=template_name)
        try:
            async with httpx.AsyncClient(
                timeout=settings.WHATAPI_TIMEOUT_SECONDS
            ) as client:
                response = await client.get(
                    url, headers=_auth_headers()
                )
                response.raise_for_status()
                body = response.json()
        except httpx.HTTPStatusError as exc:
            err_body = parse_http_error_body(exc.response)
            raise AppError(
                status_code=502,
                error_code="WHATAPI_ERROR",
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
                error_code="WHATAPI_ERROR",
                message=f"GET {url} request failed",
            ) from exc

        if not isinstance(body, dict):
            raise AppError(
                status_code=502,
                error_code="WHATAPI_ERROR",
                message="WhatAPI template fetch returned an invalid response",
            )
        return body

    async def send_template_message(
        self,
        *,
        to: str,
        template_name: str,
        language_code: str = "en",
        components: list[dict[str, Any]] | None = None,
    ) -> dict[str, str]:
        phone_number_id = (settings.WHATAPI_PHONE_NUMBER_ID or "").strip()
        if not phone_number_id:
            raise AppError(
                status_code=500,
                error_code="CONFIG_ERROR",
                message="WHATAPI_PHONE_NUMBER_ID is not configured",
            )

        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"policy": "deterministic", "code": language_code},
            },
        }
        if components:
            payload["template"]["components"] = components

        url = send_messages_request_url()
        try:
            async with httpx.AsyncClient(
                timeout=settings.WHATAPI_TIMEOUT_SECONDS
            ) as client:
                response = await client.post(
                    url, json=payload, headers=_auth_headers()
                )
                response.raise_for_status()
                body = response.json()
        except httpx.HTTPStatusError as exc:
            err_body = parse_http_error_body(exc.response)
            raise AppError(
                status_code=502,
                error_code="WHATAPI_ERROR",
                message=format_whatapi_http_error(
                    http_method="POST",
                    url=url,
                    status_code=exc.response.status_code,
                    body=err_body,
                ),
                details=whatapi_http_error_details(
                    http_method="POST",
                    url=url,
                    status_code=exc.response.status_code,
                    body=err_body,
                ),
            ) from exc
        except httpx.HTTPError as exc:
            raise AppError(
                status_code=502,
                error_code="WHATAPI_ERROR",
                message=f"POST {url} request failed",
            ) from exc

        if not isinstance(body, dict):
            raise AppError(
                status_code=502,
                error_code="WHATAPI_ERROR",
                message="WhatAPI send returned an invalid response",
            )

        message = body.get("message")
        if not isinstance(message, dict):
            raise AppError(
                status_code=502,
                error_code="WHATAPI_ERROR",
                message="WhatAPI send response missing message object",
            )
        queue_id = message.get("queue_id")
        message_status = message.get("message_status")
        if not queue_id or not isinstance(queue_id, str):
            raise AppError(
                status_code=502,
                error_code="WHATAPI_ERROR",
                message="WhatAPI send response missing queue_id",
            )
        status_str = str(message_status or "queued")
        return {"queue_id": queue_id, "message_status": status_str}


def get_whatapi_client() -> WhatApiClient:
    return WhatApiClient()
