"""Integration sync log helpers for WhatAPI outbound and inbound traffic."""

from __future__ import annotations

from typing import Any

from modules.audit.cron_sync_logging import (
    finalize_integration_sync_log_isolated,
    persist_integration_sync_log_isolated,
)
from modules.notifications.whatapi_client import message_templates_request_url


def compact_template_response(body: dict[str, Any]) -> dict[str, Any]:
    """Keep template GET responses useful in admin without huge component blobs."""
    if not isinstance(body, dict):
        return {"data": body}
    data = body.get("data")
    if not isinstance(data, list):
        return body
    compact: list[dict[str, Any]] = []
    for item in data[:5]:
        if not isinstance(item, dict):
            continue
        components = item.get("components")
        compact.append(
            {
                "name": item.get("name"),
                "language": item.get("language"),
                "status": item.get("status"),
                "category": item.get("category"),
                "parameter_format": item.get("parameter_format"),
                "component_count": len(components) if isinstance(components, list) else 0,
                "components": components if isinstance(components, list) else [],
            }
        )
    return {"data": compact, "paging": body.get("paging")}


async def log_whatapi_get_message_templates(
    *,
    template_name: str,
    engagement_id: int | None,
    user_id: int | None,
    notification_id: int | None,
    operation,
) -> dict[str, Any]:
    """Run template GET and write a dedicated integration_sync_logs row."""
    url = message_templates_request_url(template_name=template_name)
    sync_log_id = await persist_integration_sync_log_isolated(
        provider="whatapi",
        api_url=url,
        engagement_id=engagement_id,
        user_id=user_id,
        request_payload={
            "http_method": "GET",
            "api_name": "message_templates",
            "url": url,
            "query": {"name": (template_name or "").strip()},
            "call_order": 1,
            **({"notification_id": notification_id} if notification_id is not None else {}),
        },
    )
    try:
        body = await operation()
        if not isinstance(body, dict):
            raise TypeError("WhatAPI template response must be a JSON object")
        await finalize_integration_sync_log_isolated(
            sync_log_id=sync_log_id,
            status="success",
            api_endpoint_url=url,
            request_payload={
                "http_method": "GET",
                "api_name": "message_templates",
                "url": url,
                "query": {"name": (template_name or "").strip()},
                "call_order": 1,
                **({"notification_id": notification_id} if notification_id is not None else {}),
            },
            response_payload=compact_template_response(body),
        )
        return body
    except Exception as exc:
        err_payload: dict[str, Any] | None = None
        details = getattr(exc, "details", None)
        if isinstance(details, dict):
            err_payload = details
        await finalize_integration_sync_log_isolated(
            sync_log_id=sync_log_id,
            status="failed",
            api_endpoint_url=url,
            error_message=str(exc)[:2000],
            response_payload=err_payload,
        )
        raise


async def log_whatapi_messages_failure(
    *,
    template_name: str,
    service_key: str,
    phone: str | None,
    engagement_id: int | None,
    user_id: int | None,
    notification_id: int | None,
    error_message: str,
    response_payload: dict[str, Any] | None = None,
    failure_phase: str,
) -> None:
    """Record a failed direct-send POST (build or send) with root cause for admin logs."""
    from modules.notifications.whatapi_client import send_messages_request_url

    url = send_messages_request_url()
    payload = {
        "http_method": "POST",
        "api_name": "messages",
        "url": url,
        "template_name": (template_name or "").strip() or None,
        "service_key": (service_key or "").strip() or None,
        "failure_phase": failure_phase,
        "phone": phone,
        "notification_id": notification_id,
        "call_order": 2,
    }
    sync_log_id = await persist_integration_sync_log_isolated(
        provider="whatapi",
        api_url=url,
        engagement_id=engagement_id,
        user_id=user_id,
        request_payload=payload,
        status="pending",
    )
    await finalize_integration_sync_log_isolated(
        sync_log_id=sync_log_id,
        status="failed",
        api_endpoint_url=url,
        error_message=(error_message or "WhatsApp direct send failed")[:2000],
        request_payload=payload,
        response_payload=response_payload,
    )


async def log_whatapi_inbound_webhook(
    *,
    api_url: str,
    request_payload: dict[str, Any],
    handler_result: dict[str, Any],
    error_message: str | None = None,
) -> None:
    sync_log_id = await persist_integration_sync_log_isolated(
        provider="whatapi",
        api_url=api_url,
        request_payload=request_payload,
        status="pending",
    )
    await finalize_integration_sync_log_isolated(
        sync_log_id=sync_log_id,
        status="failed" if error_message else "success",
        api_endpoint_url=api_url,
        request_payload=request_payload,
        response_payload={"handler_result": handler_result},
        error_message=error_message,
    )
