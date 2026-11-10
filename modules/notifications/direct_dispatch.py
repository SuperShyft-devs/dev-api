"""In-process WhatsApp and email notification delivery."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from modules.audit.cron_sync_logging import (
    finalize_integration_sync_log_isolated,
    persist_integration_sync_log_isolated,
)
from modules.notifications.context_builder import build_member_context, normalize_whatsapp_phone
from modules.notifications.email_sender import send_email
from modules.notifications.models import NotificationService
from modules.notifications.template_variables import (
    build_legacy_message_csv,
    interpolate_email_template,
    resolve_header_link,
    resolve_slots,
)
from modules.notifications.whatsapp_fallback_get import (
    build_whatsapp_fallback_get_url,
    send_whatsapp_fallback_get,
)
from modules.notifications.whatapi_sync_logging import log_whatapi_get_message_templates
from modules.notifications.whatsapp_template_builder import build_whatsapp_send_payload
from core.exceptions import AppError
from modules.notifications.whatapi_client import (
    WhatApiClient,
    get_whatapi_client,
    message_templates_request_url,
    send_messages_request_url,
    whatapi_auth_descriptor,
)

logger = logging.getLogger(__name__)

_OTP_WHATSAPP_SERVICE_KEYS = frozenset({"whatapi-otp", "send-otp-whatsapp"})


def _whatsapp_fallback_allowed(svc: NotificationService) -> bool:
    """Legacy GET fallback cannot deliver Meta OTP templates — POST only."""
    if svc.require_otp:
        return False
    return (svc.service_key or "").strip() not in _OTP_WHATSAPP_SERVICE_KEYS


def _integration_failure_from_exc(exc: Exception) -> tuple[str, dict[str, Any] | None]:
    if isinstance(exc, AppError) and isinstance(exc.details, dict):
        return str(exc), dict(exc.details)
    return str(exc), None


@dataclass
class DeliverResult:
    status: str
    message: str
    queue_id: str | None = None
    completed_at: datetime | None = None


class DirectNotificationDispatch:
    def __init__(self, client: WhatApiClient | None = None) -> None:
        self._client = client or get_whatapi_client()

    async def deliver_batch(
        self,
        *,
        svc: NotificationService,
        notification_id: int,
        members: list[dict[str, Any]],
        participant_details: dict[str, Any] | None = None,
        engagement_id: int | None = None,
        sync_log_user_id: int | None = None,
    ) -> DeliverResult:
        if svc.channel == "email":
            return await self._deliver_email_batch(
                svc=svc,
                notification_id=notification_id,
                members=members,
                participant_details=participant_details,
                engagement_id=engagement_id,
                sync_log_user_id=sync_log_user_id,
            )
        return await self._deliver_whatsapp_batch(
            svc=svc,
            notification_id=notification_id,
            members=members,
            participant_details=participant_details,
            engagement_id=engagement_id,
            sync_log_user_id=sync_log_user_id,
        )

    async def _deliver_email_batch(
        self,
        *,
        svc: NotificationService,
        notification_id: int,
        members: list[dict[str, Any]],
        participant_details: dict[str, Any] | None,
        engagement_id: int | None,
        sync_log_user_id: int | None,
    ) -> DeliverResult:
        failures: list[str] = []
        for member in members:
            context = build_member_context(
                member, participant_details=participant_details
            )
            # Staff contact is the SMTP recipient for ops/staff dispatches; user.* is often the participant.
            to_email = str(
                (context.get("staff") or {}).get("email")
                or (context.get("user") or {}).get("email")
                or ""
            ).strip()
            if not to_email:
                failures.append("missing recipient email")
                continue

            subject = interpolate_email_template(svc.email_subject or "Notification", context)
            body = interpolate_email_template(
                svc.email_body or svc.email_html or "",
                context,
            )
            html = None
            if (svc.email_html or "").strip():
                html = interpolate_email_template(svc.email_html or "", context)
                if html == "n8n_dynamic_html_template":
                    html = body

            sync_log_id = await persist_integration_sync_log_isolated(
                provider="smtp",
                api_url=f"smtp://{to_email}",
                engagement_id=engagement_id,
                user_id=sync_log_user_id,
                request_payload={
                    "notification_id": notification_id,
                    "to": to_email,
                    "subject": subject,
                },
            )
            try:
                result = await send_email(
                    to=to_email,
                    subject=subject,
                    body_text=body,
                    body_html=html,
                )
                await finalize_integration_sync_log_isolated(
                    sync_log_id=sync_log_id,
                    status="success",
                    response_payload=result,
                )
            except Exception as exc:
                logger.error("SMTP send failed: %s", exc)
                failures.append(str(exc))
                await finalize_integration_sync_log_isolated(
                    sync_log_id=sync_log_id,
                    status="failed",
                    error_message=str(exc),
                )

        if failures:
            return DeliverResult(
                status="failed",
                message=f"SMTP delivery failed: {failures[0]}",
            )
        now = datetime.now(timezone.utc)
        return DeliverResult(
            status="sent",
            message="Email sent via SMTP",
            queue_id=None,
            completed_at=now,
        )

    async def _deliver_whatsapp_batch(
        self,
        *,
        svc: NotificationService,
        notification_id: int,
        members: list[dict[str, Any]],
        participant_details: dict[str, Any] | None,
        engagement_id: int | None,
        sync_log_user_id: int | None,
    ) -> DeliverResult:
        failures: list[str] = []
        last_queue_id: str | None = None
        used_post = False
        used_fallback = False

        for member in members:
            context = build_member_context(member, participant_details=participant_details)
            phone = str(context["user"].get("phone") or "").strip()
            if not phone:
                failures.append("missing recipient phone")
                continue

            slot_values = resolve_slots(svc.template_variables, context)
            legacy_csv = build_legacy_message_csv(svc.template_name or "", slot_values)
            header_link = resolve_header_link(svc.template_variables, context)

            template_label = (svc.template_name or "").strip()
            fetch_url = message_templates_request_url(template_name=template_label)
            send_url = send_messages_request_url()
            sync_log_id: int | None = None

            sent_via_post = False
            try:
                template_body = await log_whatapi_get_message_templates(
                    template_name=template_label,
                    engagement_id=engagement_id,
                    user_id=sync_log_user_id,
                    notification_id=notification_id,
                    operation=lambda name=template_label: self._client.fetch_message_template(
                        template_name=name
                    ),
                )
                template_name, components, language_code = await build_whatsapp_send_payload(
                    self._client,
                    svc,
                    member=member,
                    participant_details=participant_details,
                    template_body=template_body,
                )
                sync_log_id = await persist_integration_sync_log_isolated(
                    provider="whatapi",
                    api_url=send_url,
                    engagement_id=engagement_id,
                    user_id=sync_log_user_id,
                    request_payload={
                        "http_method": "POST",
                        "api_name": "messages",
                        "url": send_url,
                        "notification_id": notification_id,
                        "phone": phone,
                        "template_name": template_name,
                        "call_order": 2,
                    },
                )
                send_result = await self._client.send_template_message(
                    to=normalize_whatsapp_phone(phone),
                    template_name=template_name,
                    language_code=language_code,
                    components=components,
                )
                last_queue_id = send_result.get("queue_id")
                used_post = True
                sent_via_post = True
                send_body = {
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": normalize_whatsapp_phone(phone),
                    "type": "template",
                    "template": {
                        "name": template_name,
                        "language": {"policy": "deterministic", "code": language_code},
                        "components": components,
                    },
                }
                await finalize_integration_sync_log_isolated(
                    sync_log_id=sync_log_id,
                    status="success",
                    api_endpoint_url=send_url,
                    request_payload={
                        "http_method": "POST",
                        "api_name": "messages",
                        "url": send_url,
                        "body": send_body,
                        "notification_id": notification_id,
                        "call_order": 2,
                    },
                    response_payload={
                        "queue_id": send_result.get("queue_id"),
                        "message_status": send_result.get("message_status"),
                    },
                )
            except Exception as exc:
                err_msg, err_payload = _integration_failure_from_exc(exc)
                if not _whatsapp_fallback_allowed(svc):
                    logger.warning(
                        "WhatAPI POST failed for OTP service %s (no GET fallback): %s",
                        svc.service_key,
                        exc,
                    )
                    failures.append(err_msg)
                    if sync_log_id is not None:
                        await finalize_integration_sync_log_isolated(
                            sync_log_id=sync_log_id,
                            status="failed",
                            error_message=err_msg,
                            response_payload=err_payload,
                        )
                    continue
                logger.warning("WhatAPI POST failed, trying GET fallback: %s", exc)
                failed_url = (
                    str(err_payload.get("url") or "")
                    if isinstance(err_payload, dict)
                    else ""
                )
                failed_is_get = failed_url.endswith("message_templates") or "message_templates" in failed_url
                if sync_log_id is not None:
                    await finalize_integration_sync_log_isolated(
                        sync_log_id=sync_log_id,
                        status="failed",
                        api_endpoint_url=failed_url or (fetch_url if failed_is_get else send_url),
                        error_message=err_msg,
                        request_payload={
                            "http_method": "GET" if failed_is_get else "POST",
                            "api_name": "message_templates" if failed_is_get else "messages",
                            "url": failed_url or (fetch_url if failed_is_get else send_url),
                            "template_name": template_label,
                            "phone": phone,
                            "notification_id": notification_id,
                            "call_order": 2 if not failed_is_get else 1,
                        },
                        response_payload=err_payload,
                    )

                fb_phone = normalize_whatsapp_phone(phone)
                fb_media = (
                    header_link if header_link and "blood" in (header_link or "").lower() else None
                )
                fb_pdf = (
                    header_link if header_link and "blood" not in (header_link or "").lower() else None
                )
                fb_url = build_whatsapp_fallback_get_url(
                    phone=fb_phone,
                    message_csv=legacy_csv,
                    medialink=fb_media,
                    pdflink=fb_pdf,
                )
                fb_query = {
                    "number": fb_phone,
                    "message": legacy_csv,
                    **({"medialink": fb_media} if fb_media else {}),
                    **({"pdflink": fb_pdf} if fb_pdf else {}),
                }
                fb_log_id = await persist_integration_sync_log_isolated(
                    provider="whatapi_fallback",
                    api_url=fb_url,
                    engagement_id=engagement_id,
                    user_id=sync_log_user_id,
                    request_payload={
                        "http_method": "GET",
                        "api_name": "whatapi_webhook",
                        "url": fb_url,
                        "body": fb_query,
                    },
                )
                try:
                    fb_result = await send_whatsapp_fallback_get(
                        phone=fb_phone,
                        message_csv=legacy_csv,
                        medialink=fb_media,
                        pdflink=fb_pdf,
                    )
                    used_fallback = True
                    if not fb_result.get("accepted"):
                        failures.append("WhatsApp GET fallback not accepted")
                    await finalize_integration_sync_log_isolated(
                        sync_log_id=fb_log_id,
                        status="success" if fb_result.get("accepted") else "failed",
                        response_payload=fb_result.get("response") if isinstance(fb_result.get("response"), dict) else {
                            "accepted": fb_result.get("accepted"),
                        },
                    )
                except Exception as fb_exc:
                    err_msg, err_payload = _integration_failure_from_exc(fb_exc)
                    failures.append(err_msg)
                    await finalize_integration_sync_log_isolated(
                        sync_log_id=fb_log_id,
                        status="failed",
                        error_message=err_msg,
                        response_payload=err_payload,
                    )

        if failures:
            return DeliverResult(
                status="failed",
                message=f"WhatsApp delivery failed: {failures[0]}",
                queue_id=last_queue_id,
            )

        if used_post and not used_fallback:
            return DeliverResult(
                status="pending",
                message="WhatsApp template queued via WhatAPI POST",
                queue_id=last_queue_id,
            )

        now = datetime.now(timezone.utc)
        return DeliverResult(
            status="sent",
            message="WhatsApp sent via WhatAPI GET fallback",
            queue_id=None,
            completed_at=now,
        )


def get_direct_dispatch() -> DirectNotificationDispatch:
    return DirectNotificationDispatch()
