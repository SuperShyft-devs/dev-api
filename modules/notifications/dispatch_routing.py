"""Decide n8n vs direct notification delivery."""

from __future__ import annotations

from core.config import settings
from core.exceptions import AppError
from modules.notifications.models import NotificationService


def is_orchestrator_service(svc: NotificationService) -> bool:
    tv = svc.template_variables
    return isinstance(tv, dict) and tv.get("kind") == "orchestrator"


def _has_direct_send_config(svc: NotificationService) -> bool:
    if svc.channel == "whatsapp":
        return bool((svc.template_name or "").strip())
    if svc.channel == "email":
        return bool(
            [
                (svc.email_subject or "").strip(),
                (svc.email_body or "").strip(),
                (svc.email_html or "").strip(),
            ]
        )
    return False


def should_use_n8n_webhook(svc: NotificationService) -> bool:
    if not settings.NOTIFICATION_ALLOW_N8N_FALLBACK:
        return False
    if _has_direct_send_config(svc):
        return False
    if is_orchestrator_service(svc):
        return True
    return bool((svc.webhook_path or "").strip())


def validate_direct_service_config(svc: NotificationService) -> None:
    if svc.channel == "whatsapp":
        if not (svc.template_name or "").strip():
            raise AppError(
                status_code=400,
                error_code="CONFIG_ERROR",
                message=(
                    f"Service '{svc.service_key}' missing template_name for direct WhatsApp send "
                    "(run migration job or enable NOTIFICATION_ALLOW_N8N_FALLBACK)"
                ),
            )
        return

    if svc.channel == "email":
        if not any(
            [
                (svc.email_subject or "").strip(),
                (svc.email_body or "").strip(),
                (svc.email_html or "").strip(),
            ]
        ):
            raise AppError(
                status_code=400,
                error_code="CONFIG_ERROR",
                message=(
                    f"Service '{svc.service_key}' missing email content for direct SMTP send "
                    "(run migration job or enable NOTIFICATION_ALLOW_N8N_FALLBACK)"
                ),
            )
