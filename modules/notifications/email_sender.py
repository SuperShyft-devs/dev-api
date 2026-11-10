"""SMTP email sender for direct notification delivery."""

from __future__ import annotations

import asyncio
import smtplib
from email.message import EmailMessage

from core.config import settings
from core.exceptions import AppError


def _validate_smtp_config() -> tuple[str, int, str, str]:
    host = (settings.SMTP_HOST or "").strip()
    username = (settings.SMTP_USERNAME or "").strip()
    password = (settings.SMTP_PASSWORD or "").strip()
    from_email = (settings.SMTP_FROM_EMAIL or "").strip()
    if not host or not from_email:
        raise AppError(
            status_code=500,
            error_code="CONFIG_ERROR",
            message="SMTP_HOST and SMTP_FROM_EMAIL must be configured",
        )
    if settings.SMTP_USE_TLS and (not username or not password):
        raise AppError(
            status_code=500,
            error_code="CONFIG_ERROR",
            message="SMTP_USERNAME and SMTP_PASSWORD are required when SMTP_USE_TLS is enabled",
        )
    return host, settings.SMTP_PORT, username, password


def _send_smtp_sync(
    *,
    to: str,
    subject: str,
    body_text: str,
    body_html: str | None,
) -> None:
    host, port, username, password = _validate_smtp_config()
    from_email = settings.SMTP_FROM_EMAIL.strip()
    from_name = (settings.SMTP_FROM_NAME or "SuperShyft").strip()

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{from_email}>" if from_name else from_email
    msg["To"] = to
    msg.set_content(body_text)
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    timeout = settings.SMTP_TIMEOUT_SECONDS
    try:
        with smtplib.SMTP(host, port, timeout=timeout) as server:
            if settings.SMTP_USE_TLS:
                server.starttls()
            if username and password:
                server.login(username, password)
            server.send_message(msg)
    except smtplib.SMTPException as exc:
        raise AppError(
            status_code=502,
            error_code="SMTP_ERROR",
            message=f"SMTP delivery failed: {exc}",
        ) from exc
    except OSError as exc:
        raise AppError(
            status_code=502,
            error_code="SMTP_ERROR",
            message=f"SMTP connection failed: {exc}",
        ) from exc


async def send_email(
    *,
    to: str,
    subject: str,
    body_text: str,
    body_html: str | None = None,
) -> dict[str, str]:
    """Send an email via configured SMTP. Returns a minimal success payload."""
    await asyncio.to_thread(
        _send_smtp_sync,
        to=to,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
    )
    return {"status": "sent"}
