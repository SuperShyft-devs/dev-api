"""WhatAPI webhook verification and delivery status routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.config import settings
from core.exceptions import AppError
from db.session import get_db
from modules.notifications.whatapi_webhook_handler import WhatApiWebhookHandler

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _first_challenge(
  hub_challenge: str | None,
  challenge: str | None,
  challange: str | None,
) -> str | None:
    for value in (hub_challenge, challenge, challange):
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _verify_token_ok(
    hub_verify_token: str | None,
    verify_token: str | None,
) -> bool:
    expected = (settings.WHATAPI_WEBHOOK_VERIFY_TOKEN or "").strip()
    if not expected:
        return True
    supplied = (hub_verify_token or verify_token or "").strip()
    return supplied == expected


@router.get("/whatapi")
async def whatapi_webhook_verify(
    response: Response,
    hub_mode: str | None = Query(None, alias="hub.mode"),
    hub_challenge: str | None = Query(None, alias="hub.challenge"),
    hub_verify_token: str | None = Query(None, alias="hub.verify_token"),
    challenge: str | None = Query(None),
    challange: str | None = Query(None),
    verify_token: str | None = Query(None),
    echo: str | None = Query(None),
):
    challenge_value = _first_challenge(hub_challenge, challenge, challange)
    if challenge_value is None:
        raise AppError(
            status_code=400,
            error_code="BAD_REQUEST",
            message="Missing webhook challenge parameter",
        )
    if not _verify_token_ok(hub_verify_token, verify_token):
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="Invalid verify token",
        )
    if hub_mode and hub_mode.strip().lower() not in {"subscribe", ""}:
        raise AppError(
            status_code=400,
            error_code="BAD_REQUEST",
            message="Unsupported hub.mode",
        )
    if echo is not None:
        pass
    return Response(content=challenge_value, media_type="text/plain")


@router.post("/whatapi")
async def whatapi_webhook_status(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    try:
        body: Any = await request.json()
    except Exception:
        body = {}

    handler = WhatApiWebhookHandler()
    if isinstance(body, dict):
        result = await handler.handle_status_payload(db, payload=body)
    elif isinstance(body, list):
        result = await handler.handle_status_payload(db, payload=body)
    else:
        result = {"updated": 0, "ignored": True}

    await db.commit()
    return success_response(result)
