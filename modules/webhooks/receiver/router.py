"""Inbound webhook HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.webhooks.dependencies import get_webhooks_receiver_service
from modules.webhooks.receiver.schemas import HealthiansWebhookPayload
from modules.webhooks.receiver.service import WebhooksReceiverService

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post("/healthians")
async def healthians_webhook(
    payload: HealthiansWebhookPayload,
    request: Request,
    db: AsyncSession = Depends(get_db),
    service: WebhooksReceiverService = Depends(get_webhooks_receiver_service),
):
    result = await service.handle_healthians_webhook(
        db,
        payload=payload,
        api_endpoint_url=str(request.url.path),
    )
    await db.commit()
    return success_response(result)
