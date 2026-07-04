"""Outbound webhook forwarding."""

from __future__ import annotations

import logging

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.diagnostics.healthians.sync_log import finalize_healthians_sync_log, log_healthians_call

logger = logging.getLogger(__name__)


class WebhookSenderService:
    """Forward received webhook payloads to configured downstream URLs."""

    async def forward_payload(
        self,
        db: AsyncSession,
        *,
        payload: dict,
        engagement_id: int | None,
        user_id: int | None,
    ) -> list[dict]:
        forward_urls = [
            url.strip()
            for url in settings.HEALTHIANS_WEBHOOK_FORWARD_URL.split(",")
            if url.strip()
        ]
        results: list[dict] = []

        for url in forward_urls:
            sync_log = await log_healthians_call(
                db,
                engagement_id=engagement_id,
                user_id=user_id,
                provider="healthians",
                api_url=url,
                request_payload=payload,
                status="pending",
            )
            try:
                async with httpx.AsyncClient(
                    timeout=settings.HEALTHIANS_WEBHOOK_FORWARD_TIMEOUT_SECONDS
                ) as client:
                    resp = await client.post(url, json=payload)
                    resp.raise_for_status()
                    try:
                        resp_data = resp.json()
                    except Exception:
                        resp_data = {"status_code": resp.status_code, "body": resp.text}

                response_payload = (
                    resp_data if isinstance(resp_data, dict) else {"body": resp_data}
                )
                await finalize_healthians_sync_log(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="success",
                    response_payload=response_payload,
                )
                results.append(
                    {
                        "url": url,
                        "status": "success",
                        "sync_log_id": sync_log.sync_log_id,
                    }
                )
            except Exception as exc:
                logger.error("Healthians webhook forward failed for %s: %s", url, exc)
                await finalize_healthians_sync_log(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="failed",
                    error_message=str(exc),
                )
                results.append(
                    {
                        "url": url,
                        "status": "failed",
                        "sync_log_id": sync_log.sync_log_id,
                        "error": str(exc),
                    }
                )

        return results
