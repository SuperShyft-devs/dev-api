"""Process WhatAPI/Meta delivery status webhook payloads."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.repository import NotificationsRepository

logger = logging.getLogger(__name__)

_TERMINAL_SENT = frozenset({"sent", "delivered", "read"})
_TERMINAL_FAILED = frozenset({"failed", "undelivered", "rejected"})


def _normalize_status(raw: str | None) -> str | None:
    if raw is None:
        return None
    normalized = str(raw).strip().lower()
    return normalized or None


def _db_status_for_whatapi(status: str) -> str | None:
    if status in ("queued", "pending", "accepted"):
        return "pending"
    if status in _TERMINAL_SENT:
        return "sent"
    if status in _TERMINAL_FAILED:
        return "failed"
    return None


def _walk_for_queue_status(obj: Any) -> list[tuple[str, str]]:
    """Extract (queue_id, status) pairs from nested webhook JSON."""
    found: list[tuple[str, str]] = []

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            queue_id = node.get("queue_id") or node.get("queueId")
            status_raw = (
                node.get("message_status")
                or node.get("messageStatus")
                or node.get("status")
            )
            if isinstance(queue_id, str) and queue_id.strip():
                status = _normalize_status(
                    status_raw if isinstance(status_raw, str) else None
                )
                if status:
                    found.append((queue_id.strip(), status))
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(obj)
    return found


class WhatApiWebhookHandler:
    def __init__(self, repo: NotificationsRepository | None = None) -> None:
        self._repo = repo or NotificationsRepository()

    async def handle_status_payload(
        self, db: AsyncSession, *, payload: dict[str, Any] | list[Any]
    ) -> dict[str, Any]:
        pairs = _walk_for_queue_status(payload)
        if not pairs:
            logger.info("WhatAPI webhook: no queue_id/status pairs in payload")
            return {"updated": 0, "ignored": True}

        updated = 0
        now = datetime.now(timezone.utc)
        seen: set[str] = set()
        for queue_id, whatapi_status in pairs:
            if queue_id in seen:
                continue
            seen.add(queue_id)
            db_status = _db_status_for_whatapi(whatapi_status)
            if db_status is None:
                logger.info(
                    "WhatAPI webhook: ignoring status %s for queue_id %s",
                    whatapi_status,
                    queue_id,
                )
                continue

            notification = await self._repo.get_notification_by_queue_id(
                db, queue_id=queue_id
            )
            if notification is None:
                logger.info("WhatAPI webhook: no notification for queue_id %s", queue_id)
                continue

            values: dict[str, Any] = {"status": db_status}
            if db_status in ("sent", "failed"):
                values["completed_at"] = now

            await self._repo.update_notification(
                db,
                notification_id=notification.notification_id,
                values=values,
            )
            updated += 1

        return {"updated": updated}
