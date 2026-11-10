"""Process WhatAPI/Meta delivery status webhook payloads."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.repository import NotificationsRepository

logger = logging.getLogger(__name__)

_TERMINAL_SENT = frozenset({"sent", "delivered", "read", "played"})
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


def _error_detail_from_node(node: dict[str, Any]) -> str | None:
    errors = node.get("errors")
    if isinstance(errors, list) and errors:
        parts: list[str] = []
        for item in errors:
            if not isinstance(item, dict):
                continue
            title = item.get("title") or item.get("message") or item.get("error_data", {}).get("details")
            if title:
                parts.append(str(title).strip())
        if parts:
            return "; ".join(parts)
    for key in ("error_message", "reason", "detail", "title"):
        val = node.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _walk_for_queue_status(obj: Any) -> list[tuple[str, str, str | None]]:
    """Extract (queue_id, status, detail) tuples from nested webhook JSON."""
    found: list[tuple[str, str, str | None]] = []

    def record(queue_id: str, status_raw: Any, detail: str | None, source: dict[str, Any]) -> None:
        status = _normalize_status(status_raw if isinstance(status_raw, str) else None)
        if not status:
            return
        merged_detail = detail or _error_detail_from_node(source)
        found.append((queue_id.strip(), status, merged_detail))

    def visit(node: Any, meta_queue_id: str | None = None) -> None:
        if isinstance(node, dict):
            queue_id = node.get("queue_id") or node.get("queueId")
            if isinstance(queue_id, str) and queue_id.strip():
                meta_queue_id = queue_id.strip()
            status_raw = (
                node.get("message_status")
                or node.get("messageStatus")
                or node.get("status")
            )
            if isinstance(meta_queue_id, str) and meta_queue_id:
                if isinstance(status_raw, str) and _normalize_status(status_raw):
                    record(meta_queue_id, status_raw, _error_detail_from_node(node), node)

            statuses = node.get("statuses")
            if isinstance(statuses, list) and isinstance(meta_queue_id, str):
                for st in statuses:
                    if not isinstance(st, dict):
                        continue
                    record(
                        meta_queue_id,
                        st.get("status"),
                        _error_detail_from_node(st),
                        st,
                    )

            for value in node.values():
                visit(value, meta_queue_id)
        elif isinstance(node, list):
            for item in node:
                visit(item, meta_queue_id)

    visit(obj)
    return found


def _delivery_message(whatapi_status: str, detail: str | None) -> str | None:
    status = _normalize_status(whatapi_status)
    if status is None:
        return None
    if status in _TERMINAL_SENT:
        return f"WhatsApp delivered ({status})"
    if status in _TERMINAL_FAILED:
        base = f"WhatsApp delivery failed ({status})"
        if detail:
            return f"{base}: {detail}"
        return base
    return None


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
        for queue_id, whatapi_status, detail in pairs:
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
            msg = _delivery_message(whatapi_status, detail)
            if msg and db_status in ("sent", "failed"):
                values["message"] = msg
            if db_status in ("sent", "failed"):
                values["completed_at"] = now

            await self._repo.update_notification(
                db,
                notification_id=notification.notification_id,
                values=values,
            )
            updated += 1

        return {"updated": updated}
