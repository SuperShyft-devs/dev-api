"""Helpers for per-service engagement notification configuration."""

from __future__ import annotations

from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.notifications.events_catalog import get_event, map_legacy_event_code
from modules.notifications.models import NotificationService


class NotificationServiceConfigItem(BaseModel):
    service_key: str = Field(min_length=1)
    external_link: str | None = None

    @field_validator("external_link")
    @classmethod
    def validate_external_link(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        parsed = urlparse(stripped)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("external_link must be a valid http(s) URL")
        return stripped


def normalize_service_config(raw: object) -> NotificationServiceConfigItem | None:
    if isinstance(raw, str):
        key = raw.strip()
        if not key:
            return None
        return NotificationServiceConfigItem(service_key=key, external_link=None)
    if isinstance(raw, dict):
        key = str(raw.get("service_key", "")).strip()
        if not key:
            return None
        link = raw.get("external_link")
        return NotificationServiceConfigItem(
            service_key=key,
            external_link=str(link).strip() if link else None,
        )
    return None


def normalize_notification_services(raw: list | None) -> list[NotificationServiceConfigItem]:
    if not raw:
        return []
    result: list[NotificationServiceConfigItem] = []
    seen: set[str] = set()
    for item in raw:
        cfg = normalize_service_config(item)
        if cfg is None or cfg.service_key in seen:
            continue
        seen.add(cfg.service_key)
        result.append(cfg)
    return result


def merge_notification_service_configs(
    *groups: list[NotificationServiceConfigItem] | tuple[NotificationServiceConfigItem, ...] | None,
) -> list[NotificationServiceConfigItem]:
    """Union service configs from multiple engagement notification slots (dedupe by service_key)."""
    seen: set[str] = set()
    merged: list[NotificationServiceConfigItem] = []
    for group in groups:
        if not group:
            continue
        for cfg in group:
            key = (cfg.service_key or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(cfg)
    return merged


def extract_service_keys(raw: list | None) -> list[str]:
    return [cfg.service_key for cfg in normalize_notification_services(raw)]


def get_external_link_for_service(raw: list | None, service_key: str) -> str | None:
    for cfg in normalize_notification_services(raw):
        if cfg.service_key == service_key:
            return cfg.external_link
    return None


def serialize_for_db(configs: list[NotificationServiceConfigItem]) -> list[dict]:
    return [cfg.model_dump(mode="json") for cfg in configs]


def configs_to_api(raw: list | None) -> list[dict]:
    return serialize_for_db(normalize_notification_services(raw))


async def validate_notification_service_configs(
    db: AsyncSession,
    *,
    notification_services: list | None,
    event_label: str | None = None,
) -> list[NotificationServiceConfigItem]:
    configs = normalize_notification_services(notification_services)
    if not configs:
        return []

    keys = [cfg.service_key for cfg in configs]
    result = await db.execute(
        select(NotificationService).where(NotificationService.service_key.in_(keys))
    )
    services_by_key = {svc.service_key: svc for svc in result.scalars().all()}

    prefix = f"{event_label}: " if event_label else ""
    for cfg in configs:
        svc = services_by_key.get(cfg.service_key)
        if svc is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"{prefix}Unknown notification service '{cfg.service_key}'",
            )
        if svc.require_external_link and not cfg.external_link:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=(
                    f"{prefix}Notification service '{cfg.service_key}' requires external_link"
                ),
            )
    return configs


def _normalize_event_code(item: dict) -> str | None:
    if item.get("event_code"):
        code = str(item["event_code"]).strip()
        return map_legacy_event_code(code) if code else None
    legacy_id = item.get("notification_event_id")
    if legacy_id is not None:
        return None
    return None


async def prepare_notification_items_for_storage(
    db: AsyncSession,
    notifications: list[dict],
) -> list[dict]:
    prepared: list[dict] = []
    for item in notifications:
        event_code = _normalize_event_code(item)
        if not event_code:
            if item.get("notification_event_id") is not None:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="notification_event_id is deprecated; use event_code",
                )
            continue
        if get_event(event_code) is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Unknown event_code '{event_code}'",
            )
        event_label = event_code
        configs = await validate_notification_service_configs(
            db,
            notification_services=item.get("notification_services"),
            event_label=event_label,
        )
        if not configs:
            continue
        prepared.append({
            "event_code": event_code,
            "notification_services": serialize_for_db(configs),
        })
    return prepared
