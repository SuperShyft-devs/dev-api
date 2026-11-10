"""Server health monitoring service."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.employee.access_control import ensure_admin
from modules.employee.service import EmployeeContext
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.server_health.models import ServerHealthHostState
from modules.server_health.repository import ServerHealthRepository
from modules.server_health.schemas import (
    HealthCheckRead,
    HealthChecksByCategory,
    HealthRunRead,
    ServerHealthCpuAlertRead,
    ServerHealthCurrentRead,
    ServerHealthLatestMetricsRead,
    ServerHealthMetricsIn,
)
from modules.server_health.state_repository import ServerHealthStateRepository

logger = logging.getLogger(__name__)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def _alert_user_ids() -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()
    for part in (settings.CPU_ALERT_USER_IDS or "").split(","):
        raw = part.strip()
        if not raw.isdigit():
            continue
        user_id = int(raw)
        if user_id < 1 or user_id in seen:
            continue
        seen.add(user_id)
        ids.append(user_id)
    return ids


def _metrics_from_state(row: ServerHealthHostState) -> ServerHealthLatestMetricsRead:
    return ServerHealthLatestMetricsRead(
        hostname=row.hostname,
        cpu_usage=float(row.cpu_usage),
        memory_usage=float(row.memory_usage),
        storage_usage=float(row.storage_usage),
        load_1m=float(row.load_1m),
        cores=int(row.cores),
        timestamp=_iso(row.reported_at) or "",
    )


def _cpu_alert_from_state(row: ServerHealthHostState | None) -> ServerHealthCpuAlertRead:
    return ServerHealthCpuAlertRead(
        is_alerting=bool(row.is_alerting) if row is not None else False,
        threshold_pct=float(settings.CPU_ALERT_THRESHOLD_PCT),
        hostname=row.hostname if row is not None else None,
        last_alerted_at=_iso(row.last_alerted_at) if row is not None else None,
        last_recovered_at=_iso(row.last_recovered_at) if row is not None else None,
    )


def _overlay_run_vitals(run: HealthRunRead, metrics: ServerHealthLatestMetricsRead | None) -> HealthRunRead:
    if metrics is None:
        return run
    data = run.model_dump()
    data["cpu_pct"] = metrics.cpu_usage
    data["mem_pct"] = metrics.memory_usage
    data["storage_pct"] = metrics.storage_usage
    return HealthRunRead.model_validate(data)


class ServerHealthService:
    def __init__(
        self,
        repository: ServerHealthRepository | None = None,
        state_repository: ServerHealthStateRepository | None = None,
    ) -> None:
        self._repository = repository or ServerHealthRepository()
        self._state_repository = state_repository or ServerHealthStateRepository()

    async def get_current_status(
        self,
        employee: EmployeeContext,
        db: AsyncSession | None = None,
    ) -> ServerHealthCurrentRead | None:
        ensure_admin(employee)

        run_row = await self._repository.get_latest_run()
        if run_row is None:
            return None

        run = HealthRunRead.model_validate(run_row)
        check_rows = await self._repository.get_checks_for_run(run.id)
        checks_by_category = _group_checks_by_category(check_rows)

        host_state = None
        if db is not None:
            host_state = await self._state_repository.get_latest(db)
        latest_metrics = _metrics_from_state(host_state) if host_state is not None else None
        run = _overlay_run_vitals(run, latest_metrics)

        return ServerHealthCurrentRead(
            run=run,
            checks_by_category=checks_by_category,
            latest_metrics=latest_metrics,
            cpu_alert=_cpu_alert_from_state(host_state),
        )

    async def list_history(
        self,
        employee: EmployeeContext,
        *,
        limit: int,
        run_from: datetime | None = None,
        run_to: datetime | None = None,
    ) -> tuple[list[HealthRunRead], int]:
        ensure_admin(employee)

        rows = await self._repository.list_runs(
            limit=limit,
            run_from=run_from,
            run_to=run_to,
        )
        total = await self._repository.count_runs(run_from=run_from, run_to=run_to)
        return [HealthRunRead.model_validate(row) for row in rows], total

    async def ingest_metrics(
        self,
        db: AsyncSession,
        *,
        payload: ServerHealthMetricsIn,
        notifications_service: NotificationsService,
    ) -> dict[str, Any]:
        reported_at = _as_utc(payload.timestamp)
        hostname = payload.hostname.strip()
        threshold = float(settings.CPU_ALERT_THRESHOLD_PCT)
        above_threshold = payload.cpu_usage >= threshold

        row = await self._state_repository.get_locked_by_hostname(db, hostname=hostname)
        if row is None:
            row = self._state_repository.create(
                hostname=hostname,
                cpu_usage=payload.cpu_usage,
                memory_usage=payload.memory_usage,
                storage_usage=payload.storage_usage,
                load_1m=payload.load_1m,
                cores=payload.cores,
                reported_at=reported_at,
            )
            db.add(row)
            await db.flush()
        else:
            row.cpu_usage = payload.cpu_usage
            row.memory_usage = payload.memory_usage
            row.storage_usage = payload.storage_usage
            row.load_1m = payload.load_1m
            row.cores = payload.cores
            row.reported_at = reported_at

        alert_action = "none"
        if above_threshold and not row.is_alerting:
            alert_action = await self._dispatch_cpu_alert(
                db,
                row=row,
                payload=payload,
                notifications_service=notifications_service,
            )
        elif not above_threshold and row.is_alerting:
            row.is_alerting = False
            row.last_recovered_at = datetime.now(timezone.utc)
            alert_action = "recovered"
        elif above_threshold:
            alert_action = "already_alerting"

        await db.flush()
        return {
            "accepted": True,
            "hostname": hostname,
            "cpu_usage": payload.cpu_usage,
            "threshold_pct": threshold,
            "above_threshold": above_threshold,
            "is_alerting": bool(row.is_alerting),
            "alert_action": alert_action,
        }

    async def _dispatch_cpu_alert(
        self,
        db: AsyncSession,
        *,
        row: ServerHealthHostState,
        payload: ServerHealthMetricsIn,
        notifications_service: NotificationsService,
    ) -> str:
        user_ids = _alert_user_ids()
        if not user_ids:
            logger.warning(
                "CPU alert skipped: no recipient user ids configured (hostname=%s cpu=%.1f)",
                payload.hostname,
                payload.cpu_usage,
            )
            return "skipped_no_recipients"

        try:
            from modules.notifications.engine import get_notification_engine

            ids = await get_notification_engine().emit(
                db,
                event_code="system.cpu_threshold",
                context={
                    "admin_user_ids": user_ids,
                    "participant_details": {
                        "hostname": payload.hostname,
                        "cpu_usage": f"{payload.cpu_usage:.0f}",
                        "memory_usage": f"{payload.memory_usage:.0f}",
                        "storage_usage": f"{payload.storage_usage:.0f}",
                        "load_1m": f"{payload.load_1m}",
                        "cores": str(payload.cores),
                    },
                    "fallback_service_keys": [settings.CPU_ALERT_SERVICE_KEY],
                },
            )
            result = {"status": "dispatched" if ids else "skipped", "notification_id": ids[0] if ids else None}
        except Exception as exc:
            logger.warning(
                "CPU alert dispatch failed (hostname=%s cpu=%.1f): %s",
                payload.hostname,
                payload.cpu_usage,
                exc,
            )
            return "alert_failed"

        status = str(result.get("status") or "")
        if status == "failed":
            logger.warning(
                "CPU alert webhook failed (hostname=%s cpu=%.1f notification_id=%s)",
                payload.hostname,
                payload.cpu_usage,
                result.get("notification_id"),
            )
            return "alert_failed"

        row.is_alerting = True
        row.last_alerted_at = datetime.now(timezone.utc)
        notification_id = result.get("notification_id")
        if isinstance(notification_id, int):
            row.last_alert_notification_id = notification_id
        return "alerted"


def _group_checks_by_category(check_rows: list[dict[str, Any]]) -> list[HealthChecksByCategory]:
    grouped: dict[str, list[HealthCheckRead]] = {}
    category_order: list[str] = []

    for row in check_rows:
        check = HealthCheckRead.model_validate(row)
        category = check.category or "UNCATEGORIZED"
        if category not in grouped:
            grouped[category] = []
            category_order.append(category)
        grouped[category].append(check)

    return [
        HealthChecksByCategory(category=category, checks=grouped[category])
        for category in category_order
    ]
