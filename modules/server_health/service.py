"""Server health monitoring service."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from modules.employee.access_control import ensure_admin
from modules.employee.service import EmployeeContext
from modules.server_health.repository import ServerHealthRepository
from modules.server_health.schemas import (
    HealthCheckRead,
    HealthChecksByCategory,
    HealthRunRead,
    ServerHealthCurrentRead,
)


class ServerHealthService:
    def __init__(self, repository: ServerHealthRepository | None = None) -> None:
        self._repository = repository or ServerHealthRepository()

    async def get_current_status(self, employee: EmployeeContext) -> ServerHealthCurrentRead | None:
        ensure_admin(employee)

        run_row = await self._repository.get_latest_run()
        if run_row is None:
            return None

        run = HealthRunRead.model_validate(run_row)
        check_rows = await self._repository.get_checks_for_run(run.id)
        checks_by_category = _group_checks_by_category(check_rows)

        return ServerHealthCurrentRead(run=run, checks_by_category=checks_by_category)

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
