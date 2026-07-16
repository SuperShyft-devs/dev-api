"""Read-only access to the server health-check SQLite database."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from core.config import settings
from core.exceptions import AppError


class ServerHealthRepository:
    """Queries health_runs and health_checks from the external SQLite file."""

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path or settings.HEALTH_CHECK_DB_PATH

    def _ensure_db_available(self) -> Path:
        path = Path(self._db_path)
        if not path.is_file():
            raise AppError(
                status_code=503,
                error_code="SERVICE_UNAVAILABLE",
                message=(
                    "Server health database is not available. "
                    f"Expected file at {self._db_path}."
                ),
            )
        return path

    async def _connect(self) -> aiosqlite.Connection:
        path = self._ensure_db_available()
        uri = f"file:{path.as_posix()}?mode=ro"
        return await aiosqlite.connect(uri, uri=True)

    async def get_latest_run(self) -> dict[str, Any] | None:
        async with await self._connect() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT id, run_at, ok_count, warn_count, crit_count, overall_status
                FROM health_runs
                ORDER BY run_at DESC, id DESC
                LIMIT 1
                """
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return dict(row)

    async def get_checks_for_run(self, run_id: int) -> list[dict[str, Any]]:
        async with await self._connect() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT id, run_id, category, status, message
                FROM health_checks
                WHERE run_id = ?
                ORDER BY category ASC, id ASC
                """,
                (run_id,),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def list_runs(
        self,
        *,
        limit: int,
        run_from: datetime | None = None,
        run_to: datetime | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["1 = 1"]
        params: list[Any] = []

        if run_from is not None:
            clauses.append("run_at >= ?")
            params.append(run_from.strftime("%Y-%m-%d %H:%M:%S"))

        if run_to is not None:
            clauses.append("run_at <= ?")
            params.append(run_to.strftime("%Y-%m-%d %H:%M:%S"))

        where_sql = " AND ".join(clauses)
        params.append(limit)

        async with await self._connect() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                f"""
                SELECT id, run_at, ok_count, warn_count, crit_count, overall_status
                FROM health_runs
                WHERE {where_sql}
                ORDER BY run_at DESC, id DESC
                LIMIT ?
                """,
                params,
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def count_runs(
        self,
        *,
        run_from: datetime | None = None,
        run_to: datetime | None = None,
    ) -> int:
        clauses = ["1 = 1"]
        params: list[Any] = []

        if run_from is not None:
            clauses.append("run_at >= ?")
            params.append(run_from.strftime("%Y-%m-%d %H:%M:%S"))

        if run_to is not None:
            clauses.append("run_at <= ?")
            params.append(run_to.strftime("%Y-%m-%d %H:%M:%S"))

        where_sql = " AND ".join(clauses)

        async with await self._connect() as db:
            cursor = await db.execute(
                f"SELECT COUNT(*) FROM health_runs WHERE {where_sql}",
                params,
            )
            row = await cursor.fetchone()
            return int(row[0]) if row else 0
