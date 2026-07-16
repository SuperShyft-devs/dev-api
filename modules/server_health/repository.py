"""Read-only access to the server health-check SQLite database."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

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

    def _run_query_sync(
        self,
        sql: str,
        params: tuple[Any, ...] | list[Any] = (),
    ) -> list[dict[str, Any]]:
        path = self._ensure_db_available()
        try:
            # Open by path (not URI mode=ro) so WAL DBs remain readable.
            # query_only blocks accidental writes from this connection.
            with sqlite3.connect(path.as_posix()) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA query_only = ON")
                cursor = conn.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]
        except AppError:
            raise
        except Exception as exc:
            raise AppError(
                status_code=503,
                error_code="SERVICE_UNAVAILABLE",
                message=f"Server health database query failed: {exc}",
            ) from exc

    async def _run_query(
        self,
        sql: str,
        params: tuple[Any, ...] | list[Any] = (),
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._run_query_sync, sql, params)

    async def get_latest_run(self) -> dict[str, Any] | None:
        rows = await self._run_query(
            """
            SELECT id, run_at, ok_count, warn_count, crit_count, overall_status
            FROM health_runs
            ORDER BY run_at DESC, id DESC
            LIMIT 1
            """
        )
        if not rows:
            return None
        return rows[0]

    async def get_checks_for_run(self, run_id: int) -> list[dict[str, Any]]:
        return await self._run_query(
            """
            SELECT id, run_id, category, status, message
            FROM health_checks
            WHERE run_id = ?
            ORDER BY category ASC, id ASC
            """,
            (run_id,),
        )

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

        return await self._run_query(
            f"""
            SELECT id, run_at, ok_count, warn_count, crit_count, overall_status
            FROM health_runs
            WHERE {where_sql}
            ORDER BY run_at DESC, id DESC
            LIMIT ?
            """,
            params,
        )

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
        rows = await self._run_query(
            f"SELECT COUNT(*) AS cnt FROM health_runs WHERE {where_sql}",
            params,
        )
        if not rows:
            return 0
        return int(rows[0]["cnt"])
