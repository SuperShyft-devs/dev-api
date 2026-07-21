"""Read-only access to the server health-check SQLite database."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from core.config import settings
from core.exceptions import AppError

_RUN_BASE_COLS = "id, run_at, ok_count, warn_count, crit_count, overall_status"


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

    def _run_select_sql(self, conn: sqlite3.Connection) -> str:
        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(health_runs)").fetchall()
        }
        select_cols = _RUN_BASE_COLS
        if "cpu_pct" in cols:
            select_cols += ", cpu_pct"
        if "mem_pct" in cols:
            select_cols += ", mem_pct"
        if "storage_pct" in cols:
            select_cols += ", storage_pct"
        return select_cols

    def _run_query_sync(
        self,
        sql: str,
        params: tuple[Any, ...] | list[Any] = (),
    ) -> list[dict[str, Any]]:
        path = self._ensure_db_available()
        try:
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

    def _select_runs_sync(
        self,
        *,
        where_sql: str = "1 = 1",
        params: list[Any] | None = None,
        limit: int | None = None,
        order: str = "ORDER BY run_at DESC, id DESC",
    ) -> list[dict[str, Any]]:
        path = self._ensure_db_available()
        params = list(params or [])
        try:
            with sqlite3.connect(path.as_posix()) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA query_only = ON")
                cols = self._run_select_sql(conn)
                sql = f"SELECT {cols} FROM health_runs WHERE {where_sql} {order}"
                if limit is not None:
                    sql += " LIMIT ?"
                    params.append(limit)
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
        rows = await asyncio.to_thread(self._select_runs_sync, limit=1)
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
        return await asyncio.to_thread(
            self._select_runs_sync,
            where_sql=where_sql,
            params=params,
            limit=limit,
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
