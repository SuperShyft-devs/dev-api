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
        """Open the health DB for reads.

        Uses a normal path open (not URI ``mode=ro``) so WAL-mode databases
        still work when the process can read the file and adjacent -wal/-shm.
        ``PRAGMA query_only`` blocks writes from this connection.
        """
        path = self._ensure_db_available()
        try:
            db = await aiosqlite.connect(path.as_posix())
            await db.execute("PRAGMA query_only = ON")
            return db
        except AppError:
            raise
        except Exception as exc:
            raise AppError(
                status_code=503,
                error_code="SERVICE_UNAVAILABLE",
                message=(
                    "Server health database could not be opened "
                    f"at {self._db_path}: {exc}"
                ),
            ) from exc

    async def _run_query(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> list[Any]:
        try:
            async with await self._connect() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(sql, params)
                return await cursor.fetchall()
        except AppError:
            raise
        except Exception as exc:
            raise AppError(
                status_code=503,
                error_code="SERVICE_UNAVAILABLE",
                message=f"Server health database query failed: {exc}",
            ) from exc

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
        return dict(rows[0])

    async def get_checks_for_run(self, run_id: int) -> list[dict[str, Any]]:
        rows = await self._run_query(
            """
            SELECT id, run_id, category, status, message
            FROM health_checks
            WHERE run_id = ?
            ORDER BY category ASC, id ASC
            """,
            (run_id,),
        )
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

        rows = await self._run_query(
            f"""
            SELECT id, run_at, ok_count, warn_count, crit_count, overall_status
            FROM health_runs
            WHERE {where_sql}
            ORDER BY run_at DESC, id DESC
            LIMIT ?
            """,
            params,
        )
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
        rows = await self._run_query(
            f"SELECT COUNT(*) AS cnt FROM health_runs WHERE {where_sql}",
            params,
        )
        if not rows:
            return 0
        return int(rows[0][0] if not hasattr(rows[0], "keys") else rows[0]["cnt"])
