"""Tests for server health monitoring routes."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import aiosqlite
import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.server_health.dependencies import get_server_health_service
from modules.server_health.repository import ServerHealthRepository
from modules.server_health.service import ServerHealthService
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_health_db(db_path: Path) -> None:
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            CREATE TABLE health_runs (
                id INTEGER PRIMARY KEY,
                run_at TEXT NOT NULL,
                ok_count INTEGER NOT NULL,
                warn_count INTEGER NOT NULL,
                crit_count INTEGER NOT NULL,
                overall_status TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE health_checks (
                id INTEGER PRIMARY KEY,
                run_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT NOT NULL
            )
            """
        )
        await db.executemany(
            """
            INSERT INTO health_runs (id, run_at, ok_count, warn_count, crit_count, overall_status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (1, "2026-07-15 10:00:00", 8, 1, 0, "WARNING"),
                (2, "2026-07-16 10:00:00", 9, 0, 0, "HEALTHY"),
            ],
        )
        await db.executemany(
            """
            INSERT INTO health_checks (id, run_id, category, status, message)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, 2, "MEMORY", "OK", "Memory usage is healthy (20%)"),
                (2, 2, "SYSTEM LOAD", "OK", "Load average is normal"),
                (3, 2, "WEB ENDPOINTS (nginx sites)", "WARN", "tasktracker.supershyft.com unreachable"),
                (4, 1, "MEMORY", "WARN", "Memory usage elevated (85%)"),
            ],
        )
        await db.commit()


@pytest.fixture
async def health_db_path(tmp_path: Path) -> Path:
    db_path = tmp_path / "health.db"
    await _seed_health_db(db_path)
    return db_path


@pytest.fixture
def override_server_health_service(fastapi_app, health_db_path: Path):
    service = ServerHealthService(ServerHealthRepository(str(health_db_path)))
    fastapi_app.dependency_overrides[get_server_health_service] = lambda: service
    yield
    fastapi_app.dependency_overrides.pop(get_server_health_service, None)


@pytest.mark.asyncio
async def test_server_health_current_requires_auth(async_client):
    response = await async_client.get("/server-health/current")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_server_health_current_requires_admin(async_client, test_db_session, override_server_health_service):
    uid = 92001
    test_db_session.add(User(user_id=uid, age=30, phone="92001000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=92001, user_id=uid, role="onboarding_assistant", status="active")
    )
    await test_db_session.commit()

    response = await async_client.get("/server-health/current", headers=_auth_header(uid))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_server_health_current_returns_latest_grouped(
    async_client, test_db_session, override_server_health_service
):
    uid = 92002
    test_db_session.add(User(user_id=uid, age=30, phone="92002000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=92002, user_id=uid, role="admin", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/server-health/current", headers=_auth_header(uid))
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["run"]["id"] == 2
    assert body["data"]["run"]["overall_status"] == "HEALTHY"
    categories = {group["category"]: group["checks"] for group in body["data"]["checks_by_category"]}
    assert len(categories["MEMORY"]) == 1
    assert categories["MEMORY"][0]["status"] == "OK"
    assert len(categories["WEB ENDPOINTS (nginx sites)"]) == 1


@pytest.mark.asyncio
async def test_server_health_history_respects_limit_and_dates(
    async_client, test_db_session, override_server_health_service
):
    uid = 92003
    test_db_session.add(User(user_id=uid, age=30, phone="92003000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=92003, user_id=uid, role="admin", status="active"))
    await test_db_session.commit()

    response = await async_client.get(
        "/server-health/history",
        headers=_auth_header(uid),
        params={"limit": 1, "from": "2026-07-16T00:00:00", "to": "2026-07-16T23:59:59"},
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["data"]) == 1
    assert body["data"][0]["id"] == 2
    assert body["meta"]["limit"] == 1
    assert body["meta"]["total"] == 1


@pytest.mark.asyncio
async def test_server_health_returns_503_when_db_missing(async_client, test_db_session, fastapi_app, tmp_path):
    missing_path = tmp_path / "missing.db"
    service = ServerHealthService(ServerHealthRepository(str(missing_path)))
    fastapi_app.dependency_overrides[get_server_health_service] = lambda: service

    uid = 92004
    test_db_session.add(User(user_id=uid, age=30, phone="92004000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=92004, user_id=uid, role="admin", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/server-health/current", headers=_auth_header(uid))
    assert response.status_code == 503
    assert "not available" in response.json()["message"].lower()

    fastapi_app.dependency_overrides.pop(get_server_health_service, None)
