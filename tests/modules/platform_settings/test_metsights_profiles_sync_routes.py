"""Tests for Metsights profile sync on platform settings."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.metsights.schemas import MetsightsProfilesPage
from modules.metsights.service import MetsightsService
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id:011d}", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


def _mock_list_page(monkeypatch, *, rows: list[dict], count: int = 0):
    async def _list_profiles_page(self, *, page: int = 1):
        return MetsightsProfilesPage(
            detail="All data.",
            count=count or len(rows),
            next=f"https://api.metsights.com/profiles/?page={page + 1}" if page == 1 else None,
            previous=None if page == 1 else f"https://api.metsights.com/profiles/?page={page - 1}",
            data=rows,
        )

    monkeypatch.setattr(MetsightsService, "list_profiles_page", _list_profiles_page)


@pytest.mark.asyncio
async def test_metsights_profiles_stats_requires_auth(async_client):
    response = await async_client.get("/platform-settings/metsights-profiles/stats")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_metsights_profiles_import_page_requires_auth(async_client):
    response = await async_client.post("/platform-settings/metsights-profiles/import-page", json={"page": 1})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_metsights_profiles_stats_returns_counts(async_client, test_db_session, monkeypatch):
    uid = 9201
    await _seed_employee(test_db_session, user_id=uid, employee_id=9201)
    test_db_session.add(
        User(
            user_id=9202,
            age=28,
            phone="92020000001",
            status="active",
            metsights_profile_id="ms-existing-001",
        )
    )
    await test_db_session.commit()

    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    _mock_list_page(monkeypatch, rows=[], count=100)

    response = await async_client.get(
        "/platform-settings/metsights-profiles/stats",
        headers=_auth_header(uid),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["local_total_users"] >= 2
    assert data["local_with_metsights_profile_id"] >= 1
    assert data["metsights_total"] == 100
    assert data["estimated_not_imported"] == max(0, 100 - data["local_with_metsights_profile_id"])


@pytest.mark.asyncio
async def test_metsights_profiles_import_page_skips_existing_id(async_client, test_db_session, monkeypatch):
    uid = 9203
    await _seed_employee(test_db_session, user_id=uid, employee_id=9203)
    pid = "019e3c22-6c81-0cb7-88d4-8b74f723f89a"
    test_db_session.add(
        User(
            user_id=9204,
            age=30,
            phone="92040000001",
            status="active",
            metsights_profile_id=pid,
        )
    )
    await test_db_session.commit()

    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    _mock_list_page(
        monkeypatch,
        count=1,
        rows=[
            {
                "id": pid,
                "first_name": "Skip",
                "last_name": "Me",
                "phone": "+919999999999",
                "email": "skip@example.com",
                "gender": "Male",
                "age": 30,
            }
        ],
    )

    response = await async_client.post(
        "/platform-settings/metsights-profiles/import-page",
        headers=_auth_header(uid),
        json={"page": 1},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["skipped"] == 1
    assert body["created"] == 0
    assert body["linked"] == 0
    assert len(body["skipped_items"]) == 1
    assert body["skipped_items"][0]["metsights_profile_id"] == pid
    assert "Already linked" in body["skipped_items"][0]["reason"]


@pytest.mark.asyncio
async def test_metsights_profiles_import_page_creates_new(async_client, test_db_session, monkeypatch):
    uid = 9205
    await _seed_employee(test_db_session, user_id=uid, employee_id=9205)

    pid = "019e3c22-new-profile-uuid-0001"
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    _mock_list_page(
        monkeypatch,
        count=1,
        rows=[
            {
                "id": pid,
                "first_name": "New",
                "last_name": "Profile",
                "phone": "+918888777666",
                "email": "newprofile@example.com",
                "gender": "Female",
                "date_of_birth": "1990-01-15",
                "age": 36,
            }
        ],
    )

    response = await async_client.post(
        "/platform-settings/metsights-profiles/import-page",
        headers=_auth_header(uid),
        json={"page": 1},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["created"] == 1
    assert body["skipped"] == 0

    row = (
        await test_db_session.execute(
            text("SELECT metsights_profile_id, first_name FROM users WHERE metsights_profile_id = :p"),
            {"p": pid},
        )
    ).one()
    assert row.metsights_profile_id == pid
    assert row.first_name == "New"


@pytest.mark.asyncio
async def test_metsights_profiles_import_page_links_by_phone(async_client, test_db_session, monkeypatch):
    uid = 9206
    await _seed_employee(test_db_session, user_id=uid, employee_id=9206)
    phone = "+917777666555"
    test_db_session.add(User(user_id=9207, age=40, phone=phone, status="active"))
    await test_db_session.commit()

    pid = "019e3c22-link-by-phone-uuid01"
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    _mock_list_page(
        monkeypatch,
        count=1,
        rows=[
            {
                "id": pid,
                "first_name": "Linked",
                "last_name": "User",
                "phone": phone,
                "email": "linked@example.com",
                "gender": "1",
                "age": 40,
            }
        ],
    )

    response = await async_client.post(
        "/platform-settings/metsights-profiles/import-page",
        headers=_auth_header(uid),
        json={"page": 1},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["linked"] == 1
    assert body["created"] == 0

    row = (
        await test_db_session.execute(
            text("SELECT metsights_profile_id FROM users WHERE user_id = 9207"),
        )
    ).one()
    assert row.metsights_profile_id == pid


@pytest.mark.asyncio
async def test_metsights_profiles_stats_503_without_api_key(async_client, test_db_session, monkeypatch):
    uid = 9208
    await _seed_employee(test_db_session, user_id=uid, employee_id=9208)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    response = await async_client.get(
        "/platform-settings/metsights-profiles/stats",
        headers=_auth_header(uid),
    )
    assert response.status_code == 503
