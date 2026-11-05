"""Dashboard overview and employee /me pending_task_count."""

from __future__ import annotations

import pytest

from tests.helpers.auth import seed_employee, employee_auth_header


@pytest.mark.asyncio
async def test_dashboard_overview_returns_kpis(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=8802, role="admin")
    response = await async_client.get(
        "/admin/dashboard/overview",
        headers=employee_auth_header(8802),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert "users" in data
    assert "year_stats" in data
    assert "payment_status_totals" in data
    assert "engagement_participants_total" in data
    assert "operations" not in data


@pytest.mark.asyncio
async def test_dashboard_operations_returns_ops_block(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=8804, role="admin")
    response = await async_client.get(
        "/admin/dashboard/operations",
        headers=employee_auth_header(8804),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    ops = data["operations"]
    assert "engagements" in ops
    assert "participant_issues" in ops
    assert "pending_payments" in ops
    assert "failed_notifications" in ops
    assert "tickets" in ops
    assert "serviceability_issues" in ops


@pytest.mark.asyncio
async def test_employee_me_includes_pending_task_count(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=8803, role="admin")
    response = await async_client.get(
        "/employees/auth/me",
        headers=employee_auth_header(8803),
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert "pending_task_count" in body
    assert isinstance(body["pending_task_count"], int)


@pytest.mark.asyncio
async def test_dashboard_command_bootstrap_shape(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=8805, role="admin")
    response = await async_client.get(
        "/admin/dashboard/command",
        headers=employee_auth_header(8805),
        params={"audience": "all"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert "server_health" in data
    assert "notification_health" in data
    assert "booking_issues" in data
    assert "tickets" in data
    assert "overview_kpis" in data
    assert "action_center" in data
    assert "b2b" in data
    assert "b2c" in data
    kpis = data["overview_kpis"]
    assert "people_registered" in kpis
    assert "tests_completed" in kpis
    assert kpis["people_registered"]["b2b"] is None
    assert kpis["people_registered"]["b2c"] is None
    assert set(data["action_center"].keys()) >= {
        "upcoming_collections",
        "overdue_collections",
        "blood_reports_pending",
        "questionnaire_pending",
        "bio_ai_pending",
        "consultations_pending",
    }
    assert "lifecycle_counts" in data["b2b"]
    assert "camps" in data["b2b"]


@pytest.mark.asyncio
async def test_dashboard_command_audience_b2b_b2c(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=8806, role="admin")
    for audience in ("b2b", "b2c", "all"):
        response = await async_client.get(
            "/admin/dashboard/command",
            headers=employee_auth_header(8806),
            params={"audience": audience},
        )
        assert response.status_code == 200
        assert response.json()["data"]["filters"]["audience"] == audience


@pytest.mark.asyncio
async def test_dashboard_command_drill_people_registered(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=8807, role="admin")
    response = await async_client.get(
        "/admin/dashboard/command/drill",
        headers=employee_auth_header(8807),
        params={"drill": "people_registered", "page": 1, "limit": 10},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["drill"] == "people_registered"
    assert isinstance(body["data"]["items"], list)
    assert "page" in body["meta"]
    assert "total" in body["meta"]
