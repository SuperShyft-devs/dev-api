"""Unit tests for camp report intelligence enrichment endpoints."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from core.config import settings
from core.exceptions import AppError
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)},
        timedelta(minutes=5),
        secret_key=settings.JWT_SECRET_KEY,
    )
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(
        User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active")
    )
    await test_db_session.flush()
    test_db_session.add(
        Employee(
            employee_id=employee_id,
            user_id=user_id,
            role="admin",
            status="active",
        )
    )
    await test_db_session.commit()


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------

_MOCK_INTELLIGENCE = {
    "both": {
        "tone": "concern",
        "observation": "Sleep quality is below average.",
        "explanation": "Irregular sleep patterns affect recovery.",
        "recommendation": "Encourage consistent sleep schedules.",
    }
}


def _make_mock_enrich(*, raise_error: AppError | None = None):
    """Return an async mock for ``CampReportsService.enrich_camp_report_section``.

    When *raise_error* is set the mock raises the given ``AppError`` instead of
    returning a result.
    """

    async def _mock(
        self,
        db,
        *,
        employee,
        camp_no,
        section,
        department=None,
        city=None,
    ):
        if raise_error is not None:
            raise raise_error

        if city is not None and department is not None:
            scope_type = "city_department"
        elif city is not None:
            scope_type = "city"
        elif department is not None:
            scope_type = "department"
        else:
            scope_type = "camp"

        return {
            "camp_no": camp_no,
            "scope": {
                "type": scope_type,
                "city": city,
                "department": department,
            },
            "section": section.strip().lower(),
            "section_key": "distribution_by_sleeping_hours",
            "intelligence": _MOCK_INTELLIGENCE,
        }

    return _mock


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_camp_requires_auth(async_client):
    response = await async_client.put(
        "/reports/camps/1/enrich",
        json={"section": "sleep"},
    )
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# Valid section — camp scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_camp_valid_section(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7801, employee_id=81)
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.CampReportsService"
        ".enrich_camp_report_section",
        _make_mock_enrich(),
    )

    response = await async_client.put(
        "/reports/camps/1/enrich",
        headers=_auth_header(7801),
        json={"section": "sleep"},
    )
    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    data = body["data"]
    assert data["camp_no"] == 1
    assert data["section"] == "sleep"
    assert data["section_key"] == "distribution_by_sleeping_hours"
    assert data["intelligence"] is not None
    assert data["intelligence"]["both"]["tone"] == "concern"


# ---------------------------------------------------------------------------
# Invalid section
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_camp_invalid_section(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7802, employee_id=82)
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.CampReportsService"
        ".enrich_camp_report_section",
        _make_mock_enrich(
            raise_error=AppError(
                status_code=400,
                error_code="INVALID_SECTION",
                message="Section 'does_not_exist' is not a valid enrichable section.",
            )
        ),
    )

    response = await async_client.put(
        "/reports/camps/1/enrich",
        headers=_auth_header(7802),
        json={"section": "does_not_exist"},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_SECTION"


# ---------------------------------------------------------------------------
# Camp not found
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_camp_not_found(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7803, employee_id=83)
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.CampReportsService"
        ".enrich_camp_report_section",
        _make_mock_enrich(
            raise_error=AppError(
                status_code=404,
                error_code="CAMP_NOT_FOUND",
                message="Camp does not exist",
            )
        ),
    )

    response = await async_client.put(
        "/reports/camps/99999/enrich",
        headers=_auth_header(7803),
        json={"section": "sleep"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "CAMP_NOT_FOUND"


# ---------------------------------------------------------------------------
# Four scope variants
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_correct_scope_camp(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7804, employee_id=84)
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.CampReportsService"
        ".enrich_camp_report_section",
        _make_mock_enrich(),
    )

    response = await async_client.put(
        "/reports/camps/1/enrich",
        headers=_auth_header(7804),
        json={"section": "sleep"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["scope"]["type"] == "camp"
    assert data["scope"]["city"] is None
    assert data["scope"]["department"] is None


@pytest.mark.asyncio
async def test_enrich_correct_scope_department(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7805, employee_id=85)
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.CampReportsService"
        ".enrich_camp_report_section",
        _make_mock_enrich(),
    )

    response = await async_client.put(
        "/reports/camps/1/department/engineering/enrich",
        headers=_auth_header(7805),
        json={"section": "sleep"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["scope"]["type"] == "department"
    assert data["scope"]["department"] == "engineering"
    assert data["scope"]["city"] is None


@pytest.mark.asyncio
async def test_enrich_correct_scope_city(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7806, employee_id=86)
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.CampReportsService"
        ".enrich_camp_report_section",
        _make_mock_enrich(),
    )

    response = await async_client.put(
        "/reports/camps/1/Mumbai/enrich",
        headers=_auth_header(7806),
        json={"section": "sleep"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["scope"]["type"] == "city"
    assert data["scope"]["city"] == "Mumbai"
    assert data["scope"]["department"] is None


@pytest.mark.asyncio
async def test_enrich_correct_scope_city_department(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7807, employee_id=87)
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.CampReportsService"
        ".enrich_camp_report_section",
        _make_mock_enrich(),
    )

    response = await async_client.put(
        "/reports/camps/1/Mumbai/department/engineering/enrich",
        headers=_auth_header(7807),
        json={"section": "sleep"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["scope"]["type"] == "city_department"
    assert data["scope"]["city"] == "Mumbai"
    assert data["scope"]["department"] == "engineering"


# ---------------------------------------------------------------------------
# Empty section body validation (Pydantic)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_camp_empty_section_rejected(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7808, employee_id=88)

    response = await async_client.put(
        "/reports/camps/1/enrich",
        headers=_auth_header(7808),
        json={"section": ""},
    )
    # Pydantic rejects min_length=1
    assert response.status_code == 400


# ---------------------------------------------------------------------------
# Multiple valid sections
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enrich_each_valid_section(
    async_client, test_db_session, monkeypatch
):
    """Verify all 7 enrichable section names are accepted by the mock."""
    await _seed_employee(test_db_session, user_id=7809, employee_id=89)
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.CampReportsService"
        ".enrich_camp_report_section",
        _make_mock_enrich(),
    )

    valid_sections = [
        "overall_risk",
        "physical_activity",
        "sleep",
        "oxidative_stress",
        "participation",
        "disease_risks",
        "positive_highlights",
    ]
    for section in valid_sections:
        response = await async_client.put(
            "/reports/camps/1/enrich",
            headers=_auth_header(7809),
            json={"section": section},
        )
        assert response.status_code == 200, f"Failed for section: {section}"
        assert response.json()["data"]["section"] == section
