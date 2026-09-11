"""Integration tests for employee routes (employee-only)."""

from __future__ import annotations

from io import BytesIO

import pytest
from openpyxl import load_workbook

from core.subject_auth import create_access_token
from modules.employee.models import Employee
from modules.users.models import User
from tests.helpers.auth import employee_auth_header, make_employee, seed_employee, user_auth_header


def _auth_header(employee_id: int) -> dict[str, str]:
    return employee_auth_header(employee_id)


async def _seed_admin_employee(test_db_session, *, employee_id: int):
    await seed_employee(test_db_session, employee_id=employee_id, role="admin")


@pytest.mark.asyncio
async def test_database_backup_returns_xlsx_for_employee(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=501)

    response = await async_client.get("/employees/database-backup", headers=_auth_header(501))
    assert response.status_code == 200
    assert "spreadsheetml" in (response.headers.get("content-type") or "")
    assert "attachment" in (response.headers.get("content-disposition") or "").lower()

    wb = load_workbook(BytesIO(response.content))
    names = {n.lower() for n in wb.sheetnames}
    assert "users" in names
    assert "employee" in names


@pytest.mark.asyncio
async def test_database_backup_accepts_access_token_query(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=502)

    token = create_access_token(502, "employee")
    response = await async_client.get(f"/employees/database-backup?access_token={token}")
    assert response.status_code == 200
    wb = load_workbook(BytesIO(response.content))
    assert len(wb.sheetnames) >= 1


@pytest.mark.asyncio
async def test_database_backup_rejects_non_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=8012, phone="8012000000", age=30, status="active"))
    await test_db_session.commit()

    response = await async_client.get("/employees/database-backup", headers=user_auth_header(8012))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_employee_requires_auth(async_client):
    response = await async_client.post(
        "/employees",
        json={"name": "A", "phone": "8000000001", "role": "admin"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_employee_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=8001, phone="8001000000", age=30, status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/employees",
        headers=user_auth_header(8001),
        json={"name": "B", "phone": "8000000002", "role": "admin"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_employee_creates_row(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=11)

    payload = {
        "name": "New Admin",
        "phone": "9000000000",
        "email": "newadmin@test.example",
        "role": "admin",
    }
    response = await async_client.post("/employees", headers=_auth_header(11), json=payload)
    assert response.status_code == 201

    employee_id = response.json()["data"]["employee_id"]
    created = await test_db_session.get(Employee, employee_id)
    assert created is not None
    assert created.name == "New Admin"
    assert created.phone == "9000000000"
    assert (created.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_create_employee_rejects_partner_roles(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=17)

    for idx, role in enumerate(
        ("onboarding_assistant", "expert", "phlebo", "organization_manager"),
        start=1,
    ):
        response = await async_client.post(
            "/employees",
            headers=_auth_header(17),
            json={"name": "Bad Role", "phone": f"90000000{idx:02d}", "role": role},
        )
        assert response.status_code == 400


@pytest.mark.asyncio
async def test_list_employees_paginates_and_filters(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=12)

    test_db_session.add_all(
        [
            make_employee(employee_id=100, name="Alice Admin", role="admin", status="active"),
            make_employee(
                employee_id=101,
                name="Bob Ops",
                role="organization_manager",
                status="inactive",
                phone="9101000000",
                email="bob@test.example",
            ),
        ]
    )
    await test_db_session.commit()

    response = await async_client.get("/employees?page=1&limit=10&status=active", headers=_auth_header(12))
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    for row in body["data"]:
        assert (row["status"] or "").lower() == "active"

    alice = next((r for r in body["data"] if r["employee_id"] == 100), None)
    assert alice is not None
    assert alice["name"] == "Alice Admin"


@pytest.mark.asyncio
async def test_list_employees_search_matches_name_and_role_substring(async_client, test_db_session):
    """Free-text search must not 500 when ILIKE runs against the employee_role enum."""
    await _seed_admin_employee(test_db_session, employee_id=16)

    test_db_session.add_all(
        [
            make_employee(
                employee_id=110,
                name="Rina Shah",
                role="inferior_admin",
                status="active",
                phone="9110000000",
                email="rina@test.example",
            ),
            make_employee(
                employee_id=111,
                name="Deepa Gupta",
                role="organization_manager",
                status="active",
                phone="9111000000",
                email="deepa@test.example",
            ),
        ]
    )
    await test_db_session.commit()

    by_role = await async_client.get(
        "/employees?page=1&limit=10&search=org&sort_by=employee_id&sort_dir=desc",
        headers=_auth_header(16),
    )
    assert by_role.status_code == 200
    role_ids = {row["employee_id"] for row in by_role.json()["data"]}
    assert 111 in role_ids

    by_name = await async_client.get(
        "/employees?page=1&limit=10&search=Rina",
        headers=_auth_header(16),
    )
    assert by_name.status_code == 200
    name_ids = {row["employee_id"] for row in by_name.json()["data"]}
    assert 110 in name_ids


@pytest.mark.asyncio
async def test_get_employee_returns_details(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=13)

    test_db_session.add(
        make_employee(
            employee_id=201,
            name="Carol Operator",
            role="organization_manager",
            status="active",
            phone="9201000000",
            email="carol@test.example",
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/employees/201", headers=_auth_header(13))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["employee_id"] == 201
    assert data["name"] == "Carol Operator"
    assert data["phone"] == "9201000000"


@pytest.mark.asyncio
async def test_update_employee_updates_fields(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=14)

    test_db_session.add(
        make_employee(
            employee_id=301,
            name="Old Name",
            role="organization_manager",
            status="active",
            phone="9301000000",
            email="old@test.example",
        )
    )
    await test_db_session.commit()

    payload = {
        "name": "Updated Admin",
        "phone": "9301000000",
        "email": "old@test.example",
        "role": "admin",
    }
    response = await async_client.put("/employees/301", headers=_auth_header(14), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(Employee, 301)
    assert updated is not None
    assert updated.role == "admin"
    assert updated.name == "Updated Admin"


@pytest.mark.asyncio
async def test_update_employee_status_sets_inactive(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=15)

    test_db_session.add(
        make_employee(
            employee_id=401,
            name="Status Target",
            role="organization_manager",
            status="active",
            phone="9401000000",
            email="status@test.example",
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/employees/401/status",
        headers=_auth_header(15),
        json={"status": "inactive"},
    )
    assert response.status_code == 200

    updated = await test_db_session.get(Employee, 401)
    assert updated is not None
    assert (updated.status or "").lower() == "inactive"


@pytest.mark.asyncio
async def test_update_employee_status_rejects_inactive_for_employee_one(async_client, test_db_session):
    await _seed_admin_employee(test_db_session, employee_id=18)

    response = await async_client.patch(
        "/employees/1/status",
        headers=_auth_header(18),
        json={"status": "inactive"},
    )
    assert response.status_code == 403

    protected = await test_db_session.get(Employee, 1)
    assert protected is not None
    assert (protected.status or "").lower() == "active"
