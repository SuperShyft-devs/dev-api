"""Security and contract tests for Inferior Admin RBAC."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import (
    Employee,
    EmployeeCategoryPermission,
    EmployeeRole,
    EmployeeTaskPermission,
    PermissionCategory,
)
from modules.employee.permissions import (
    PERMISSION_CATEGORY_KEYS,
    TASK_CATALOG,
    PermissionAction,
    PermissionGrant,
    classify_operation,
    grant_allows,
)
from modules.users.models import User


def _headers(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)},
        timedelta(minutes=5),
        secret_key=settings.JWT_SECRET_KEY,
    )
    return {"Authorization": f"Bearer {token}"}


def test_catalog_contract_has_exact_fifteen_categories():
    assert PERMISSION_CATEGORY_KEYS == {
        "users",
        "organizations",
        "engagements",
        "engagement_console",
        "assessments",
        "diagnostics",
        "reports",
        "experts",
        "payments_bookings",
        "notifications",
        "checklists_tasks",
        "support",
        "employees",
        "platform_settings",
        "system_monitoring",
    }


@pytest.mark.parametrize(
    ("method", "path", "category", "action"),
    [
        ("GET", "/users/{user_id}", "users", PermissionAction.view),
        ("PUT", "/users/{user_id}", "users", PermissionAction.edit),
        ("POST", "/employees/users", "users", PermissionAction.edit),
        ("GET", "/employees", "employees", PermissionAction.view),
        ("GET", "/employees/database-backup", "system_monitoring", PermissionAction.edit),
        ("GET", "/engagements/{engagement_id}/console", "engagement_console", PermissionAction.view),
        ("POST", "/engagements/{engagement_id}/console/participants/{user_id}/book", "engagement_console", PermissionAction.edit),
        ("POST", "/engagements/{engagement_id}/participants/load-blood-reports", "reports", PermissionAction.edit),
        ("GET", "/diagnostic-packages", "diagnostics", PermissionAction.view),
        ("POST", "/diagnostic-packages", "diagnostics", PermissionAction.edit),
        ("PATCH", "/diagnostic-test-groups/{group_id}/tests/order", "diagnostics", PermissionAction.edit),
        ("GET", "/audit/integration-sync-logs", "system_monitoring", PermissionAction.view),
        ("POST", "/admin-temp/sync-questionnaire-seed", "system_monitoring", PermissionAction.edit),
    ],
)
def test_operation_manifest_explicit_classification(method, path, category, action):
    capability = classify_operation(path, method)
    assert capability is not None
    assert capability.category == category
    assert capability.action is action


@pytest.mark.parametrize(
    ("method", "path", "task_key"),
    [
        ("GET", "/users/{user_id}", "directory"),
        ("PUT", "/users/{user_id}", "profiles"),
        ("GET", "/users/{user_id}/participant-journey", "participant_journeys"),
        ("POST", "/diagnostic-packages", "packages"),
        ("PATCH", "/diagnostic-test-groups/{group_id}/tests/order", "tests_groups"),
        ("GET", "/reports/camps/{camp_no}", "camp_reports"),
        ("GET", "/employees/database-backup", "database_backup"),
        ("GET", "/organizations/{organization_id}/participants", "participants"),
        ("POST", "/uploads/organizations/logo", "assets"),
        ("POST", "/engagements/{engagement_id}/create-metsights-profiles", "integrations"),
        ("GET", "/engagements/{engagement_id}/onboarding-assistants", "staffing"),
        ("POST", "/engagements/{engagement_id}/console/participants/{user_id}/book-home-collection/book", "home_collection"),
        ("POST", "/questionnaire/questions/{question_id}/healthy-habit-rules", "healthy_habit_rules"),
        ("POST", "/diagnostic-packages/{package_id}/test-groups", "package_test_groups"),
        ("POST", "/notifications/dispatch", "dispatch"),
        ("PUT", "/engagements/{engagement_id}/notifications", "engagement_configuration"),
        ("PUT", "/checklist-templates/{template_id}/items/{item_id}", "template_items"),
        ("PATCH", "/platform-settings/b2c-onboarding", "b2c_onboarding"),
    ],
)
def test_operation_manifest_assigns_nested_tasks(method, path, task_key):
    capability = classify_operation(path, method)
    assert capability is not None
    assert capability.task_key == task_key


def test_every_category_has_configurable_tasks():
    assert set(TASK_CATALOG) == PERMISSION_CATEGORY_KEYS
    for tasks in TASK_CATALOG.values():
        assert tasks
        assert len({task[0] for task in tasks}) == len(tasks)


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/users/me"),
        ("POST", "/users"),
        ("POST", "/auth/verify-otp"),
        ("POST", "/webhooks/healthians"),
        ("GET", "/checklist/my-tasks"),
        ("POST", "/support/tickets"),
        ("GET", "/experts/portal/me"),
    ],
)
def test_operation_manifest_explicit_exclusions(method, path):
    assert classify_operation(path, method) is None


def test_every_registered_operation_is_classified_or_excluded(fastapi_app):
    for route in fastapi_app.routes:
        path = getattr(route, "path", None)
        for method in getattr(route, "methods", set()) or set():
            if path and method not in {"HEAD", "OPTIONS"}:
                capability = classify_operation(path, method)
                if capability is not None:
                    assert capability.task_key in {
                        task[0] for task in TASK_CATALOG[capability.category]
                    }


def test_every_production_operation_is_classified_or_excluded():
    from main import app

    for route in app.routes:
        path = getattr(route, "path", None)
        for method in getattr(route, "methods", set()) or set():
            if path and method not in {"HEAD", "OPTIONS"}:
                capability = classify_operation(path, method)
                if capability is not None:
                    assert capability.task_key in {
                        task[0] for task in TASK_CATALOG[capability.category]
                    }


def test_edit_grant_implies_read_but_view_does_not_allow_edit():
    edit = PermissionGrant(can_view=True, can_edit=True)
    view = PermissionGrant(can_view=True, can_edit=False)
    assert grant_allows(edit, PermissionAction.view)
    assert grant_allows(edit, PermissionAction.edit)
    assert grant_allows(view, PermissionAction.view)
    assert not grant_allows(view, PermissionAction.edit)
    assert not grant_allows(None, PermissionAction.view)


async def _seed_employee(
    db,
    *,
    user_id: int,
    employee_id: int,
    role: EmployeeRole,
    grants: dict[str, tuple[bool, bool]] | None = None,
):
    db.add(User(user_id=user_id, phone=f"7{user_id:09d}", age=30, status="active"))
    await db.flush()
    db.add(
        Employee(
            employee_id=employee_id,
            user_id=user_id,
            role=role,
            status="active",
        )
    )
    await db.flush()
    for key, (can_view, can_edit) in (grants or {}).items():
        if await db.get(PermissionCategory, key) is None:
            db.add(
                PermissionCategory(
                    category_key=key,
                    display_name=key.replace("_", " ").title(),
                    description="Test category",
                    display_order=sorted(PERMISSION_CATEGORY_KEYS).index(key) + 1,
                    is_active=True,
                )
            )
            await db.flush()
        db.add(
            EmployeeCategoryPermission(
                employee_id=employee_id,
                category_key=key,
                can_view=can_view,
                can_edit=can_edit,
            )
        )
    await db.commit()


@pytest.mark.asyncio
async def test_users_me_exposes_additive_permission_contract(async_client, test_db_session):
    await _seed_employee(
        test_db_session,
        user_id=9701,
        employee_id=9701,
        role=EmployeeRole.inferior_admin,
        grants={"users": (True, False)},
    )
    response = await async_client.get("/users/me", headers=_headers(9701))
    assert response.status_code == 200
    employee = response.json()["data"]["employee"]
    assert employee["role"] == "inferior_admin"
    assert employee["permissions"]["version"] == 1
    assert employee["permissions"]["categories"]["users"] == {
        "can_view": True,
        "can_edit": False,
    }


@pytest.mark.asyncio
async def test_inferior_admin_view_and_edit_enforcement(async_client, test_db_session):
    await _seed_employee(
        test_db_session,
        user_id=9702,
        employee_id=9702,
        role=EmployeeRole.inferior_admin,
        grants={"users": (True, False)},
    )
    read = await async_client.get("/users?page=1&limit=1", headers=_headers(9702))
    assert read.status_code == 200

    write = await async_client.put(
        "/users/999999",
        headers=_headers(9702),
        json={"phone": "7000000000", "status": "active"},
    )
    assert write.status_code == 403
    assert write.json() == {
        "error_code": "PERMISSION_DENIED",
        "message": "You do not have permission to edit Users.",
        "category": "users",
        "action": "edit",
    }


@pytest.mark.asyncio
async def test_task_override_allows_directory_but_denies_profile_edit(
    async_client, test_db_session
):
    await _seed_employee(
        test_db_session,
        user_id=9720,
        employee_id=9720,
        role=EmployeeRole.inferior_admin,
        grants={"users": (True, True)},
    )
    test_db_session.add(
        EmployeeTaskPermission(
            employee_id=9720,
            category_key="users",
            task_key="directory",
            can_view=True,
            can_edit=False,
        )
    )
    await test_db_session.commit()

    read = await async_client.get("/users?page=1&limit=1", headers=_headers(9720))
    assert read.status_code == 200

    write = await async_client.put(
        "/users/999999",
        headers=_headers(9720),
        json={"phone": "7000000000", "status": "active"},
    )
    assert write.status_code == 403
    assert write.json()["task_key"] == "profiles"


@pytest.mark.asyncio
async def test_permission_is_checked_before_resource_lookup(async_client, test_db_session):
    await _seed_employee(
        test_db_session,
        user_id=9703,
        employee_id=9703,
        role=EmployeeRole.inferior_admin,
    )
    response = await async_client.get("/users/999999", headers=_headers(9703))
    assert response.status_code == 403
    assert response.json()["error_code"] == "PERMISSION_DENIED"


@pytest.mark.asyncio
async def test_admin_temp_rejects_ordinary_authenticated_user(async_client, test_db_session):
    test_db_session.add(
        User(user_id=9704, phone="7000097040", age=30, status="active")
    )
    await test_db_session.commit()
    response = await async_client.post(
        "/admin-temp/sync-questionnaire-seed", headers=_headers(9704)
    )
    assert response.status_code == 403
    assert response.json()["error_code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_permission_api_rejects_stale_version(async_client, test_db_session):
    await _seed_employee(
        test_db_session,
        user_id=9705,
        employee_id=9705,
        role=EmployeeRole.admin,
    )
    await _seed_employee(
        test_db_session,
        user_id=9706,
        employee_id=9706,
        role=EmployeeRole.inferior_admin,
    )
    response = await async_client.put(
        "/employees/9706/permissions",
        headers=_headers(9705),
        json={"expected_version": 99, "permissions": []},
    )
    assert response.status_code == 409
    assert response.json()["error_code"] == "PERMISSIONS_VERSION_CONFLICT"


@pytest.mark.asyncio
async def test_only_full_admin_can_read_permission_catalog(async_client, test_db_session):
    await _seed_employee(
        test_db_session,
        user_id=9707,
        employee_id=9707,
        role=EmployeeRole.inferior_admin,
        grants={"employees": (True, True)},
    )
    response = await async_client.get(
        "/employees/permission-categories", headers=_headers(9707)
    )
    assert response.status_code == 403
    assert response.json()["message"] == "Only administrators can manage roles and permissions."


@pytest.mark.asyncio
async def test_backup_requires_system_monitoring_edit(async_client, test_db_session):
    await _seed_employee(
        test_db_session,
        user_id=9708,
        employee_id=9708,
        role=EmployeeRole.inferior_admin,
        grants={"system_monitoring": (True, False)},
    )
    response = await async_client.get(
        "/employees/database-backup", headers=_headers(9708)
    )
    assert response.status_code == 403
    assert response.json() == {
        "error_code": "PERMISSION_DENIED",
        "message": "You do not have permission to edit System Monitoring.",
        "category": "system_monitoring",
        "action": "edit",
    }


@pytest.mark.asyncio
async def test_admin_creates_role_and_grants_atomically(async_client, test_db_session):
    await _seed_employee(
        test_db_session,
        user_id=9709,
        employee_id=9709,
        role=EmployeeRole.admin,
    )
    if await test_db_session.get(PermissionCategory, "users") is None:
        test_db_session.add(
            PermissionCategory(
                category_key="users",
                display_name="Users",
                description="Users",
                display_order=1,
                is_active=True,
            )
        )
    test_db_session.add(
        User(user_id=9710, phone="7000097100", age=30, status="active")
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/employees",
        headers=_headers(9709),
        json={
            "user_id": 9710,
            "role": "inferior_admin",
            "permissions": [
                {
                    "category_key": "users",
                    "can_view": True,
                    "can_edit": True,
                    "tasks": [
                        {
                            "task_key": task_key,
                            "can_view": True,
                            "can_edit": task_key == "profiles",
                        }
                        for task_key, _, _ in TASK_CATALOG["users"]
                    ],
                }
            ],
        },
    )
    assert response.status_code == 201
    employee_id = response.json()["data"]["employee_id"]
    grant = await test_db_session.get(
        EmployeeCategoryPermission, (employee_id, "users")
    )
    assert grant is not None
    assert grant.can_view is True
    assert grant.can_edit is True
    profile_task = await test_db_session.get(
        EmployeeTaskPermission, (employee_id, "users", "profiles")
    )
    assert profile_task is not None
    assert profile_task.can_edit is True


@pytest.mark.asyncio
async def test_self_permission_escalation_is_protected(async_client, test_db_session):
    await _seed_employee(
        test_db_session,
        user_id=9711,
        employee_id=9711,
        role=EmployeeRole.admin,
    )
    response = await async_client.put(
        "/employees/9711/permissions",
        headers=_headers(9711),
        json={"expected_version": 1, "permissions": []},
    )
    assert response.status_code == 403
    assert response.json()["error_code"] == "PROTECTED_ADMIN"


@pytest.mark.asyncio
async def test_protected_staff_user_creation_requires_auth(async_client):
    response = await async_client.post(
        "/employees/users",
        json={"phone": "7000097999", "status": "active"},
    )
    assert response.status_code == 401
