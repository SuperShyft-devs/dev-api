"""Tests for test group provider validation against assigned packages."""

from __future__ import annotations

import pytest

from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageGroup,
    DiagnosticPackageTestGroup,
    DiagnosticProfileGroup,
    DiagnosticTestGroup,
)
from tests.helpers.auth import employee_auth_header, seed_employee


async def _seed_group_on_packages(
    session,
    *,
    group_provider: str | None,
    packages: list[tuple[str, str | None]],
) -> tuple[DiagnosticTestGroup, list[DiagnosticPackage]]:
    profile = DiagnosticProfileGroup()
    session.add(profile)
    await session.flush()
    tg = DiagnosticTestGroup(
        profile_group_id=profile.profile_group_id,
        is_default_for_group=True,
        group_name="Shared TG",
        group_key=f"shared_tg_{id(session)}",
        diagnostic_provider=group_provider,
    )
    session.add(tg)
    await session.flush()
    created_packages: list[DiagnosticPackage] = []
    for idx, (name, pkg_provider) in enumerate(packages):
        pkg_group = DiagnosticPackageGroup()
        session.add(pkg_group)
        await session.flush()
        package = DiagnosticPackage(
            package_group_id=pkg_group.package_group_id,
            is_default_for_group=True,
            package_name=name,
            diagnostic_provider=pkg_provider,
            status="active",
            package_for="public",
        )
        session.add(package)
        await session.flush()
        session.add(
            DiagnosticPackageTestGroup(
                diagnostic_package_id=package.diagnostic_package_id,
                group_id=tg.group_id,
                display_order=idx,
            )
        )
        created_packages.append(package)
    await session.commit()
    await session.refresh(tg)
    for package in created_packages:
        await session.refresh(package)
    return tg, created_packages


@pytest.mark.asyncio
async def test_update_test_group_provider_rejected_when_packages_mismatch(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=940, role="admin")
    tg, packages = await _seed_group_on_packages(
        test_db_session,
        group_provider=None,
        packages=[("Healthians pkg", "healthians"), ("Orange pkg", "orange_health")],
    )
    healthians_pkg, orange_pkg = packages

    response = await async_client.put(
        f"/diagnostic-test-groups/{tg.group_id}",
        headers=employee_auth_header(940),
        json={"diagnostic_provider": "healthians"},
    )
    assert response.status_code == 400
    body = response.json()
    assert body["error_code"] == "DIAGNOSTIC_TEST_GROUP_PACKAGE_PROVIDER_MISMATCH"
    assert "packages" in body
    mismatch_ids = {p["diagnostic_package_id"] for p in body["packages"]}
    assert orange_pkg.diagnostic_package_id in mismatch_ids
    assert healthians_pkg.diagnostic_package_id not in mismatch_ids


@pytest.mark.asyncio
async def test_update_test_group_provider_allowed_after_removing_mismatch(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=941, role="admin")
    tg, packages = await _seed_group_on_packages(
        test_db_session,
        group_provider=None,
        packages=[("Healthians pkg", "healthians"), ("Orange pkg", "orange_health")],
    )
    orange_pkg = packages[1]

    remove = await async_client.delete(
        f"/diagnostic-packages/{orange_pkg.diagnostic_package_id}/test-groups/{tg.group_id}",
        headers=employee_auth_header(941),
    )
    assert remove.status_code == 204

    response = await async_client.put(
        f"/diagnostic-test-groups/{tg.group_id}",
        headers=employee_auth_header(941),
        json={"diagnostic_provider": "healthians"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["diagnostic_provider"] == "healthians"


@pytest.mark.asyncio
async def test_list_packages_for_test_group(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=942, role="admin")
    tg, packages = await _seed_group_on_packages(
        test_db_session,
        group_provider=None,
        packages=[("Pkg A", "healthians")],
    )

    response = await async_client.get(
        f"/diagnostic-test-groups/{tg.group_id}/packages",
        headers=employee_auth_header(942),
    )
    assert response.status_code == 200
    rows = response.json()["data"]
    assert len(rows) == 1
    assert rows[0]["diagnostic_package_id"] == packages[0].diagnostic_package_id
    assert rows[0]["package_name"] == "Pkg A"
