"""Tests for Healthians provider test diff on diagnostic packages."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageGroup,
    DiagnosticPackageTestGroup,
    DiagnosticTestGroup,
    DiagnosticTestGroupTest,
    HealthParameter,
)
from tests.helpers.auth import employee_auth_header, seed_employee


async def _add_healthians_package(session, **fields) -> DiagnosticPackage:
    group = DiagnosticPackageGroup()
    session.add(group)
    await session.flush()
    defaults = {
        "package_name": "Compare Pkg",
        "diagnostic_provider": "healthians",
        "external_package_code": "11561",
        "status": "active",
        "package_for": "public",
    }
    defaults.update(fields)
    pkg = DiagnosticPackage(package_group_id=group.package_group_id, **defaults)
    session.add(pkg)
    await session.commit()
    await session.refresh(pkg)
    return pkg


async def _assign_tests(session, package: DiagnosticPackage, tests: list[HealthParameter], group_key: str) -> None:
    group = DiagnosticTestGroup(group_name=group_key, group_key=group_key)
    session.add(group)
    session.add_all(tests)
    await session.flush()
    session.add(
        DiagnosticPackageTestGroup(
            diagnostic_package_id=package.diagnostic_package_id,
            group_id=group.group_id,
        )
    )
    for index, test in enumerate(tests):
        session.add(
            DiagnosticTestGroupTest(
                group_id=group.group_id,
                test_id=test.test_id,
                display_order=index,
            )
        )
    await session.commit()


def _mock_healthians_product(constituents: list[dict], name: str = "Healthians Product") -> dict:
    return {"name": name, "constituents": constituents}


@pytest.fixture
def mock_healthians_api():
    with (
        patch(
            "modules.diagnostics.service.healthians_client.get_access_token",
            new_callable=AsyncMock,
            return_value="token",
        ),
        patch(
            "modules.diagnostics.service.healthians_client.get_product_details",
            new_callable=AsyncMock,
        ) as get_product_details,
    ):
        yield get_product_details


@pytest.mark.asyncio
async def test_provider_test_diff_in_sync(async_client, test_db_session, mock_healthians_api):
    await seed_employee(test_db_session, employee_id=1001, role="admin")
    pkg = await _add_healthians_package(test_db_session)
    await _assign_tests(
        test_db_session,
        pkg,
        [
            HealthParameter(test_name="Haemoglobin", parameter_type="test", healthians_parameter_key="135"),
            HealthParameter(test_name="Cholesterol", parameter_type="test", healthians_parameter_key="625"),
        ],
        "core",
    )
    mock_healthians_api.return_value = _mock_healthians_product(
        [
            {"id": "135", "name": "Haemoglobin"},
            {"id": "625", "name": "Cholesterol-Total, Serum"},
        ]
    )

    response = await async_client.get(
        f"/diagnostic-packages/{pkg.diagnostic_package_id}/provider-test-diff",
        headers=employee_auth_header(1001),
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["status"] == "in_sync"
    assert body["counts"]["matched"] == 2
    assert body["counts"]["only_in_package"] == 0
    assert body["counts"]["only_at_provider"] == 0
    assert body["counts"]["not_linked"] == 0
    assert body["healthians_package_name"] == "Healthians Product"
    assert body["external_package_code"] == "11561"


@pytest.mark.asyncio
async def test_provider_test_diff_all_buckets(async_client, test_db_session, mock_healthians_api):
    await seed_employee(test_db_session, employee_id=1002, role="admin")
    pkg = await _add_healthians_package(test_db_session)
    catalog_only = HealthParameter(
        test_name="Catalog Lipid",
        parameter_type="test",
        healthians_parameter_key="900",
    )
    shared_a = HealthParameter(test_name="Shared A", parameter_type="test", healthians_parameter_key="777")
    shared_b = HealthParameter(test_name="Shared B", parameter_type="test", healthians_parameter_key="777")
    await _assign_tests(
        test_db_session,
        pkg,
        [
            HealthParameter(test_name="Matched Test", parameter_type="test", healthians_parameter_key="100"),
            HealthParameter(test_name="Extra Local", parameter_type="test", healthians_parameter_key="200"),
            HealthParameter(test_name="Unlinked", parameter_type="test", healthians_parameter_key=None),
            shared_a,
            shared_b,
            catalog_only,
        ],
        "mix",
    )
    mock_healthians_api.return_value = _mock_healthians_product(
        [
            {"id": "100", "name": "Provider Matched"},
            {"id": "300", "name": "Provider Only Unknown"},
            {"id": "900", "name": "Provider Catalog"},
            {"id": "777", "name": "Shared Constituent"},
        ]
    )

    response = await async_client.get(
        f"/diagnostic-packages/{pkg.diagnostic_package_id}/provider-test-diff",
        headers=employee_auth_header(1002),
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["status"] == "out_of_sync"
    assert body["counts"]["matched"] == 4
    assert body["counts"]["only_in_package"] == 1
    assert body["counts"]["only_at_provider"] == 1
    assert body["counts"]["not_linked"] == 1

    matched_shared = [row for row in body["matched"] if row["provider_id"] == "777"]
    assert len(matched_shared) == 2
    assert all(row["shared_provider_id"] is True for row in matched_shared)

    only_local = body["only_in_package"][0]
    assert only_local["test_name"] == "Extra Local"
    assert only_local["provider_id"] == "200"

    only_provider = body["only_at_provider"][0]
    assert only_provider["provider_id"] == "300"
    assert only_provider["catalog_test_id"] is None
    assert only_provider["catalog_test_name"] is None

    assert body["not_linked"][0]["test_name"] == "Unlinked"


@pytest.mark.asyncio
async def test_provider_test_diff_catalog_only_at_provider(async_client, test_db_session, mock_healthians_api):
    await seed_employee(test_db_session, employee_id=1003, role="admin")
    pkg = await _add_healthians_package(test_db_session)
    catalog = HealthParameter(test_name="In Catalog", parameter_type="test", healthians_parameter_key="555")
    test_db_session.add(catalog)
    await test_db_session.commit()
    mock_healthians_api.return_value = _mock_healthians_product([{"id": "555", "name": "Provider Lipid"}])

    response = await async_client.get(
        f"/diagnostic-packages/{pkg.diagnostic_package_id}/provider-test-diff",
        headers=employee_auth_header(1003),
    )
    assert response.status_code == 200
    only_provider = response.json()["data"]["only_at_provider"][0]
    assert only_provider["catalog_test_id"] == catalog.test_id
    assert only_provider["catalog_test_name"] == "In Catalog"


@pytest.mark.asyncio
async def test_provider_test_diff_missing_external_code(async_client, test_db_session, mock_healthians_api):
    await seed_employee(test_db_session, employee_id=1004, role="admin")
    pkg = await _add_healthians_package(test_db_session, external_package_code=None)

    response = await async_client.get(
        f"/diagnostic-packages/{pkg.diagnostic_package_id}/provider-test-diff",
        headers=employee_auth_header(1004),
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "EXTERNAL_PACKAGE_CODE_REQUIRED"
    mock_healthians_api.assert_not_called()


@pytest.mark.asyncio
async def test_provider_test_diff_non_healthians_provider(async_client, test_db_session, mock_healthians_api):
    await seed_employee(test_db_session, employee_id=1005, role="admin")
    pkg = await _add_healthians_package(
        test_db_session,
        diagnostic_provider="orange_health",
        external_package_code="oh-1",
    )

    response = await async_client.get(
        f"/diagnostic-packages/{pkg.diagnostic_package_id}/provider-test-diff",
        headers=employee_auth_header(1005),
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "PROVIDER_NOT_SUPPORTED"
    mock_healthians_api.assert_not_called()


@pytest.mark.asyncio
async def test_test_group_provider_test_diff_preview(async_client, test_db_session, mock_healthians_api):
    await seed_employee(test_db_session, employee_id=1006, role="admin")
    group = DiagnosticTestGroup(
        group_name="Lipid Group",
        group_key="lipid_group_diff",
        diagnostic_provider=None,
    )
    test_db_session.add(group)
    test_db_session.add(
        HealthParameter(test_name="Cholesterol", parameter_type="test", healthians_parameter_key="625")
    )
    await test_db_session.flush()
    test = await test_db_session.scalar(
        select(HealthParameter).where(HealthParameter.test_name == "Cholesterol")
    )
    test_db_session.add(
        DiagnosticTestGroupTest(group_id=group.group_id, test_id=test.test_id, display_order=0)
    )
    await test_db_session.commit()
    await test_db_session.refresh(group)

    mock_healthians_api.return_value = _mock_healthians_product(
        [{"id": "625", "name": "Cholesterol-Total, Serum"}, {"id": "999", "name": "Extra"}],
        name="Lipid Profile Advance",
    )

    response = await async_client.get(
        f"/diagnostic-test-groups/{group.group_id}/provider-test-diff",
        params={"preview_external_test_group_code": "1"},
        headers=employee_auth_header(1006),
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["counts"]["matched"] == 1
    assert body["counts"]["only_at_provider"] == 1
    assert body["external_package_code"] == "1"
    mock_healthians_api.assert_called_once()
    assert mock_healthians_api.call_args.kwargs.get("deal_type") == "profile"
