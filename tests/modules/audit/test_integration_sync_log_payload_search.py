"""Tests for partial JSON request-payload search on integration sync logs."""

from __future__ import annotations

import pytest

from modules.audit.models import IntegrationSyncLog
from modules.audit.payload_search import PayloadSearchCriteria, parse_payload_search
from modules.audit.repository import AuditRepository


@pytest.mark.parametrize(
    ("raw", "contains", "keys"),
    [
        (None, None, ()),
        ("", None, ()),
        ('{"albumin": 4.62}', {"albumin": 4.62}, ()),
        ('{ "albumin": 4.62, "creatinine": 0.81 }', {"albumin": 4.62, "creatinine": 0.81}, ()),
        ('{ "albumin":', None, ("albumin",)),
        ('{ "albumin": ', None, ("albumin",)),
        ('{ "albumin": 4.62, "creatinine":', {"albumin": 4.62}, ("creatinine",)),
    ],
)
def test_parse_payload_search(raw: str | None, contains: dict | None, keys: tuple[str, ...]) -> None:
    criteria = parse_payload_search(raw)
    if contains is None and not keys:
        assert criteria is None
        return
    assert criteria is not None
    assert criteria.contains == contains
    assert criteria.keys == keys


@pytest.mark.asyncio
async def test_list_sync_logs_payload_search_matches_request_payload_values(test_db_session) -> None:
    repo = AuditRepository()
    test_db_session.add(
        IntegrationSyncLog(
            provider="metsights",
            api_endpoint_url="/records/B79E29CBE0A3/blood-parameters/",
            request_payload={"albumin": 4.62, "creatinine": 0.81, "albumin_unit": "0"},
            response_payload={"pushed": True},
            status="success",
        )
    )
    test_db_session.add(
        IntegrationSyncLog(
            provider="metsights",
            api_endpoint_url="/records/OTHER123/blood-parameters/",
            request_payload={"albumin": 3.1, "creatinine": 1.2},
            response_payload={"pushed": True},
            status="success",
        )
    )
    await test_db_session.commit()

    rows = await repo.list_sync_logs(
        test_db_session,
        page=1,
        limit=25,
        provider="metsights",
        payload_search=PayloadSearchCriteria(contains={"albumin": 4.62}, keys=()),
    )
    assert len(rows) == 1
    assert "B79E29CBE0A3" in rows[0].api_endpoint_url


@pytest.mark.asyncio
async def test_list_sync_logs_payload_search_ignores_response_payload(test_db_session) -> None:
    repo = AuditRepository()
    test_db_session.add(
        IntegrationSyncLog(
            provider="metsights",
            api_endpoint_url="/records/GETONLY/blood-parameters/",
            request_payload={"record_id": "GETONLY"},
            response_payload={"albumin": 4.62, "calcium": 9.81},
            status="success",
        )
    )
    test_db_session.add(
        IntegrationSyncLog(
            provider="metsights",
            api_endpoint_url="/records/PUSH01/blood-parameters/",
            request_payload={"albumin": 4.62, "albumin_unit": "0"},
            response_payload={"pushed": True},
            status="success",
        )
    )
    await test_db_session.commit()

    rows = await repo.list_sync_logs(
        test_db_session,
        page=1,
        limit=25,
        payload_search=PayloadSearchCriteria(contains={"albumin": 4.62}, keys=()),
    )
    assert len(rows) == 1
    assert "PUSH01" in rows[0].api_endpoint_url


@pytest.mark.asyncio
async def test_list_sync_logs_payload_search_key_only(test_db_session) -> None:
    repo = AuditRepository()
    test_db_session.add(
        IntegrationSyncLog(
            provider="metsights",
            api_endpoint_url="/records/KEY01/blood-parameters/",
            request_payload={"albumin": 4.62, "creatinine": 0.81},
            status="success",
        )
    )
    test_db_session.add(
        IntegrationSyncLog(
            provider="metsights",
            api_endpoint_url="/records/KEY02/blood-parameters/",
            request_payload={"creatinine": 0.81},
            status="success",
        )
    )
    await test_db_session.commit()

    criteria = parse_payload_search('{ "albumin":')
    assert criteria is not None

    rows = await repo.list_sync_logs(
        test_db_session,
        page=1,
        limit=25,
        payload_search=criteria,
    )
    assert len(rows) == 1
    assert "KEY01" in rows[0].api_endpoint_url


@pytest.mark.asyncio
async def test_list_sync_logs_payload_search_combined_with_endpoint_search(test_db_session) -> None:
    repo = AuditRepository()
    test_db_session.add(
        IntegrationSyncLog(
            provider="metsights",
            api_endpoint_url="/records/B79E29CBE0A3/blood-parameters/",
            request_payload={"albumin": 4.62},
            status="success",
        )
    )
    test_db_session.add(
        IntegrationSyncLog(
            provider="metsights",
            api_endpoint_url="/records/B79E29CBE0A3/vitals/",
            request_payload={"albumin": 4.62},
            status="success",
        )
    )
    await test_db_session.commit()

    rows = await repo.list_sync_logs(
        test_db_session,
        page=1,
        limit=25,
        search="blood-parameters",
        payload_search=PayloadSearchCriteria(contains={"albumin": 4.62}, keys=()),
    )
    assert len(rows) == 1
    assert rows[0].api_endpoint_url.endswith("/blood-parameters/")
