"""Tests for POST /webhooks/healthians."""

from __future__ import annotations

from datetime import date, time

import pytest
from sqlalchemy import text

from modules.engagements.models import Engagement, EngagementParticipant
from modules.users.models import User


def _sample_payload(
    *,
    booking_id: str = "1387716654555",
    event_type: str = "status_updated",
    ref_booking_id: str | None = None,
) -> dict:
    data: dict = {
        "booking_status": "BS005",
        "customer_status": "BS005",
    }
    if ref_booking_id is not None:
        data["ref_booking_id"] = ref_booking_id
    return {
        "type": event_type,
        "booking_id": booking_id,
        "data": data,
    }


async def _seed_diagnostic_package(test_db_session, *, diagnostic_package_id: int = 1):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, :ref, :pname, 'Healthians', 'active', 0) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        ),
        {"did": diagnostic_package_id, "ref": f"REF{diagnostic_package_id}", "pname": "Diag"},
    )
    await test_db_session.commit()


async def _seed_engagement(test_db_session, *, engagement_id: int, engagement_code: str):
    await _seed_diagnostic_package(test_db_session)
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp",
            engagement_code=engagement_code,
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 1),
            status="active",
        )
    )
    await test_db_session.commit()


async def _seed_user(test_db_session, *, user_id: int):
    test_db_session.add(
        User(
            user_id=user_id,
            age=30,
            phone=f"{user_id}000000000",
            status="active",
        )
    )
    await test_db_session.flush()


async def _seed_participant(
    test_db_session,
    *,
    engagement_participant_id: int,
    engagement_id: int,
    user_id: int,
    booking_id: str | None = None,
):
    await _seed_user(test_db_session, user_id=user_id)
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=engagement_participant_id,
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date(2026, 5, 1),
            slot_start_time=time(10, 0),
            booking_id=booking_id,
        )
    )
    await test_db_session.commit()


def _fake_httpx_client(*, succeed: bool = True):
    class _FakeResponse:
        status_code = 200
        text = "ok"

        def raise_for_status(self):
            if not succeed:
                raise RuntimeError("webhook failed")

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            return _FakeResponse()

    return _FakeClient


@pytest.mark.asyncio
async def test_receive_creates_inbound_sync_log(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(
        "modules.webhooks.sender.service.settings.HEALTHIANS_WEBHOOK_FORWARD_URL",
        "",
    )

    payload = _sample_payload()
    response = await async_client.post("/webhooks/healthians", json=payload)
    assert response.status_code == 200, response.text

    body = response.json()["data"]
    assert body["received"] is True
    assert body["sync_log_id"] is not None
    assert body["forwards"] == []

    result = await test_db_session.execute(
        text(
            "SELECT provider, engagement_id, user_id, api_endpoint_url, request_payload, "
            "response_payload, status, error_message "
            "FROM integration_sync_logs WHERE sync_log_id = :sync_log_id"
        ),
        {"sync_log_id": body["sync_log_id"]},
    )
    row = result.mappings().one()
    assert row["provider"] == "healthians"
    assert row["engagement_id"] is None
    assert row["user_id"] is None
    assert row["api_endpoint_url"] == "/webhooks/healthians"
    assert row["request_payload"]["booking_id"] == payload["booking_id"]
    assert row["status"] == "success"
    assert row["response_payload"]["received"] is True
    assert row["error_message"] is None


@pytest.mark.asyncio
async def test_receive_resolves_engagement_and_user(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(
        "modules.webhooks.sender.service.settings.HEALTHIANS_WEBHOOK_FORWARD_URL",
        "",
    )

    await _seed_engagement(test_db_session, engagement_id=9701, engagement_code="ENG9701")
    await _seed_participant(
        test_db_session,
        engagement_participant_id=97001,
        engagement_id=9701,
        user_id=9701,
        booking_id="1387716654555",
    )

    response = await async_client.post(
        "/webhooks/healthians",
        json=_sample_payload(booking_id="1387716654555"),
    )
    assert response.status_code == 200, response.text
    sync_log_id = response.json()["data"]["sync_log_id"]

    result = await test_db_session.execute(
        text(
            "SELECT engagement_id, user_id "
            "FROM integration_sync_logs WHERE sync_log_id = :sync_log_id"
        ),
        {"sync_log_id": sync_log_id},
    )
    row = result.mappings().one()
    assert row["engagement_id"] == 9701
    assert row["user_id"] == 9701


@pytest.mark.asyncio
async def test_receive_unknown_booking_id(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(
        "modules.webhooks.sender.service.settings.HEALTHIANS_WEBHOOK_FORWARD_URL",
        "",
    )

    response = await async_client.post(
        "/webhooks/healthians",
        json=_sample_payload(booking_id="unknown-booking-id"),
    )
    assert response.status_code == 200, response.text

    sync_log_id = response.json()["data"]["sync_log_id"]
    result = await test_db_session.execute(
        text(
            "SELECT engagement_id, user_id, status "
            "FROM integration_sync_logs WHERE sync_log_id = :sync_log_id"
        ),
        {"sync_log_id": sync_log_id},
    )
    row = result.mappings().one()
    assert row["engagement_id"] is None
    assert row["user_id"] is None
    assert row["status"] == "success"


@pytest.mark.asyncio
async def test_ref_booking_id_fallback(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(
        "modules.webhooks.sender.service.settings.HEALTHIANS_WEBHOOK_FORWARD_URL",
        "",
    )

    await _seed_engagement(test_db_session, engagement_id=9702, engagement_code="ENG9702")
    await _seed_participant(
        test_db_session,
        engagement_participant_id=97002,
        engagement_id=9702,
        user_id=9702,
        booking_id="22494618",
    )

    response = await async_client.post(
        "/webhooks/healthians",
        json=_sample_payload(booking_id="224949781", ref_booking_id="22494618"),
    )
    assert response.status_code == 200, response.text
    sync_log_id = response.json()["data"]["sync_log_id"]

    result = await test_db_session.execute(
        text(
            "SELECT engagement_id, user_id "
            "FROM integration_sync_logs WHERE sync_log_id = :sync_log_id"
        ),
        {"sync_log_id": sync_log_id},
    )
    row = result.mappings().one()
    assert row["engagement_id"] == 9702
    assert row["user_id"] == 9702


@pytest.mark.asyncio
async def test_forward_creates_outbound_sync_logs(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(
        "modules.webhooks.sender.service.settings.HEALTHIANS_WEBHOOK_FORWARD_URL",
        "https://forward-one.test/hook,https://forward-two.test/hook",
    )
    monkeypatch.setattr(
        "modules.webhooks.sender.service.httpx.AsyncClient",
        _fake_httpx_client(succeed=True),
    )

    payload = _sample_payload(booking_id="forward-booking-1")
    response = await async_client.post("/webhooks/healthians", json=payload)
    assert response.status_code == 200, response.text

    data = response.json()["data"]
    assert len(data["forwards"]) == 2
    assert all(item["status"] == "success" for item in data["forwards"])

    result = await test_db_session.execute(
        text(
            "SELECT api_endpoint_url, request_payload, response_payload, status "
            "FROM integration_sync_logs "
            "WHERE provider = 'healthians' "
            "AND api_endpoint_url LIKE 'https://forward-%' "
            "ORDER BY sync_log_id ASC"
        )
    )
    rows = result.mappings().all()
    assert len(rows) == 2
    assert rows[0]["api_endpoint_url"] == "https://forward-one.test/hook"
    assert rows[1]["api_endpoint_url"] == "https://forward-two.test/hook"
    assert rows[0]["request_payload"]["booking_id"] == payload["booking_id"]
    assert rows[0]["status"] == "success"
    assert rows[0]["response_payload"] == {"message": "ok"}


@pytest.mark.asyncio
async def test_forward_failure_logged(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(
        "modules.webhooks.sender.service.settings.HEALTHIANS_WEBHOOK_FORWARD_URL",
        "https://forward-fail.test/hook",
    )
    monkeypatch.setattr(
        "modules.webhooks.sender.service.httpx.AsyncClient",
        _fake_httpx_client(succeed=False),
    )

    response = await async_client.post(
        "/webhooks/healthians",
        json=_sample_payload(booking_id="forward-fail-booking"),
    )
    assert response.status_code == 200, response.text

    data = response.json()["data"]
    assert data["received"] is True
    assert data["forwards"][0]["status"] == "failed"
    assert "webhook failed" in data["forwards"][0]["error"]

    inbound = await test_db_session.execute(
        text(
            "SELECT status FROM integration_sync_logs "
            "WHERE api_endpoint_url = '/webhooks/healthians' "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    assert inbound.mappings().one()["status"] == "success"

    outbound = await test_db_session.execute(
        text(
            "SELECT status, error_message, response_payload "
            "FROM integration_sync_logs "
            "WHERE api_endpoint_url = 'https://forward-fail.test/hook' "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = outbound.mappings().one()
    assert row["status"] == "failed"
    assert "webhook failed" in row["error_message"]
    assert row["response_payload"] is None
