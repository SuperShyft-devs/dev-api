"""Tests for WhatAPI integration sync logging helpers."""

from __future__ import annotations

import pytest

from modules.notifications.whatapi_sync_logging import compact_template_response


def test_compact_template_response_keeps_template_fields():
    body = {
        "data": [
            {
                "name": "otpmsg__",
                "language": "sk",
                "status": "APPROVED",
                "components": [{"type": "BODY", "text": "Hello {{1}}"}],
            }
        ]
    }
    out = compact_template_response(body)
    assert out["data"][0]["name"] == "otpmsg__"
    assert out["data"][0]["component_count"] == 1


@pytest.mark.asyncio
async def test_post_whatapi_webhook_writes_integration_sync_log(async_client, test_db_session):
    from sqlalchemy import text

    before = await test_db_session.execute(
        text(
            "SELECT COUNT(*) FROM integration_sync_logs "
            "WHERE api_endpoint_url = '/webhooks/whatapi'"
        )
    )
    count_before = before.scalar() or 0

    payload = {"message": {"queue_id": "no-such-queue", "message_status": "delivered"}}
    resp = await async_client.post("/webhooks/whatapi", json=payload)
    assert resp.status_code == 200

    after = await test_db_session.execute(
        text(
            "SELECT COUNT(*) FROM integration_sync_logs "
            "WHERE api_endpoint_url = '/webhooks/whatapi'"
        )
    )
    assert (after.scalar() or 0) > count_before

    row = await test_db_session.execute(
        text(
            "SELECT request_payload->>'api_name' FROM integration_sync_logs "
            "WHERE api_endpoint_url = '/webhooks/whatapi' "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    assert row.scalar() == "whatapi_inbound_webhook"
