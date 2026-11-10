"""Tests for GET/POST /webhooks/whatapi."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from core.config import settings
from modules.notifications.models import Notification


async def _seed_service(test_db_session, *, service_key: str) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'whatsapp', 'test-webhook', true, false, false, false) "
            "ON CONFLICT (service_key) DO NOTHING"
        ),
        {"sk": service_key, "dn": service_key},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_whatapi_get_challenge_echoes_plain_text(async_client, monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_WEBHOOK_VERIFY_TOKEN", "verify-me")
    resp = await async_client.get(
        "/webhooks/whatapi",
        params={
            "hub.mode": "subscribe",
            "hub.challenge": "TEST123",
            "hub.verify_token": "verify-me",
        },
    )
    assert resp.status_code == 200
    assert resp.text == "TEST123"
    assert "text/plain" in resp.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_whatapi_get_rejects_bad_verify_token(async_client, monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_WEBHOOK_VERIFY_TOKEN", "verify-me")
    resp = await async_client.get(
        "/webhooks/whatapi",
        params={
            "hub.challenge": "TEST123",
            "hub.verify_token": "wrong",
        },
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_whatapi_get_challange_typo_param(async_client, monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_WEBHOOK_VERIFY_TOKEN", "")
    resp = await async_client.get(
        "/webhooks/whatapi",
        params={"challange": "TYPO-CHAL", "echo": "true"},
    )
    assert resp.status_code == 200
    assert resp.text == "TYPO-CHAL"


@pytest.mark.asyncio
async def test_whatapi_post_updates_notification_by_queue_id(
    async_client, test_db_session, monkeypatch
):
    service_key = "whatapi-wh-test"
    await _seed_service(test_db_session, service_key=service_key)
    queue_id = "887c0d94-e0bd-49fe-98b3-955e81f9ed47"
    notification = Notification(
        service_key=service_key,
        status="pending",
        channel="whatsapp",
        user={"contacts": ["919362830757"]},
        message="queued",
        queue_id=queue_id,
        dispatched_at=datetime.now(timezone.utc),
    )
    test_db_session.add(notification)
    await test_db_session.commit()
    await test_db_session.refresh(notification)

    payload = {
        "message": {
            "queue_id": queue_id,
            "message_status": "delivered",
        }
    }
    resp = await async_client.post("/webhooks/whatapi", json=payload)
    assert resp.status_code == 200

    await test_db_session.refresh(notification)
    assert notification.status == "sent"
    assert notification.completed_at is not None
    assert notification.message == "WhatsApp delivered (delivered)"


@pytest.mark.asyncio
async def test_whatapi_post_updates_message_on_failed(
    async_client, test_db_session, monkeypatch
):
    service_key = "whatapi-wh-fail"
    await _seed_service(test_db_session, service_key=service_key)
    queue_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    notification = Notification(
        service_key=service_key,
        status="pending",
        channel="whatsapp",
        user={"contacts": ["919876543210"]},
        message="WhatsApp template queued via WhatAPI POST",
        queue_id=queue_id,
        dispatched_at=datetime.now(timezone.utc),
    )
    test_db_session.add(notification)
    await test_db_session.commit()
    await test_db_session.refresh(notification)

    payload = {
        "message": {
            "queue_id": queue_id,
            "message_status": "failed",
            "errors": [{"title": "Message undeliverable"}],
        }
    }
    resp = await async_client.post("/webhooks/whatapi", json=payload)
    assert resp.status_code == 200

    await test_db_session.refresh(notification)
    assert notification.status == "failed"
    assert "undeliverable" in (notification.message or "").lower()


@pytest.mark.asyncio
async def test_whatapi_post_meta_envelope_with_queue_and_statuses(
    async_client, test_db_session,
):
    service_key = "whatapi-wh-meta"
    await _seed_service(test_db_session, service_key=service_key)
    queue_id = "b2c3d4e5-f6a7-8901-bcde-f12345678901"
    notification = Notification(
        service_key=service_key,
        status="pending",
        channel="whatsapp",
        user={"contacts": ["919876543210"]},
        message="queued",
        queue_id=queue_id,
        dispatched_at=datetime.now(timezone.utc),
    )
    test_db_session.add(notification)
    await test_db_session.commit()
    await test_db_session.refresh(notification)

    payload = {
        "queue_id": queue_id,
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "statuses": [
                                {
                                    "status": "delivered",
                                    "id": "wamid.IGNORED",
                                }
                            ]
                        }
                    }
                ]
            }
        ],
    }
    resp = await async_client.post("/webhooks/whatapi", json=payload)
    assert resp.status_code == 200

    await test_db_session.refresh(notification)
    assert notification.status == "sent"
