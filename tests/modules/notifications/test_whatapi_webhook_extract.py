"""Unit tests for WhatAPI webhook payload extraction."""

from __future__ import annotations

from modules.notifications.whatapi_webhook_handler import _walk_for_queue_status


def test_walk_finds_queue_id_and_message_status():
    payload = {
        "message": {
            "queue_id": "q-1",
            "message_status": "failed",
            "errors": [{"title": "Template mismatch"}],
        }
    }
    pairs = _walk_for_queue_status(payload)
    assert ("q-1", "failed", "Template mismatch") in pairs


def test_walk_meta_statuses_with_ancestor_queue_id():
    payload = {
        "queue_id": "q-2",
        "entry": [{"changes": [{"value": {"statuses": [{"status": "delivered"}]}}]}],
    }
    pairs = _walk_for_queue_status(payload)
    assert any(q == "q-2" and status == "delivered" for q, status, _ in pairs)


def test_walk_ignores_wamid_only_statuses():
    payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "statuses": [{"id": "wamid.xxx", "status": "failed"}],
                        }
                    }
                ]
            }
        ]
    }
    assert _walk_for_queue_status(payload) == []
