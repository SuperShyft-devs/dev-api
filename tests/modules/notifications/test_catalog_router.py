"""Catalog HTTP tests."""

import pytest

from modules.notifications.bindings_service import NotificationBindingsService


def test_catalog_service_lists_32_events():
    service = NotificationBindingsService()
    events = service.list_catalog_events()
    assert len(events) == 32
    assert events[0].recipient_payload_hints.get("user")


def test_catalog_includes_recipient_payload_hints():
    service = NotificationBindingsService()
    events = service.list_catalog_events()
    registered = next(e for e in events if e.code == "users.registered")
    assert "user_id" in registered.recipient_payload_hints.get("user", "")


def test_catalog_http_event_includes_payload_contact_fields():
    service = NotificationBindingsService()
    events = service.list_catalog_events()
    payments = next(e for e in events if e.code == "payments.completed")
    assert payments.allows_payload_recipient is True
    paths = {f.path for f in payments.payload_contact_fields}
    assert "booker.phone" in paths
