"""Catalog HTTP tests."""

import pytest

from modules.notifications.bindings_service import NotificationBindingsService


def test_catalog_service_lists_32_events():
    service = NotificationBindingsService()
    events = service.list_catalog_events()
    assert len(events) == 32
    assert events[0].recipient_payload_hints.get("user")


def test_catalog_api_events_include_payload_fields_for_otp():
    service = NotificationBindingsService()
    events = service.list_catalog_events()
    otp = next(e for e in events if e.code == "auth.otp_sent")
    assert otp.allows_payload_recipient is True
    assert otp.trigger_source == "api"
    assert any(f.id == "phone" for f in otp.payload_contact_fields)


def test_catalog_cron_event_disallows_payload_recipient():
    service = NotificationBindingsService()
    events = service.list_catalog_events()
    pretest = next(e for e in events if e.code == "reminders.pretest")
    assert pretest.trigger_source == "cron"
    assert pretest.allows_payload_recipient is False
    assert pretest.payload_contact_fields == []


def test_catalog_includes_recipient_payload_hints():
    service = NotificationBindingsService()
    events = service.list_catalog_events()
    registered = next(e for e in events if e.code == "users.registered")
    assert "user_id" in registered.recipient_payload_hints.get("user", "")
