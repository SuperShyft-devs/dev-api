"""Catalog HTTP tests."""

import pytest

from modules.notifications.bindings_service import NotificationBindingsService


def test_catalog_service_lists_35_events():
    service = NotificationBindingsService()
    events = service.list_catalog_events()
    assert len(events) == 35
