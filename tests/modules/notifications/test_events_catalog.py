"""Tests for hardcoded notification events catalog."""

from modules.notifications.events_catalog import (
    EVENTS,
    LEGACY_EVENT_CODE_MAP,
    current_event_codes,
    get_event,
    list_events,
    map_legacy_event_code,
)


def test_events_catalog_has_32_events():
    assert len(EVENTS) == 32
    assert len(list_events()) == 32
    assert len(list_events(status="current")) == 21
    assert len(list_events(status="add")) == 11


def test_current_event_codes_count():
    assert len(current_event_codes()) == 21


def test_payments_completed_is_http_trigger():
    ev = get_event("payments.completed")
    assert ev is not None
    assert ev.trigger_kind == "http"
    assert ev.status == "current"


def test_legacy_map_covers_onboarding():
    assert map_legacy_event_code("onboarding") == "users.onboarded"
    ev = get_event("users.onboarded")
    assert ev is not None
    assert ev.legacy_event_code == "onboarding"


def test_legacy_map_keys_match_known_auto_events():
    for legacy in (
        "pretest_guidelines",
        "blood_report_ready",
        "consultation_remainder",
        "consultation_booking_alert",
    ):
        assert legacy in LEGACY_EVENT_CODE_MAP
        assert get_event(LEGACY_EVENT_CODE_MAP[legacy]) is not None
    assert map_legacy_event_code("consultation_ready") == "reminders.consultation_ready_to_book"
    assert map_legacy_event_code("consultation_booking_alert") == "consultations.booked"
