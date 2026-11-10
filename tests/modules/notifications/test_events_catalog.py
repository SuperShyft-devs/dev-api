"""Tests for hardcoded notification events catalog."""

from modules.notifications.events_catalog import (
    EVENTS,
    LEGACY_EVENT_CODE_MAP,
    current_event_codes,
    get_event,
    list_events,
    map_legacy_event_code,
)


def test_events_catalog_has_35_events():
    assert len(EVENTS) == 35
    assert len(list_events()) == 35
    assert len(list_events(status="current")) == 21
    assert len(list_events(status="add")) == 14


def test_current_event_codes_count():
    assert len(current_event_codes()) == 21


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
    ):
        assert legacy in LEGACY_EVENT_CODE_MAP
        assert get_event(LEGACY_EVENT_CODE_MAP[legacy]) is not None
