"""Tests for slot_detail blood collection date matching."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from modules.engagements.camp_overview import _summary_dates
from modules.engagements.enums import BloodCollectionType
from modules.engagements.slot_detail_dates import (
    blood_collection_dates_include,
    enabled_blood_collection_dates,
    engagement_collection_date_matches,
)


def test_blood_collection_dates_include_enabled_date():
    slot_detail = {
        "blood_collection": {
            "2026-06-02": {"is_enable": True, "cabins": []},
            "2026-06-03": {"is_enable": False, "cabins": []},
        }
    }
    assert blood_collection_dates_include(slot_detail, date(2026, 6, 2)) is True
    assert blood_collection_dates_include(slot_detail, date(2026, 6, 3)) is False


def test_engagement_collection_date_matches_start_date_fallback():
    assert engagement_collection_date_matches(
        start_date=date(2026, 6, 2),
        slot_detail=None,
        collection_date=date(2026, 6, 2),
    )
    assert not engagement_collection_date_matches(
        start_date=date(2026, 6, 3),
        slot_detail=None,
        collection_date=date(2026, 6, 2),
    )


def test_blood_collection_dates_exclude_disabled_section():
    slot_detail = {
        "blood_collection": {
            "is_enable": False,
            "2026-06-02": {"is_enable": True, "cabins": []},
        }
    }
    assert blood_collection_dates_include(slot_detail, date(2026, 6, 2)) is False


def test_engagement_collection_date_does_not_fallback_when_blood_dates_exist():
    slot_detail = {
        "blood_collection": {
            "2026-06-05": {"is_enable": True, "cabins": []},
        }
    }
    assert not engagement_collection_date_matches(
        start_date=date(2026, 6, 2),
        slot_detail=slot_detail,
        collection_date=date(2026, 6, 2),
    )


def test_engagement_collection_date_matches_participant_booking_when_no_slot_blood():
    assert engagement_collection_date_matches(
        start_date=date(2026, 10, 14),
        slot_detail=None,
        collection_date=date(2026, 10, 16),
        has_participant_booking_on_date=True,
    )
    assert not engagement_collection_date_matches(
        start_date=date(2026, 10, 14),
        slot_detail=None,
        collection_date=date(2026, 10, 16),
        has_participant_booking_on_date=False,
    )


def test_enabled_blood_collection_dates_sorted_and_skips_disabled():
    slot_detail = {
        "blood_collection": {
            "is_enable": True,
            "2026-06-07": {"is_enable": True, "cabins": []},
            "2026-06-05": {"is_enable": True, "cabins": []},
            "2026-06-03": {"is_enable": False, "cabins": []},
            "note": "ignored",
        }
    }
    assert enabled_blood_collection_dates(slot_detail) == [
        date(2026, 6, 5),
        date(2026, 6, 7),
    ]


def test_camp_collection_summary_uses_onsite_dates_not_window():
    engagement = SimpleNamespace(
        blood_collection_type=BloodCollectionType.camp_collection,
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 30),
    )
    slot_detail = {
        "blood_collection": {
            "2026-06-07": {"is_enable": True, "cabins": []},
            "2026-06-05": {"is_enable": True, "cabins": []},
        }
    }
    assert _summary_dates(engagement, slot_detail) == [date(2026, 6, 5), date(2026, 6, 7)]


def test_camp_collection_without_onsite_dates_has_no_days():
    engagement = SimpleNamespace(
        blood_collection_type="camp_collection",
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 30),
    )
    assert _summary_dates(engagement, None) == []


def test_home_collection_summary_uses_start_and_end():
    engagement = SimpleNamespace(
        blood_collection_type="home_collection",
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 3),
    )
    assert _summary_dates(engagement, None) == [
        date(2026, 6, 1),
        date(2026, 6, 2),
        date(2026, 6, 3),
    ]


def test_enabled_blood_collection_dates_empty_when_section_disabled():
    slot_detail = {
        "blood_collection": {
            "is_enable": False,
            "2026-06-02": {"is_enable": True, "cabins": []},
        }
    }
    assert enabled_blood_collection_dates(slot_detail) == []
    assert enabled_blood_collection_dates(None) == []


def test_engagement_collection_date_ignores_booking_flag_when_slot_blood_exists():
    slot_detail = {
        "blood_collection": {
            "2026-06-05": {"is_enable": True, "cabins": []},
        }
    }
    assert not engagement_collection_date_matches(
        start_date=date(2026, 6, 2),
        slot_detail=slot_detail,
        collection_date=date(2026, 6, 2),
        has_participant_booking_on_date=True,
    )
