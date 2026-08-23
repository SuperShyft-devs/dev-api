"""Tests for slot_detail blood collection date matching."""

from __future__ import annotations

from datetime import date

from modules.engagements.slot_detail_dates import (
    blood_collection_dates_include,
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
