"""Tests for Metsights profile reuse helpers."""

from __future__ import annotations

import pytest

from modules.metsights.service import (
    MetsightsService,
    _phones_equivalent,
    _profile_name_matches,
)


def test_phones_equivalent_normalizes_country_code_and_dashes():
    assert _phones_equivalent("8762830757", "+91-8762830757")
    assert _phones_equivalent("+918762830757", "+91-8762830757")
    assert not _phones_equivalent("8762830757", "9167228151")


def test_profile_name_matches_allows_leading_initial():
    row = {"first_name": "S Pratheek", "last_name": "Bedre"}
    assert _profile_name_matches(row, first_name="Pratheek", last_name="Bedre")


@pytest.mark.asyncio
async def test_find_best_existing_profile_id_prefers_name_match(monkeypatch):
    listed = {
        "detail": "ok",
        "data": [
            {
                "id": "new-profile",
                "first_name": "Pratheek",
                "last_name": "Bedre",
                "phone": "+91-8762830757",
                "email": "pratheek.fitnastic@gmail.com",
                "created_at": "2026-04-28T00:00:00+05:30",
            },
            {
                "id": "original-profile",
                "first_name": "S Pratheek",
                "last_name": "Bedre",
                "phone": "+91-8762830757",
                "email": "pratheek.fitnastic@gmail.com",
                "created_at": "2025-12-05T00:00:00+05:30",
            },
        ],
    }

    async def _list_profiles(self, **kwargs):
        return listed

    from modules.metsights.client import MetsightsClient

    monkeypatch.setattr(MetsightsClient, "list_profiles", _list_profiles)
    svc = MetsightsService(client=MetsightsClient())

    profile_id = await svc._find_best_existing_profile_id(
        first_name="Pratheek",
        last_name="Bedre",
        phone="8762830757",
        email="pratheek.fitnastic@gmail.com",
    )
    assert profile_id == "original-profile"
