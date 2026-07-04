"""Unit tests for Nominatim mapping and location enrichment."""

from __future__ import annotations

import pytest

from modules.geocoding.client import (
    enrich_location_fields,
    location_fields_complete,
    map_nominatim_result,
    merge_location_fields,
)


def test_map_nominatim_result_maps_expected_fields():
    mapped = map_nominatim_result(
        {
            "name": "Marol Naka (Line 1)",
            "display_name": "Marol Naka (Line 1), Mumbai, Maharashtra, 400072, India",
            "lat": "19.1083663",
            "lon": "72.8788727",
            "address": {
                "suburb": "Saki Naka",
                "neighbourhood": "Chimatpada",
                "city": "Mumbai",
                "state": "Maharashtra",
                "postcode": "400072",
                "country": "India",
            },
        }
    )
    assert mapped["landmark"] == "Marol Naka (Line 1)"
    assert mapped["sub_locality"] == "Saki Naka"
    assert mapped["city"] == "Mumbai"
    assert mapped["pincode"] == "400072"
    assert mapped["state"] == "Maharashtra"
    assert mapped["country"] == "India"
    assert mapped["latitude"] == pytest.approx(19.1083663)
    assert mapped["longitude"] == pytest.approx(72.8788727)


def test_location_fields_complete_requires_all_fields():
    assert not location_fields_complete(address="A", city="Mumbai")
    assert location_fields_complete(
        address="A",
        sub_locality="S",
        landmark="L",
        city="Mumbai",
        pincode="400072",
        state="MH",
        country="India",
        latitude=19.1,
        longitude=72.8,
    )


def test_merge_location_fields_does_not_overwrite_existing():
    merged = merge_location_fields(
        {"address": "User Address", "city": None, "latitude": None},
        {"address": "Display Name", "city": "Mumbai", "latitude": 19.1},
    )
    assert merged["address"] == "User Address"
    assert merged["city"] == "Mumbai"
    assert merged["latitude"] == 19.1


@pytest.mark.asyncio
async def test_enrich_location_fields_skips_geocode_when_complete(monkeypatch):
    async def _should_not_run(*_args, **_kwargs):
        raise AssertionError("search_places should not be called")

    monkeypatch.setattr("modules.geocoding.client.search_places", _should_not_run)
    result = await enrich_location_fields(
        address="A",
        sub_locality="S",
        landmark="L",
        city="Mumbai",
        pincode="400072",
        state="MH",
        country="India",
        latitude=19.1,
        longitude=72.8,
    )
    assert result["city"] == "Mumbai"
    assert result["latitude"] == pytest.approx(19.1)


@pytest.mark.asyncio
async def test_enrich_location_fields_fills_missing_from_geocode(monkeypatch):
    async def _fake_search(query: str, *, limit: int = 3):
        assert "Marol" in query
        assert limit == 1
        return [
            {
                "display_name": "Marol Naka, Mumbai",
                "address": "Marol Naka, Mumbai",
                "sub_locality": "Saki Naka",
                "landmark": "Marol Naka",
                "city": "Mumbai",
                "pincode": "400072",
                "state": "Maharashtra",
                "country": "India",
                "latitude": 19.1,
                "longitude": 72.8,
            }
        ]

    monkeypatch.setattr("modules.geocoding.client.search_places", _fake_search)
    result = await enrich_location_fields(address="Marol Naka", city="Mumbai")
    assert result["address"] == "Marol Naka"
    assert result["sub_locality"] == "Saki Naka"
    assert result["landmark"] == "Marol Naka"
    assert result["pincode"] == "400072"
    assert result["latitude"] == pytest.approx(19.1)
