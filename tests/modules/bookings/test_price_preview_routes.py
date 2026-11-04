"""Tests for POST /book/price-preview."""

from __future__ import annotations

import pytest

from modules.discounts.models import DiscountCode
from modules.users.models import User
from tests.modules.bookings.test_bookings_routes import (
    _auth_header,
    _seed_healthians_diagnostic_package,
)


async def _seed_user(test_db_session, user_id: int) -> None:
    test_db_session.add(
        User(
            user_id=user_id,
            age=30,
            phone=f"{user_id:010d}"[-10:],
            status="active",
            first_name="Preview",
            last_name="User",
            gender="male",
            city="Mumbai",
            parent_id=None,
        )
    )
    await test_db_session.commit()


async def _seed_fixed_discount(test_db_session, *, code: str = "COMPTEST", fixed_paise: int = 199800) -> None:
    test_db_session.add(
        DiscountCode(
            code=code,
            name="Complementary test",
            status="active",
            discount_type="fixed",
            fixed_amount_paise=fixed_paise,
            scope_mode="general",
            package_apply_mode="all",
        )
    )
    await test_db_session.commit()


def _preview_items(*user_ids: int, package_id: int = 1, amount_paise: int = 199900):
    return [
        {
            "user_id": uid,
            "entity_type": "diagnostic_package",
            "entity_id": package_id,
            "amount_paise": amount_paise,
        }
        for uid in user_ids
    ]


@pytest.mark.asyncio
async def test_price_preview_requires_auth(async_client):
    response = await async_client.post(
        "/book/price-preview",
        json={"items": _preview_items(1)},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_price_preview_without_code(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    await _seed_user(test_db_session, 960001)

    response = await async_client.post(
        "/book/price-preview",
        headers=_auth_header(960001),
        json={"items": _preview_items(960001, 960002)},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["ok"] is True
    assert body["code"] is None
    assert body["discount_paise"] == 0
    assert body["original_paise"] == 399800
    assert body["final_paise"] == 399800
    breakdown = body["price_breakdown"]
    assert breakdown["promo"]["applied"] is False
    assert len(breakdown["lines"]) == 2
    assert all(line["discount_paise"] == 0 for line in breakdown["lines"])
    assert all(line["payable_paise"] == 199900 for line in breakdown["lines"])


@pytest.mark.asyncio
async def test_price_preview_with_fixed_code_multi_member(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    await _seed_user(test_db_session, 960010)
    await _seed_fixed_discount(test_db_session)

    response = await async_client.post(
        "/book/price-preview",
        headers=_auth_header(960010),
        json={
            "code": "COMPTEST",
            "items": _preview_items(960010, 960011),
        },
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["ok"] is True
    assert body["code"] == "COMPTEST"
    assert body["original_paise"] == 399800
    assert body["discount_paise"] == 199800
    assert body["final_paise"] == 200000
    lines = body["price_breakdown"]["lines"]
    assert len(lines) == 2
    assert sum(line["discount_paise"] for line in lines) == 199800
    assert sum(line["payable_paise"] for line in lines) == 200000


@pytest.mark.asyncio
async def test_price_preview_invalid_code_still_returns_breakdown(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    await _seed_user(test_db_session, 960020)

    response = await async_client.post(
        "/book/price-preview",
        headers=_auth_header(960020),
        json={
            "code": "NOTREAL99",
            "items": _preview_items(960020),
        },
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["ok"] is False
    assert body.get("message")
    assert body["discount_paise"] == 0
    assert body["price_breakdown"]["totals"]["payable_paise"] == 199900


@pytest.mark.asyncio
async def test_discounts_validate_still_requires_code(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    await _seed_user(test_db_session, 960030)

    response = await async_client.post(
        "/discounts/validate",
        headers=_auth_header(960030),
        json={"items": _preview_items(960030)},
    )
    assert response.status_code in (400, 422)
