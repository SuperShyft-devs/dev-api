"""Tests for payment verify/failed authorization and fulfillment behavior."""

from __future__ import annotations

import hashlib
import hmac
from datetime import timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


def _mock_razorpay_order(*, amount_paise: int, receipt: str):
    return {"id": f"order_test_{receipt}", "amount": amount_paise, "currency": "INR"}


def _razorpay_signature(*, order_id: str, payment_id: str) -> str:
    message = f"{order_id}|{payment_id}"
    return hmac.new(
        key=settings.RAZORPAY_KEY_SECRET.encode(),
        msg=message.encode(),
        digestmod=hashlib.sha256,
    ).hexdigest()


async def _seed_diag_package(test_db_session) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, "
            "diagnostic_provider, status, price) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active', 500) "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status, "
            "price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_payment_failed_forbidden_for_unrelated_user(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    await _seed_diag_package(test_db_session)

    owner = User(
        user_id=930001,
        age=30,
        phone="9300010000",
        status="active",
        first_name="Own",
        last_name="Er",
        gender="male",
        city="City",
        parent_id=None,
    )
    other = User(
        user_id=930002,
        age=30,
        phone="9300020000",
        status="active",
        first_name="Oth",
        last_name="Er",
        gender="female",
        city="City",
        parent_id=None,
    )
    test_db_session.add_all([owner, other])
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 930001,
                "address": "Addr",
                "pincode": "111111",
                "city": "City",
                "blood_collection_date": "2026-09-01",
                "blood_collection_time_slot": "10:00",
                "diagnostic_package_id": 1,
            }
        ]
    }

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        book_resp = await async_client.post("/book/bio-ai", headers=_auth_header(930001), json=payload)

    assert book_resp.status_code == 200
    razorpay_order_id = book_resp.json()["data"]["razorpay_order_id"]

    failed_resp = await async_client.post(
        "/payments/failed",
        headers=_auth_header(930002),
        json={"razorpay_order_id": razorpay_order_id, "failure_reason": "test"},
    )
    assert failed_resp.status_code == 403

    order_status = (
        await test_db_session.execute(
            text("SELECT status FROM orders WHERE razorpay_order_id = :oid"),
            {"oid": razorpay_order_id},
        )
    ).scalar_one()
    assert order_status == "created"


@pytest.mark.asyncio
async def test_verify_payment_forbidden_for_unrelated_user(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    await _seed_diag_package(test_db_session)

    owner = User(
        user_id=930010,
        age=30,
        phone="9300100000",
        status="active",
        first_name="Own",
        last_name="Er",
        gender="male",
        city="City",
        parent_id=None,
    )
    other = User(
        user_id=930011,
        age=30,
        phone="9300110000",
        status="active",
        first_name="Oth",
        last_name="Er",
        gender="female",
        city="City",
        parent_id=None,
    )
    test_db_session.add_all([owner, other])
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 930010,
                "address": "Addr",
                "pincode": "111111",
                "city": "City",
                "blood_collection_date": "2026-09-01",
                "blood_collection_time_slot": "10:00",
                "diagnostic_package_id": 1,
            }
        ]
    }

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        book_resp = await async_client.post("/book/bio-ai", headers=_auth_header(930010), json=payload)

    razorpay_order_id = book_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_930010"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)

    verify_resp = await async_client.post(
        "/payments/verify",
        headers=_auth_header(930011),
        json={
            "razorpay_order_id": razorpay_order_id,
            "razorpay_payment_id": payment_id,
            "razorpay_signature": signature,
        },
    )
    assert verify_resp.status_code == 403


@pytest.mark.asyncio
async def test_verify_payment_rolls_back_when_fulfillment_fails(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")
    await _seed_diag_package(test_db_session)

    user = User(
        user_id=930020,
        age=30,
        phone="9300200000",
        status="active",
        first_name="Pay",
        last_name="Er",
        gender="male",
        city="City",
        parent_id=None,
    )
    test_db_session.add(user)
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 930020,
                "address": "Addr",
                "pincode": "111111",
                "city": "City",
                "blood_collection_date": "2026-09-01",
                "blood_collection_time_slot": "10:00",
                "diagnostic_package_id": 1,
            }
        ]
    }

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        book_resp = await async_client.post("/book/bio-ai", headers=_auth_header(930020), json=payload)

    booking_id = book_resp.json()["data"]["booking_id"]
    razorpay_order_id = book_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_930020"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)

    with patch(
        "modules.users.service.UsersService.fulfill_bio_ai_booking",
        side_effect=RuntimeError("fulfillment boom"),
    ):
        verify_resp = await async_client.post(
            "/payments/verify",
            headers=_auth_header(930020),
            json={
                "razorpay_order_id": razorpay_order_id,
                "razorpay_payment_id": payment_id,
                "razorpay_signature": signature,
            },
        )

    assert verify_resp.status_code == 500

    booking_status = (
        await test_db_session.execute(
            text("SELECT status FROM bookings WHERE booking_id = :bid"),
            {"bid": booking_id},
        )
    ).scalar_one()
    assert booking_status == "pending"

    order_status = (
        await test_db_session.execute(
            text("SELECT status FROM orders WHERE razorpay_order_id = :oid"),
            {"oid": razorpay_order_id},
        )
    ).scalar_one()
    assert order_status == "created"
