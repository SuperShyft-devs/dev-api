"""Tests for authenticated B2C Bio AI booking (`POST /users/me/book-bio-ai`)."""

from __future__ import annotations

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


@pytest.mark.asyncio
async def test_book_bio_ai_requires_auth(async_client):
    response = await async_client.post(
        "/users/me/book-bio-ai",
        json={"blood_collection_date": "2026-02-01", "blood_collection_time_slot": "10:00"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_book_bio_ai_returns_payment_info(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PK1', 'Package', '1', 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "assessment_type_code = EXCLUDED.assessment_type_code, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status, price) "
            "VALUES (1, 'REF1', 'Diag Package', 'active', 1500) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()

    user = User(
        user_id=915010,
        age=30,
        phone="9150100000",
        status="active",
        first_name="Book",
        last_name="User",
        gender="male",
        city="Mumbai",
        address="Test Addr",
        pin_code="400001",
        is_participant=False,
    )
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(915010)
    payload = {"blood_collection_date": "2026-02-01", "blood_collection_time_slot": "10:30"}

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        response = await async_client.post("/users/me/book-bio-ai", headers=headers, json=payload)

    assert response.status_code == 200
    data = response.json()["data"]
    assert "razorpay_order_id" in data
    assert data["amount_paise"] == 150000
    assert len(data["booking_ids"]) == 1
    assert data["currency"] == "INR"

    booking_row = (
        await test_db_session.execute(
            text("SELECT booking_type, metadata FROM bookings WHERE booking_id = :bid"),
            {"bid": data["booking_id"]},
        )
    ).first()
    assert booking_row.booking_type == "bio_ai"
    assert booking_row.metadata is not None


@pytest.mark.asyncio
async def test_book_bio_ai_diagnostic_package_override(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status, price) VALUES "
            "(1, 'REF1', 'Diag 1', 'active', 500), (2, 'REF2', 'Diag 2', 'active', 800) "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings (settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id) "
            "VALUES (1, 1, 1) ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id"
        )
    )
    await test_db_session.commit()

    user = User(
        user_id=915011,
        age=28,
        phone="9150110000",
        status="active",
        first_name="Diag",
        last_name="Pick",
        gender="female",
        city="Pune",
        address="Addr",
        pin_code="411001",
        is_participant=True,
    )
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(915011)
    payload = {
        "blood_collection_date": "2026-03-01",
        "blood_collection_time_slot": "09:00",
        "diagnostic_package_id": 2,
    }

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        response = await async_client.post("/users/me/book-bio-ai", headers=headers, json=payload)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["amount_paise"] == 80000

    booking_row = (
        await test_db_session.execute(
            text("SELECT metadata FROM bookings WHERE booking_id = :bid"),
            {"bid": data["booking_id"]},
        )
    ).first()
    assert booking_row.metadata["diagnostic_package_id"] == 2
