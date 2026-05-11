"""Tests for Bio AI booking (`POST /users/{user_id}/book-bio-ai`) — no Razorpay."""

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


@pytest.mark.asyncio
async def test_book_bio_ai_requires_auth(async_client):
    response = await async_client.post(
        "/users/1/book-bio-ai",
        json={"blood_collection_date": "2026-02-01", "blood_collection_time_slot": "10:00"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_book_bio_ai_creates_booking_without_payment(async_client, test_db_session, monkeypatch):
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
        side_effect=AssertionError("Razorpay must not be called for book-bio-ai"),
    ):
        response = await async_client.post("/users/915010/book-bio-ai", headers=headers, json=payload)

    assert response.status_code == 200
    data = response.json()["data"]
    assert "razorpay_order_id" not in data
    assert "amount_paise" not in data
    assert data["user_id"] == 915010
    assert data["created"] is False
    assert data["is_participant"] is True
    assert data["engagement_id"] is not None
    assert data["engagement_code"]
    assert data["engagement_participant_id"] is not None
    assert data["assessment_instance_id"] is not None

    booking_row = (
        await test_db_session.execute(
            text(
                "SELECT booking_id, booking_type, metadata, status FROM bookings "
                "WHERE user_id = :uid AND booking_type = 'bio_ai' ORDER BY booking_id DESC LIMIT 1"
            ),
            {"uid": 915010},
        )
    ).first()
    assert booking_row.booking_type == "bio_ai"
    assert booking_row.metadata is not None
    assert booking_row.status == "confirmed"

    order_rows = (
        await test_db_session.execute(
            text("SELECT COUNT(*) FROM orders WHERE booking_id = :bid"),
            {"bid": booking_row.booking_id},
        )
    ).scalar()
    assert order_rows == 0
    junction_rows = (
        await test_db_session.execute(
            text("SELECT COUNT(*) FROM order_bookings WHERE booking_id = :bid"),
            {"bid": booking_row.booking_id},
        )
    ).scalar()
    assert junction_rows == 0


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
        side_effect=AssertionError("Razorpay must not be called"),
    ):
        response = await async_client.post("/users/915011/book-bio-ai", headers=headers, json=payload)

    assert response.status_code == 200
    data = response.json()["data"]
    assert "amount_paise" not in data
    assert data["user_id"] == 915011

    booking_row = (
        await test_db_session.execute(
            text(
                "SELECT metadata, amount_paise FROM bookings "
                "WHERE user_id = :uid AND booking_type = 'bio_ai' ORDER BY booking_id DESC LIMIT 1"
            ),
            {"uid": 915011},
        )
    ).first()
    assert booking_row.metadata["diagnostic_package_id"] == 2
    assert booking_row.amount_paise == 80000


@pytest.mark.asyncio
async def test_book_bio_ai_forbidden_unrelated_user(async_client, test_db_session, monkeypatch):
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

    u1 = User(
        user_id=915020,
        age=30,
        phone="9150200000",
        status="active",
        first_name="A",
        last_name="One",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    u2 = User(
        user_id=915021,
        age=30,
        phone="9150210000",
        status="active",
        first_name="B",
        last_name="Two",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add_all([u1, u2])
    await test_db_session.commit()

    payload = {"blood_collection_date": "2026-02-01", "blood_collection_time_slot": "10:30"}
    response = await async_client.post("/users/915021/book-bio-ai", headers=_auth_header(915020), json=payload)
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_book_bio_ai_employee_can_book_for_user(async_client, test_db_session, monkeypatch):
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

    staff = User(
        user_id=915030,
        age=40,
        phone="9150300000",
        status="active",
        first_name="Staff",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    participant = User(
        user_id=915031,
        age=25,
        phone="9150310000",
        status="active",
        first_name="Pat",
        last_name="Ient",
        gender="female",
        city="Pune",
        address="Patient Rd",
        pin_code="411002",
        parent_id=None,
    )
    test_db_session.add_all([staff, participant])
    await test_db_session.commit()

    await test_db_session.execute(
        text(
            "INSERT INTO employee (employee_id, user_id, role, status) VALUES (915099, 915030, 'admin', 'active') "
            "ON CONFLICT (employee_id) DO UPDATE SET user_id = EXCLUDED.user_id, role = EXCLUDED.role, status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()

    payload = {"blood_collection_date": "2026-02-15", "blood_collection_time_slot": "11:00"}
    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=AssertionError("Razorpay must not be called"),
    ):
        response = await async_client.post("/users/915031/book-bio-ai", headers=_auth_header(915030), json=payload)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 915031
    assert data["created"] is False
    assert data["is_participant"] is True
    assert data["engagement_id"] is not None
    assert "razorpay_order_id" not in data
