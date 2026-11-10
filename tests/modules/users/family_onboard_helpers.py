"""Shared seeds and payloads for family onboard tests."""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db.column_types import STATUS_ENGAGEMENT
from modules.engagements.constants import DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY
from tests.helpers.auth import seed_employee

ONBOARDING_ALERT_SERVICE_KEY = DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY


def engagement_status_is(raw_status: object, label: str) -> bool:
    """Match engagement status from raw SQL row or ORM (int code or string label)."""
    expected = STATUS_ENGAGEMENT[label]
    if raw_status is None:
        return False
    if isinstance(raw_status, int):
        return raw_status == expected
    s = str(raw_status).strip().lower()
    if s.isdigit():
        return int(s) == expected
    return s == label


async def seed_family_onboard_gift_notification_services(test_db_session: AsyncSession) -> None:
    """Seed hardcoded family-onboard Diwali gift notification services."""
    from modules.notifications.family_onboard_gift_notify import (
        RECEIVER_EMAIL_KEY,
        RECEIVER_WHATSAPP_KEY,
        SENDER_EMAIL_KEY,
        SENDER_WHATSAPP_KEY,
    )

    for key, channel, path in (
        (SENDER_WHATSAPP_KEY, "whatsapp", "/family-onboard-sender-whatsapp-v1"),
        (SENDER_EMAIL_KEY, "email", "/family-onboard-sender-email-v1"),
        (RECEIVER_WHATSAPP_KEY, "whatsapp", "/family-onboard-receiver-whatsapp-v1"),
        (RECEIVER_EMAIL_KEY, "email", "/family-onboard-receiver-email-v1"),
    ):
        await test_db_session.execute(
            text(
                "INSERT INTO notification_services "
                "(service_key, display_name, channel, webhook_path, is_active, "
                "require_blood_report_url, require_bio_ai_report_url, require_participant_detail, "
                "require_otp, require_session_details, require_external_link) "
                "VALUES (:sk, :sk, :ch, :path, true, false, false, true, false, false, false) "
                "ON CONFLICT (service_key) DO UPDATE SET "
                "require_participant_detail = true, is_active = true"
            ),
            {"sk": key, "ch": channel, "path": path},
        )
    await test_db_session.commit()


async def seed_family_onboard_onboarding_notification_setup(
    test_db_session: AsyncSession,
    *,
    employee_id: int = 8801,
) -> None:
    """Default OA + onboarding event services so family confirm fires Booking Alert."""
    service_key = ONBOARDING_ALERT_SERVICE_KEY
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, "
            "require_blood_report_url, require_bio_ai_report_url, require_participant_detail, "
            "require_otp, require_session_details, require_external_link) "
            "VALUES (:sk, 'Booking Alert', 'whatsapp', '/booking-alert-test-v1', true, "
            "false, false, false, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('bio_ai', 'BioAI', true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        )
    )
    type_row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai'")
        )
    ).one()
    type_id = int(type_row[0])
    services_json = json.dumps([{"service_key": service_key, "external_link": None}])
    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, notification_services, engagement_type_id
            )
            VALUES ('users.onboarded', 'User onboarded', 'assistants', CAST(:services AS json), :type_id)
            ON CONFLICT ON CONSTRAINT uq_notification_events_binding DO UPDATE
            SET notification_services = EXCLUDED.notification_services
            """
        ),
        {"type_id": type_id, "services": services_json},
    )
    await seed_employee(
        test_db_session,
        employee_id=employee_id,
        role="admin",
        name="Family OA",
        phone="8801000001",
        email="family.oa@test.example",
        commit=False,
    )
    from tests.helpers.employee_departments import seed_employee_departments

    await seed_employee_departments(
        test_db_session, employee_id=employee_id, departments=["ops"], commit=False
    )
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id) "
            "VALUES (1, 1, 6) "
            "ON CONFLICT (settings_id) DO NOTHING"
        )
    )
    await test_db_session.commit()


async def seed_family_onboard_platform_defaults(test_db_session: AsyncSession) -> None:
    """Seed packages aligned with platform_settings B2C default diagnostic_package_id=6."""
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package "
            "(diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price, external_package_code) "
            "VALUES (6, 'REF-H6', 'Healthians Package', 'healthians', 1, 500, '101') "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, "
            "external_package_code = EXCLUDED.external_package_code, status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()


def _family_onboard_type_defaults_entry(*, auto_booking: bool) -> dict:
    return {
        "assessment_package_id": 1,
        "diagnostic_package_id": 6,
        "blood_collection_type": "home_collection",
        "create_profile_on_metsights": True,
        "enroll_for_fitprint_full": False,
        "auto_booking": auto_booking,
        "slot_duration": 20,
        "payment_type": "paid_by_patient",
    }


async def seed_family_onboard_b2c_onboarding_settings(
    test_db_session: AsyncSession,
    *,
    auto_booking: bool = False,
    engagement_type_code: str = "bio_ai",
) -> None:
    """Seed packages and per-type B2C onboarding defaults (including auto_booking)."""
    await seed_family_onboard_platform_defaults(test_db_session)
    by_type = {engagement_type_code: _family_onboard_type_defaults_entry(auto_booking=auto_booking)}
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights, b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type) "
            "VALUES (1, 1, 6, 'bio_ai', 'home_collection', true, false, CAST(:by_type AS jsonb)) "
            "ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type = EXCLUDED.b2c_default_engagement_type, "
            "b2c_default_blood_collection_type = EXCLUDED.b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights = EXCLUDED.b2c_default_create_profile_on_metsights, "
            "b2c_default_enroll_for_fitprint_full = EXCLUDED.b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type = EXCLUDED.b2c_onboarding_by_engagement_type"
        ),
        {"by_type": json.dumps(by_type)},
    )
    await test_db_session.commit()


async def seed_primary_user(
    test_db_session: AsyncSession,
    *,
    user_id: int,
    phone: str,
    email: str,
    relationship: str = "self",
    first_name: str = "Primary",
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, age, phone, email, status, parent_id, relationship) "
            "VALUES (:uid, :first_name, 35, :phone, :email, 1, NULL, CAST(:relationship AS user_relationship_enum)) "
            "ON CONFLICT (user_id) DO NOTHING"
        ),
        {
            "uid": user_id,
            "first_name": first_name,
            "phone": phone,
            "email": email,
            "relationship": relationship,
        },
    )
    await test_db_session.commit()


async def seed_subprofile_user(
    test_db_session: AsyncSession,
    *,
    user_id: int,
    parent_id: int,
    phone: str,
    email: str,
    relationship: str = "child",
    first_name: str = "Sub",
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, age, phone, email, status, parent_id, relationship) "
            "VALUES (:uid, :first_name, 12, :phone, :email, 1, :parent_id, "
            "CAST(:relationship AS user_relationship_enum)) "
            "ON CONFLICT (user_id) DO NOTHING"
        ),
        {
            "uid": user_id,
            "parent_id": parent_id,
            "first_name": first_name,
            "phone": phone,
            "email": email,
            "relationship": relationship,
        },
    )
    await test_db_session.commit()


def engagement_ids_from_start(start_data: dict) -> list[int]:
    """Collect engagement_id values from a family-onboard start response data object."""
    return [int(m["engagement_id"]) for m in start_data["members"]]


def family_onboard_start_payload(
    *,
    booker_phone: str = "9000100001",
    member_phone: str = "9000100002",
    second_member_phone: str | None = None,
    member_relation: str = "child",
    booker_email: str = "adi.family@example.com",
    member_email: str | None = None,
) -> dict:
    member_entry: dict = {
        "name": "Akash Kumar",
        "phone": member_phone,
        "relation": member_relation,
        "age": 28,
        "gender": "Male",
        "blood_collection_date": "2026-03-15",
        "blood_collection_time_slot": "10:00",
    }
    if member_email is not None:
        member_entry["email"] = member_email
    members = [member_entry]
    if second_member_phone:
        members.append(
            {
                "name": "Riya Kumar",
                "phone": second_member_phone,
                "relation": "spouse",
                "age": 26,
                "gender": "Female",
                "blood_collection_date": "2026-03-15",
                "blood_collection_time_slot": "11:00",
            }
        )
    return {
        "booker": {
            "name": "Adi Kumar",
            "phone": booker_phone,
            "email": booker_email,
        },
        "members": members,
    }
