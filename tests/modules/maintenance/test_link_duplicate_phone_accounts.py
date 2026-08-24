"""Tests for linking duplicate primary phone accounts into main/sub profiles."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from modules.employee.models import Employee
from modules.maintenance.link_duplicate_phone_accounts import (
    link_duplicate_phone_accounts,
    names_similar,
    phone_key,
)
from modules.users.models import User


def _user(
    *,
    user_id: int,
    phone: str,
    email: str,
    first_name: str = "Shrenik",
    last_name: str = "Nishar",
    parent_id: int | None = None,
    is_participant: bool | None = True,
) -> User:
    return User(
        user_id=user_id,
        age=30,
        first_name=first_name,
        last_name=last_name,
        phone=phone,
        email=email,
        status="active",
        is_participant=is_participant,
        parent_id=parent_id,
        relationship="self" if parent_id is None else "child",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


async def _seed_engagement(test_db_session, *, engagement_id: int, code: str) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Test Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status) "
            "VALUES (:eid, 'Camp', :code, 'bio_ai', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'running')"
        ),
        {"eid": engagement_id, "code": code},
    )


async def _enroll(test_db_session, *, engagement_id: int, user_id: int, slot: str) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_participants "
            "(engagement_id, user_id, booked_by_user_id, engagement_date, slot_start_time) "
            "VALUES (:eid, :uid, :uid, '2026-02-01', :slot)"
        ),
        {"eid": engagement_id, "uid": user_id, "slot": slot},
    )


def test_phone_key_strips_country_code():
    assert phone_key("9769746493") == "9769746493"
    assert phone_key("+919769746493") == "9769746493"
    assert phone_key("919769746493") == "9769746493"


def test_names_similar_same_person():
    left = _user(user_id=1, phone="1", email="a@example.com")
    right = _user(user_id=2, phone="2", email="b@example.com")
    assert names_similar(left, right) is True
    right.first_name = "John"
    right.last_name = "Doe"
    assert names_similar(left, right) is False


@pytest.mark.asyncio
async def test_link_picks_account_with_more_engagements(test_db_session):
    await _seed_engagement(test_db_session, engagement_id=88001, code="LINK88001")
    await _seed_engagement(test_db_session, engagement_id=88011, code="LINK88011")
    main = _user(user_id=88001, phone="+917000008801", email="link88001-main@example.com")
    extra = _user(user_id=88002, phone="7000008801", email="link88001-sub@example.com")
    test_db_session.add_all([main, extra])
    await test_db_session.flush()
    await _enroll(test_db_session, engagement_id=88001, user_id=88001, slot="09:00")
    await _enroll(test_db_session, engagement_id=88011, user_id=88001, slot="09:20")
    await _enroll(test_db_session, engagement_id=88001, user_id=88002, slot="09:40")
    await test_db_session.commit()

    dry = await link_duplicate_phone_accounts(test_db_session, dry_run=True, phone="7000008801")
    assert dry["linked_groups"] == 1
    assert dry["linked"][0]["main_user_id"] == 88001
    row = (
        await test_db_session.execute(text("SELECT parent_id FROM users WHERE user_id = 88002"))
    ).first()
    assert row.parent_id is None

    applied = await link_duplicate_phone_accounts(test_db_session, dry_run=False, phone="7000008801")
    await test_db_session.commit()
    assert applied["linked_groups"] == 1
    assert applied["linked"][0]["main_user_id"] == 88001
    assert applied["linked"][0]["sub_user_ids"] == [88002]

    extra_row = (
        await test_db_session.execute(
            text("SELECT parent_id, relationship, phone FROM users WHERE user_id = 88002")
        )
    ).first()
    assert extra_row.parent_id == 88001
    assert extra_row.relationship == "other"
    assert extra_row.phone == "+917000008801"

    main_row = (
        await test_db_session.execute(text("SELECT parent_id FROM users WHERE user_id = 88001"))
    ).first()
    assert main_row.parent_id is None


@pytest.mark.asyncio
async def test_link_keeps_employee_as_main(test_db_session):
    await _seed_engagement(test_db_session, engagement_id=88002, code="LINK88002")
    await _seed_engagement(test_db_session, engagement_id=88012, code="LINK88012")
    staff = _user(user_id=88011, phone="+917000008802", email="link88002-staff@example.com")
    member = _user(user_id=88012, phone="7000008802", email="link88002-member@example.com")
    test_db_session.add_all([staff, member])
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=88011, user_id=88011, role="admin", status="active"))
    await _enroll(test_db_session, engagement_id=88002, user_id=88012, slot="09:00")
    await _enroll(test_db_session, engagement_id=88012, user_id=88012, slot="09:20")
    await test_db_session.commit()

    result = await link_duplicate_phone_accounts(test_db_session, dry_run=False, phone="7000008802")
    await test_db_session.commit()
    assert result["linked"][0]["main_user_id"] == 88011
    member_row = (
        await test_db_session.execute(text("SELECT parent_id FROM users WHERE user_id = 88012"))
    ).first()
    assert member_row.parent_id == 88011


@pytest.mark.asyncio
async def test_link_skips_dissimilar_names_when_required(test_db_session):
    left = _user(
        user_id=88021,
        phone="+917000008803",
        email="link88003-a@example.com",
        first_name="John",
        last_name="Doe",
    )
    right = _user(
        user_id=88022,
        phone="7000008803",
        email="link88003-b@example.com",
        first_name="Riya",
        last_name="Sharma",
    )
    test_db_session.add_all([left, right])
    await test_db_session.commit()

    result = await link_duplicate_phone_accounts(
        test_db_session,
        dry_run=False,
        require_similar_name=True,
        phone="7000008803",
    )
    await test_db_session.commit()
    assert result["linked_groups"] == 0
    assert result["skipped"][0]["reason"] == "dissimilar_names"
    for user_id in (88021, 88022):
        row = (
            await test_db_session.execute(
                text("SELECT parent_id FROM users WHERE user_id = :uid"),
                {"uid": user_id},
            )
        ).first()
        assert row.parent_id is None


@pytest.mark.asyncio
async def test_link_reparents_children_of_demoted_primary(test_db_session):
    await _seed_engagement(test_db_session, engagement_id=88003, code="LINK88003")
    main = _user(user_id=88031, phone="+917000008804", email="link88004-main@example.com")
    extra = _user(user_id=88032, phone="7000008804", email="link88004-extra@example.com")
    child = _user(
        user_id=88033,
        phone="7000008819",
        email="link88004-child@example.com",
        first_name="Kid",
        last_name="Nishar",
        parent_id=88032,
    )
    test_db_session.add_all([main, extra, child])
    await test_db_session.flush()
    await _enroll(test_db_session, engagement_id=88003, user_id=88031, slot="09:00")
    await test_db_session.commit()

    result = await link_duplicate_phone_accounts(test_db_session, dry_run=False, phone="7000008804")
    await test_db_session.commit()
    assert result["linked"][0]["reparented_children"] == 1
    child_row = (
        await test_db_session.execute(text("SELECT parent_id FROM users WHERE user_id = 88033"))
    ).first()
    extra_row = (
        await test_db_session.execute(text("SELECT parent_id FROM users WHERE user_id = 88032"))
    ).first()
    assert extra_row.parent_id == 88031
    assert child_row.parent_id == 88031


@pytest.mark.asyncio
async def test_send_otp_ambiguous_then_ok_after_link(async_client, test_db_session):
    await _seed_engagement(test_db_session, engagement_id=88004, code="LINK88004")
    older = _user(user_id=88041, phone="+917000008805", email="link88005-a@example.com")
    newer = _user(user_id=88042, phone="7000008805", email="link88005-b@example.com")
    test_db_session.add_all([older, newer])
    await test_db_session.flush()
    await _enroll(test_db_session, engagement_id=88004, user_id=88041, slot="09:00")
    await test_db_session.commit()

    blocked = await async_client.post("/auth/send-otp", json={"phone": "7000008805"})
    assert blocked.status_code == 409
    assert blocked.json()["error_code"] == "AMBIGUOUS_PHONE"

    await link_duplicate_phone_accounts(test_db_session, dry_run=False, phone="7000008805")
    await test_db_session.commit()

    allowed = await async_client.post("/auth/send-otp", json={"phone": "7000008805"})
    assert allowed.status_code == 200
