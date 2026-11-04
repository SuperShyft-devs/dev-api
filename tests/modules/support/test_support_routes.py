"""Support ticket employee routes."""

from __future__ import annotations

import pytest

from modules.support.models import SupportTicket
from modules.users.models import User
from tests.helpers.auth import employee_auth_header, seed_employee, user_auth_header


async def _create_open_ticket(test_db_session, *, user_id: int, query: str = "Need help") -> SupportTicket:
    ticket = SupportTicket(
        user_id=user_id,
        contact_input="9999999999",
        query_text=query,
        status="open",
    )
    test_db_session.add(ticket)
    await test_db_session.commit()
    await test_db_session.refresh(ticket)
    return ticket


@pytest.mark.asyncio
async def test_resolve_ticket_sets_employee_and_notes(async_client, test_db_session):
    employee_id = 88001
    user_id = 88002
    await seed_employee(test_db_session, employee_id=employee_id, name="Resolver One")
    test_db_session.add(User(user_id=user_id, age=30, phone="8800200000", status="active"))
    await test_db_session.commit()

    ticket = await _create_open_ticket(test_db_session, user_id=user_id)

    response = await async_client.patch(
        f"/support/tickets/{ticket.ticket_id}/status",
        headers=employee_auth_header(employee_id),
        json={"status": "resolved", "resolution_notes": "Cancelled appointment"},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["status"] == "resolved"
    assert body["resolved_by_employee_id"] == employee_id
    assert body["resolved_by_employee_name"] == "Resolver One"
    assert body["resolution_notes"] == "Cancelled appointment"
    assert body["resolved_at"] is not None


@pytest.mark.asyncio
async def test_reopen_ticket_clears_resolver_fields(async_client, test_db_session):
    employee_id = 88003
    user_id = 88004
    await seed_employee(test_db_session, employee_id=employee_id)
    test_db_session.add(User(user_id=user_id, age=30, phone="8800400000", status="active"))
    await test_db_session.commit()

    ticket = await _create_open_ticket(test_db_session, user_id=user_id)
    resolve = await async_client.patch(
        f"/support/tickets/{ticket.ticket_id}/status",
        headers=employee_auth_header(employee_id),
        json={"status": "resolved", "resolution_notes": "Done"},
    )
    assert resolve.status_code == 200

    reopen = await async_client.patch(
        f"/support/tickets/{ticket.ticket_id}/status",
        headers=employee_auth_header(employee_id),
        json={"status": "open"},
    )
    assert reopen.status_code == 200
    body = reopen.json()["data"]
    assert body["status"] == "open"
    assert body["resolved_by_employee_id"] is None
    assert body["resolved_at"] is None
    assert body["resolution_notes"] == "Done"


@pytest.mark.asyncio
async def test_reject_closed_status(async_client, test_db_session):
    employee_id = 88005
    user_id = 88006
    await seed_employee(test_db_session, employee_id=employee_id)
    test_db_session.add(User(user_id=user_id, age=30, phone="8800600000", status="active"))
    await test_db_session.commit()

    ticket = await _create_open_ticket(test_db_session, user_id=user_id)

    response = await async_client.patch(
        f"/support/tickets/{ticket.ticket_id}/status",
        headers=employee_auth_header(employee_id),
        json={"status": "closed"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_ticket_includes_resolution_fields(async_client, test_db_session):
    employee_id = 88007
    user_id = 88008
    await seed_employee(test_db_session, employee_id=employee_id, name="Detail Agent")
    test_db_session.add(User(user_id=user_id, age=30, phone="8800800000", status="active"))
    await test_db_session.commit()

    ticket = await _create_open_ticket(test_db_session, user_id=user_id)
    await async_client.patch(
        f"/support/tickets/{ticket.ticket_id}/status",
        headers=employee_auth_header(employee_id),
        json={"status": "resolved"},
    )

    response = await async_client.get(
        f"/support/tickets/{ticket.ticket_id}",
        headers=employee_auth_header(employee_id),
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["resolved_by_employee_name"] == "Detail Agent"
    assert body["status"] == "resolved"


@pytest.mark.asyncio
async def test_submit_ticket_still_works_without_employee_auth(async_client, test_db_session):
    user_id = 88009
    test_db_session.add(User(user_id=user_id, age=30, phone="8800900000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/support/tickets",
        headers=user_auth_header(user_id),
        json={"user_id": user_id, "query_text": "Help please"},
    )
    assert response.status_code == 201
    assert response.json()["data"]["status"] == "open"
