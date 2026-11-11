"""Tests for payload contact field extraction."""

from __future__ import annotations

from modules.notifications.emit_contact_catalog import (
    payload_contact_fields_for_event,
    resolve_payload_targets,
)
from modules.notifications.event_payload_meta import payload_trigger_apis_for_event


def test_payments_completed_picker_fields_merged_from_trigger_apis():
    fields = payload_contact_fields_for_event("payments.completed")
    assert len(fields) == 6
    assert {f["id"] for f in fields} == {
        "booker.phone",
        "booker.email",
        "payer_user_id",
        "members.phones",
        "members.emails",
        "members.user_ids",
    }
    apis = payload_trigger_apis_for_event("payments.completed")
    assert "POST /book/bio-ai" in apis
    assert "POST /payments/verify" in apis


def test_resolve_payload_members_phones_dedupe():
    ctx = {
        "booker": {"phone": "9000000001", "user_id": 1},
        "members": [
            {"phone": "9000000002", "user_id": 2},
            {"phone": "9000000001", "user_id": 3},
        ],
    }
    uids, contacts = resolve_payload_targets(
        ctx,
        ["booker.phone", "members.phones"],
        "payments.completed",
    )
    assert uids == []
    phones = [c.get("phone") for c in contacts if c.get("phone")]
    assert sorted(phones) == ["9000000001", "9000000002"]


def test_resolve_payload_user_ids_and_email():
    ctx = {
        "user_id": 10,
        "email": "a@example.com",
        "members": [{"email": "b@example.com", "user_id": 11}],
    }
    uids, contacts = resolve_payload_targets(
        ctx,
        ["user_id", "email", "members.emails", "members.user_ids"],
        "payments.completed",
    )
    assert 10 in uids and 11 in uids
    emails = [c.get("email") for c in contacts if c.get("email")]
    assert "a@example.com" in emails
    assert "b@example.com" in emails
