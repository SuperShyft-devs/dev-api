"""Payload contact catalog and HTTP-only gate."""

from modules.notifications.payload_contact_catalog import (
    allows_payload_recipient,
    get_payload_contact_field,
    payload_contact_fields_for_event,
)


def test_payments_completed_has_booker_and_members_paths():
    fields = payload_contact_fields_for_event("payments.completed")
    paths = {f["path"] for f in fields}
    assert "booker.phone" in paths
    assert "members[].phone" in paths
    assert allows_payload_recipient("payments.completed")


def test_cron_event_disallows_payload_recipient():
    assert not allows_payload_recipient("reminders.pretest")
    assert payload_contact_fields_for_event("reminders.pretest") == []


def test_get_payload_contact_field():
    field = get_payload_contact_field("payments.completed", "members[].email")
    assert field is not None
    assert field.value_type == "email"
    assert field.cardinality == "array"
