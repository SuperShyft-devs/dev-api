"""Tests for payload contact path resolution."""

from modules.notifications.payload_path_resolver import resolve_payload_contacts


def test_scalar_booker_phone():
    ctx = {"booker": {"phone": "9999888877", "name": "Alex Booker"}}
    contacts = resolve_payload_contacts(ctx, payload_path="booker.phone", value_type="phone")
    assert len(contacts) == 1
    assert contacts[0]["phone"] == "9999888877"
    assert contacts[0]["first_name"] == "Alex"


def test_members_array_phones_deduped():
    ctx = {
        "members": [
            {"phone": "9111111111"},
            {"phone": "9222222222"},
            {"phone": "9111111111"},
        ],
    }
    contacts = resolve_payload_contacts(ctx, payload_path="members[].phone", value_type="phone")
    assert len(contacts) == 2
    phones = {c["phone"] for c in contacts}
    assert phones == {"9111111111", "9222222222"}


def test_nested_support_user_email():
    ctx = {"participant_details": {"user": {"email": "User@Example.COM"}}}
    contacts = resolve_payload_contacts(
        ctx, payload_path="participant_details.user.email", value_type="email"
    )
    assert contacts == [{"email": "user@example.com"}]


def test_missing_values_returns_empty():
    ctx = {"booker": {"email": ""}}
    assert resolve_payload_contacts(ctx, payload_path="booker.phone", value_type="phone") == []
