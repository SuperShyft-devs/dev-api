"""Recipient-aware template variable fields."""

from modules.notifications.variable_catalog import (
    is_team_recipient,
    variable_fields_for_recipient,
)


def test_ops_recipient_includes_staff_and_participant():
    groups = ("user", "engagement", "session")
    fields = variable_fields_for_recipient(groups, "departments.ops")
    assert fields[0] == "staff.name"
    assert "participant.name" in fields
    assert "user.name" not in fields


def test_user_recipient_unchanged():
    groups = ("user", "engagement")
    base = variable_fields_for_recipient(groups, "user")
    team = variable_fields_for_recipient(groups, "departments.tech")
    assert "staff.name" not in base
    assert "staff.name" in team
    assert is_team_recipient("assistants.phlebo")
