"""Test helpers for organization contact_person_user_ids JSON."""

from __future__ import annotations


def org_contact_person_ids(user_id: int | None) -> dict | None:
    if user_id is None:
        return None
    return {"organization_managers": [user_id]}
