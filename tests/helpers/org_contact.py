"""Test helpers for organization contact_person_user_ids JSON.

Values are partner_ids with role=organization_manager (column name retained).
"""

from __future__ import annotations


def org_contact_person_ids(partner_id: int | None) -> dict | None:
    if partner_id is None:
        return None
    return {"organization_managers": [partner_id]}
