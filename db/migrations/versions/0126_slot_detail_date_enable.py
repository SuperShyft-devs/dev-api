"""Wrap slot_detail dates as {is_enable, cabins}.

Revision ID: 0126_slot_detail_date_enable
Revises: 0125_hp_multiple_choice

Converts legacy engagement_slot_info.slot_detail shape
  section → date → [cabin, ...]
to
  section → date → { is_enable: true, cabins: [...] }

Already-wrapped date entries are left unchanged.
Downgrade is intentionally a no-op.

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text

revision = "0126_slot_detail_date_enable"
down_revision = "0125_hp_multiple_choice"
branch_labels = None
depends_on = None


def _wrap_date_entry(raw):
    if isinstance(raw, list):
        return {"is_enable": True, "cabins": raw}
    if isinstance(raw, dict) and ("cabins" in raw or "is_enable" in raw):
        cabins = raw.get("cabins")
        return {
            "is_enable": raw.get("is_enable", True) is not False,
            "cabins": cabins if isinstance(cabins, list) else [],
        }
    return {"is_enable": True, "cabins": []}


def _rewrite_slot_detail(slot_detail):
    if not isinstance(slot_detail, dict):
        return slot_detail, False
    changed = False
    updated = {}
    for section_name in ("blood_collection", "consultation"):
        section = slot_detail.get(section_name)
        if section is None:
            continue
        if not isinstance(section, dict):
            updated[section_name] = section
            continue
        new_section = {}
        for date_key, raw in section.items():
            if isinstance(raw, list):
                new_section[date_key] = _wrap_date_entry(raw)
                changed = True
            elif isinstance(raw, dict) and ("cabins" in raw or "is_enable" in raw):
                wrapped = _wrap_date_entry(raw)
                new_section[date_key] = wrapped
                if wrapped != raw:
                    changed = True
            else:
                new_section[date_key] = _wrap_date_entry(raw)
                changed = True
        updated[section_name] = new_section
    for key, value in slot_detail.items():
        if key not in updated:
            updated[key] = value
    return updated, changed


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '60s'"))
    connection = op.get_bind()
    inspector = inspect(connection)
    if "engagement_slot_info" not in inspector.get_table_names():
        return

    rows = connection.execute(
        text("SELECT slot_detail_id, slot_detail FROM engagement_slot_info ORDER BY slot_detail_id")
    ).fetchall()
    for slot_detail_id, slot_detail_raw in rows:
        if slot_detail_raw is None:
            continue
        slot_detail = (
            slot_detail_raw
            if isinstance(slot_detail_raw, dict)
            else json.loads(slot_detail_raw)
        )
        rewritten, changed = _rewrite_slot_detail(slot_detail)
        if not changed:
            continue
        connection.execute(
            text(
                "UPDATE engagement_slot_info "
                "SET slot_detail = CAST(:slot_detail AS jsonb) "
                "WHERE slot_detail_id = :slot_detail_id"
            ),
            {
                "slot_detail": json.dumps(rewritten),
                "slot_detail_id": slot_detail_id,
            },
        )


def downgrade() -> None:
    pass
