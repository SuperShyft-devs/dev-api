"""Normalize engagement cabin keys to SlugKey format.

Revision ID: 0124_normalize_cabin_keys
Revises: 0123_otp_svc_require_otp

Converts legacy cabin keys (e.g. room1_1, btc-001) to slug keys derived from
cabin_name using the same rules as common.slug.slugify_cabin_key, with _2/_3
suffixes for duplicates within a slot_detail document. Updates participant and
consultation booking references that point at the old keys.
"""

from __future__ import annotations

import json
import re

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text

revision = "0124_normalize_cabin_keys"
down_revision = "0123_otp_svc_require_otp"
branch_labels = None
depends_on = None

_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_MULTI_UNDERSCORE = re.compile(r"_+")


def _slugify(value: str) -> str:
    normalized = (value or "").strip().lower()
    if not normalized:
        return ""
    slug = _NON_ALNUM.sub("_", normalized)
    return _MULTI_UNDERSCORE.sub("_", slug).strip("_")


def _derive_base_key(cabin_name: str, cabin_key: str) -> str:
    from_name = _slugify(cabin_name)
    if from_name:
        return from_name
    return _slugify(cabin_key)


def _migrate_slot_detail(slot_detail: dict) -> tuple[dict, dict[str, str]]:
    """Return updated slot_detail and old_key -> new_key mapping."""
    if not isinstance(slot_detail, dict):
        return slot_detail, {}

    mapping: dict[str, str] = {}
    used: set[str] = set()
    entries: list[tuple[dict, str, str]] = []

    for section_name in ("blood_collection", "consultation"):
        section = slot_detail.get(section_name)
        if not isinstance(section, dict):
            continue
        for cabins in section.values():
            if not isinstance(cabins, list):
                continue
            for cabin in cabins:
                if not isinstance(cabin, dict):
                    continue
                old_key = (cabin.get("cabin_key") or "").strip()
                cabin_name = (cabin.get("cabin_name") or "").strip()
                entries.append((cabin, old_key, cabin_name))

    for cabin, old_key, cabin_name in entries:
        base = _derive_base_key(cabin_name, old_key)
        if not base:
            if old_key:
                used.add(old_key)
            continue

        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used.add(candidate)
        cabin["cabin_key"] = candidate
        if old_key and old_key != candidate:
            mapping[old_key] = candidate

    return slot_detail, mapping


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '60s'"))
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "engagement_slot_info"):
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
        updated, key_map = _migrate_slot_detail(slot_detail)
        if not key_map:
            continue

        connection.execute(
            text(
                "UPDATE engagement_slot_info "
                "SET slot_detail = CAST(:slot_detail AS jsonb) "
                "WHERE slot_detail_id = :slot_detail_id"
            ),
            {
                "slot_detail_id": slot_detail_id,
                "slot_detail": json.dumps(updated),
            },
        )

        engagement_ids = connection.execute(
            text(
                "SELECT engagement_id FROM engagements "
                "WHERE slot_detail_id = :slot_detail_id"
            ),
            {"slot_detail_id": slot_detail_id},
        ).fetchall()

        for (engagement_id,) in engagement_ids:
            for old_key, new_key in key_map.items():
                connection.execute(
                    text(
                        "UPDATE engagement_participants "
                        "SET blood_collection_cabin = :new_key "
                        "WHERE engagement_id = :engagement_id "
                        "AND blood_collection_cabin = :old_key"
                    ),
                    {
                        "engagement_id": engagement_id,
                        "old_key": old_key,
                        "new_key": new_key,
                    },
                )
                connection.execute(
                    text(
                        "UPDATE consultation_bookings cb "
                        "SET consultation_cabin = :new_key "
                        "FROM engagement_participants ep "
                        "WHERE cb.engagement_participant_id = ep.engagement_participant_id "
                        "AND ep.engagement_id = :engagement_id "
                        "AND cb.consultation_cabin = :old_key"
                    ),
                    {
                        "engagement_id": engagement_id,
                        "old_key": old_key,
                        "new_key": new_key,
                    },
                )


def downgrade() -> None:
    # Data normalization is not reversible without storing the previous keys.
    pass
