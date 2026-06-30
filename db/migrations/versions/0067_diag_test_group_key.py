"""Add group_key column to diagnostic_test_groups.

Revision ID: 0067_diag_test_group_key
Revises: 0066_org_contact_person
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0067_diag_test_group_key"
down_revision = "0066_org_contact_person"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return column_name in {col["name"] for col in inspector.get_columns(table_name)}


def _constraint_exists(inspector: sa.Inspector, table_name: str, constraint_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return constraint_name in {uc["name"] for uc in inspector.get_unique_constraints(table_name)}


def _slugify_group_name(value: str) -> str:
    normalized = value.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", normalized)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "group"


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)
    table_name = "diagnostic_test_groups"

    if not _table_exists(inspector, table_name):
        return

    if not _column_exists(inspector, table_name, "group_key"):
        op.add_column(table_name, sa.Column("group_key", sa.String(), nullable=True))

    rows = connection.execute(
        text("SELECT group_id, group_name, group_key FROM diagnostic_test_groups ORDER BY group_id")
    ).fetchall()
    used_keys: set[str] = set()
    for row in rows:
        group_id, group_name, existing_key = row[0], row[1], row[2]
        if existing_key:
            used_keys.add(existing_key)
            continue
        base = _slugify_group_name(group_name or "")
        candidate = base
        suffix = 2
        while candidate in used_keys:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used_keys.add(candidate)
        connection.execute(
            text("UPDATE diagnostic_test_groups SET group_key = :group_key WHERE group_id = :group_id"),
            {"group_key": candidate, "group_id": group_id},
        )

    inspector = inspect(connection)
    if _column_exists(inspector, table_name, "group_key"):
        op.alter_column(table_name, "group_key", nullable=False)

    inspector = inspect(connection)
    if not _constraint_exists(inspector, table_name, "uq_diagnostic_test_groups_group_key"):
        op.create_unique_constraint(
            "uq_diagnostic_test_groups_group_key",
            table_name,
            ["group_key"],
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
