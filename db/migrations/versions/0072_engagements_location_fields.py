"""Engagements: sub_locality, landmark, state, country, latitude, longitude.

Revision ID: 0072_eng_location_fields
Revises: 0071_ihr_drop_eng_unique
Create Date: 2026-07-04

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0072_eng_location_fields"
down_revision = "0071_ihr_drop_eng_unique"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "engagements"):
        return

    columns = [
        ("sub_locality", sa.Column("sub_locality", sa.String(), nullable=True)),
        ("landmark", sa.Column("landmark", sa.String(), nullable=True)),
        ("state", sa.Column("state", sa.String(), nullable=True)),
        ("country", sa.Column("country", sa.String(), nullable=True)),
        ("latitude", sa.Column("latitude", sa.Float(), nullable=True)),
        ("longitude", sa.Column("longitude", sa.Float(), nullable=True)),
    ]
    for name, column in columns:
        if not _column_exists(inspector, "engagements", name):
            op.add_column("engagements", column)


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "engagements"):
        return

    for name in ("longitude", "latitude", "country", "state", "landmark", "sub_locality"):
        if _column_exists(inspector, "engagements", name):
            op.drop_column("engagements", name)
