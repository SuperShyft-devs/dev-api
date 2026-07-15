"""Create expert_types table and seed initial rows.

Revision ID: 0086_expert_types
Revises: 0085_diag_pkg_image
Create Date: 2026-07-15

Changes:
- Create expert_types table (id PK, type_key unique, type)
- Seed: doctor, nutritionist
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0086_expert_types"
down_revision = "0085_diag_pkg_image"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "expert_types"):
        op.create_table(
            "expert_types",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("type_key", sa.String(), nullable=False),
            sa.Column("type", sa.String(), nullable=False),
            sa.UniqueConstraint("type_key", name="uq_expert_types_type_key"),
        )
        op.execute(
            "INSERT INTO expert_types (type_key, type) VALUES ('doctor', 'Doctor'), ('nutritionist', 'Nutritionist')"
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
