"""Add mandatory age and keep optional date_of_birth.

Revision ID: 0008_users_add_age_nullable_dob
Revises: 0007_user_linked_profiles
Create Date: 2026-03-16
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


revision = "0008_users_add_age_nullable_dob"
down_revision = "0007_user_linked_profiles"
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

    if not _table_exists(inspector, "users"):
        return

    # Step 1: add column first as nullable to support existing data.
    if not _column_exists(inspector, "users", "age"):
        op.add_column("users", sa.Column("age", sa.Integer(), nullable=True))

    # Step 2: backfill existing rows before NOT NULL enforcement.
    connection.execute(text("UPDATE users SET age = 0 WHERE age IS NULL"))

    # Enforce NOT NULL after backfill.
    op.alter_column("users", "age", existing_type=sa.Integer(), nullable=False)

    # Step 3: ensure date_of_birth remains optional.
    if _column_exists(inspector, "users", "date_of_birth"):
        op.alter_column("users", "date_of_birth", existing_type=sa.Date(), nullable=True)


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
