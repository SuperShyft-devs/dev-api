"""Add linked profile columns and constraints on users.

Revision ID: 0007_user_linked_profiles
Revises: 0006_support_tickets
Create Date: 2026-03-15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


revision = "0007_user_linked_profiles"
down_revision = "0006_support_tickets"
branch_labels = None
depends_on = None


_RELATIONSHIP_CHECK_NAME = "ck_users_relationship_allowed"
_PRIMARY_PHONE_UNIQUE_INDEX = "uq_users_phone_primary_only"
_EMAIL_UNIQUE_CONSTRAINT = "uq_users_email_global"


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _constraint_exists(connection: sa.Connection, constraint_name: str) -> bool:
    result = connection.execute(
        text(
            """
            SELECT 1
            FROM information_schema.table_constraints
            WHERE constraint_name = :constraint_name
            LIMIT 1
            """
        ),
        {"constraint_name": constraint_name},
    )
    return result.scalar() is not None


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "users"):
        return

    if not _column_exists(inspector, "users", "parent_id"):
        op.add_column("users", sa.Column("parent_id", sa.Integer(), nullable=True))
        op.create_foreign_key("fk_users_parent_id", "users", "users", ["parent_id"], ["user_id"])

    inspector = inspect(connection)
    if not _column_exists(inspector, "users", "relationship"):
        op.add_column(
            "users",
            sa.Column("relationship", sa.String(), nullable=True, server_default=sa.text("'self'")),
        )

    connection.execute(text("UPDATE users SET parent_id = NULL WHERE parent_id IS NOT NULL"))
    connection.execute(
        text(
            """
            UPDATE users
            SET relationship = 'self'
            WHERE relationship IS NULL OR trim(relationship) = ''
            """
        )
    )

    if not _constraint_exists(connection, _RELATIONSHIP_CHECK_NAME):
        op.create_check_constraint(
            _RELATIONSHIP_CHECK_NAME,
            "users",
            "relationship IN ('self', 'spouse', 'child', 'sibling', 'parent', 'grandparent', 'other')",
        )

    op.alter_column("users", "relationship", existing_type=sa.String(), nullable=False)

    inspector = inspect(connection)
    if not _index_exists(inspector, "users", _PRIMARY_PHONE_UNIQUE_INDEX):
        op.create_index(
            _PRIMARY_PHONE_UNIQUE_INDEX,
            "users",
            ["phone"],
            unique=True,
            postgresql_where=sa.text("parent_id IS NULL"),
        )

    if not _constraint_exists(connection, _EMAIL_UNIQUE_CONSTRAINT):
        op.create_unique_constraint(_EMAIL_UNIQUE_CONSTRAINT, "users", ["email"])


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
