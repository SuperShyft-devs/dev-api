"""organization contact_person replaces contact name/email/phone

Revision ID: 0062_organization_contact_person
Revises: 0061_engagement_camp_no
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0062_organization_contact_person"
down_revision = "0061_engagement_camp_no"
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

    if not _table_exists(inspector, "organizations"):
        return

    if not _column_exists(inspector, "organizations", "contact_person"):
        op.add_column("organizations", sa.Column("contact_person", sa.Integer(), nullable=True))
        op.create_foreign_key(
            "fk_organizations_contact_person_users",
            "organizations",
            "users",
            ["contact_person"],
            ["user_id"],
        )

    inspector = inspect(connection)
    for column_name in ("contact_name", "contact_email", "contact_phone"):
        if _column_exists(inspector, "organizations", column_name):
            op.drop_column("organizations", column_name)


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
