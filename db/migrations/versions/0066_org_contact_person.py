"""organization contact person and organization_manager role

Revision ID: 0066_org_contact_person
Revises: 0065_camp_reports
Create Date: 2026-06-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0066_org_contact_person"
down_revision = "0065_camp_reports"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    return column_name in {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "organizations"):
        if not _column_exists(inspector, "organizations", "contact_person_user_id"):
            op.add_column(
                "organizations",
                sa.Column("contact_person_user_id", sa.Integer(), nullable=True),
            )
            op.create_foreign_key(
                "fk_organizations_contact_person_user_id",
                "organizations",
                "users",
                ["contact_person_user_id"],
                ["user_id"],
            )

        for column in ("contact_name", "contact_email", "contact_phone", "contact_designation"):
            if _column_exists(inspector, "organizations", column):
                op.drop_column("organizations", column)
            inspector = inspect(connection)

    connection.execute(
        text("ALTER TYPE employee_role ADD VALUE IF NOT EXISTS 'organization_manager'")
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
