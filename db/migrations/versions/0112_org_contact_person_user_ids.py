"""Replace contact_person_user_id with contact_person_user_ids JSON.

Revision ID: 0112_org_contact_person_user_ids
Revises: 0111_engagement_slot_detail
"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0112_org_contact_person_user_ids"
down_revision = "0111_engagement_slot_detail"
branch_labels = None
depends_on = None


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    return column_name in {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _column_exists(inspector, "organizations", "contact_person_user_ids"):
        op.add_column(
            "organizations",
            sa.Column("contact_person_user_ids", sa.JSON(), nullable=True),
        )

    if _column_exists(inspector, "organizations", "contact_person_user_id"):
        rows = connection.execute(
            text(
                "SELECT organization_id, contact_person_user_id "
                "FROM organizations "
                "WHERE contact_person_user_id IS NOT NULL"
            )
        ).fetchall()
        for organization_id, contact_person_user_id in rows:
            payload = json.dumps({"organization_managers": [int(contact_person_user_id)]})
            connection.execute(
                text(
                    "UPDATE organizations "
                    "SET contact_person_user_ids = CAST(:payload AS json) "
                    "WHERE organization_id = :organization_id"
                ),
                {"payload": payload, "organization_id": int(organization_id)},
            )

        op.drop_constraint(
            "fk_organizations_contact_person_user_id",
            "organizations",
            type_="foreignkey",
        )
        op.drop_column("organizations", "contact_person_user_id")


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
