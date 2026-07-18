"""Add engagement and integration defaults for B2C onboarding.

Revision ID: 0099_b2c_onboarding_defaults
Revises: 0098_consult_summary_attach
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision = "0099_b2c_onboarding_defaults"
down_revision = "0098_consult_summary_attach"
branch_labels = None
depends_on = None


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if table_name not in inspector.get_table_names():
        return False
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)
    table_name = "platform_settings"
    if table_name not in inspector.get_table_names():
        return

    engagement_kind = postgresql.ENUM(name="engagement_kind", create_type=False)
    blood_collection_type = postgresql.ENUM(name="blood_collection_type_enum", create_type=False)

    if not _column_exists(inspector, table_name, "b2c_default_engagement_type"):
        op.add_column(
            table_name,
            sa.Column(
                "b2c_default_engagement_type",
                engagement_kind,
                nullable=False,
                server_default=sa.text("'bio_ai'"),
            ),
        )

    if not _column_exists(inspector, table_name, "b2c_default_blood_collection_type"):
        op.add_column(
            table_name,
            sa.Column("b2c_default_blood_collection_type", blood_collection_type, nullable=True),
        )

    if not _column_exists(inspector, table_name, "b2c_default_create_profile_on_metsights"):
        op.add_column(
            table_name,
            sa.Column(
                "b2c_default_create_profile_on_metsights",
                sa.Boolean(),
                nullable=False,
                server_default=sa.true(),
            ),
        )

    if not _column_exists(inspector, table_name, "b2c_default_enroll_for_fitprint_full"):
        op.add_column(
            table_name,
            sa.Column(
                "b2c_default_enroll_for_fitprint_full",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            ),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
