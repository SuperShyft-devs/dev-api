"""Add per-engagement-type B2C onboarding defaults JSONB.

Revision ID: 0106_b2c_by_engagement_type
Revises: 0105_drop_old_cols_unique_ep
"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision = "0106_b2c_by_engagement_type"
down_revision = "0105_drop_old_cols_unique_ep"
branch_labels = None
depends_on = None

_ENGAGEMENT_KINDS = (
    "bio_ai",
    "blood_test",
    "consultation",
    "blood_test_with_consultation",
    "bio_ai_with_consultation",
)


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

    if not _column_exists(inspector, table_name, "b2c_onboarding_by_engagement_type"):
        op.add_column(
            table_name,
            sa.Column(
                "b2c_onboarding_by_engagement_type",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            ),
        )

    rows = connection.execute(
        sa.text(
            """
            SELECT
                settings_id,
                b2c_default_assessment_package_id,
                b2c_default_diagnostic_package_id,
                b2c_default_blood_collection_type::text,
                b2c_default_create_profile_on_metsights,
                b2c_default_enroll_for_fitprint_full,
                b2c_onboarding_by_engagement_type
            FROM platform_settings
            """
        )
    ).fetchall()

    for row in rows:
        if row.b2c_onboarding_by_engagement_type:
            continue
        slice_defaults = {
            "assessment_package_id": int(row.b2c_default_assessment_package_id),
            "diagnostic_package_id": int(row.b2c_default_diagnostic_package_id),
            "blood_collection_type": row.b2c_default_blood_collection_type,
            "create_profile_on_metsights": bool(row.b2c_default_create_profile_on_metsights),
            "enroll_for_fitprint_full": bool(row.b2c_default_enroll_for_fitprint_full),
        }
        payload = {kind: dict(slice_defaults) for kind in _ENGAGEMENT_KINDS}
        connection.execute(
            sa.text(
                """
                UPDATE platform_settings
                SET b2c_onboarding_by_engagement_type = CAST(:payload AS jsonb)
                WHERE settings_id = :settings_id
                """
            ),
            {"payload": json.dumps(payload), "settings_id": row.settings_id},
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
