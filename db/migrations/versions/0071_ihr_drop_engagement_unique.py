"""Drop unique (user_id, engagement_id) on individual_health_report.

Restores multiple IHR rows per engagement (one per assessment).

Revision ID: 0071_ihr_drop_eng_unique
Revises: 0070_ihr_eng_ext_param
Create Date: 2026-07-04

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0071_ihr_drop_eng_unique"
down_revision = "0070_ihr_eng_ext_param"
branch_labels = None
depends_on = None


def _has_unique_constraint(table: str, name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(uc.get("name") == name for uc in inspector.get_unique_constraints(table))


def upgrade() -> None:
    if _has_unique_constraint(
        "individual_health_report",
        "uq_individual_health_report_user_engagement",
    ):
        op.drop_constraint(
            "uq_individual_health_report_user_engagement",
            "individual_health_report",
            type_="unique",
        )


def downgrade() -> None:
    # Re-applying the unique constraint requires at most one row per
    # (user_id, engagement_id). Dedupe keeping newest report_id.
    op.execute(
        """
        DELETE FROM individual_health_report
        WHERE report_id IN (
            SELECT report_id
            FROM (
                SELECT
                    report_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, engagement_id
                        ORDER BY report_id DESC
                    ) AS rn
                FROM individual_health_report
            ) ranked
            WHERE rn > 1
        )
        """
    )
    if not _has_unique_constraint(
        "individual_health_report",
        "uq_individual_health_report_user_engagement",
    ):
        op.create_unique_constraint(
            "uq_individual_health_report_user_engagement",
            "individual_health_report",
            ["user_id", "engagement_id"],
        )
