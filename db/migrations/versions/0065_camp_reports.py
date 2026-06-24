"""Replace organization_health_report with camp_reports.

Revision ID: 0065_camp_reports
Revises: 0064_camp_report_sections
Create Date: 2026-06-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0065_camp_reports"
down_revision = "0064_camp_report_sections"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "organization_health_report"):
        op.drop_table("organization_health_report")

    if not _table_exists(inspector, "camp_reports"):
        op.create_table(
            "camp_reports",
            sa.Column("report_id", sa.Integer(), nullable=False, autoincrement=True),
            sa.Column("report", sa.JSON(), nullable=True),
            sa.Column("camp_no", sa.BigInteger(), nullable=False),
            sa.Column("department", sa.String(), nullable=True),
            sa.Column("organization_id", sa.Integer(), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.ForeignKeyConstraint(["organization_id"], ["organizations.organization_id"]),
            sa.PrimaryKeyConstraint("report_id"),
        )
        op.create_index("ix_camp_reports_camp_no", "camp_reports", ["camp_no"])
        op.create_index(
            "uq_camp_reports_camp_no_overall",
            "camp_reports",
            ["camp_no"],
            unique=True,
            postgresql_where=sa.text("department IS NULL"),
        )
        op.create_index(
            "uq_camp_reports_camp_no_department",
            "camp_reports",
            ["camp_no", "department"],
            unique=True,
            postgresql_where=sa.text("department IS NOT NULL"),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
