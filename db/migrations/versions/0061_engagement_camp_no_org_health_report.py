"""engagement camp_no and organization health report camp fields

Revision ID: 0061_engagement_camp_no
Revises: 0060_category_key_category_of
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text

revision = "0061_engagement_camp_no"
down_revision = "0060_category_key_category_of"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _drop_fks_to_table(inspector: sa.Inspector, *, referenced_table: str) -> None:
    for table_name in inspector.get_table_names():
        for fk in inspector.get_foreign_keys(table_name):
            referred = fk.get("referred_table")
            if referred == referenced_table:
                op.drop_constraint(fk["name"], table_name, type_="foreignkey")


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "organization_camps"):
        _drop_fks_to_table(inspector, referenced_table="organization_camps")
        op.drop_table("organization_camps")

    inspector = inspect(connection)
    if _table_exists(inspector, "engagements") and not _column_exists(inspector, "engagements", "camp_no"):
        op.add_column("engagements", sa.Column("camp_no", sa.Integer(), nullable=True))

    inspector = inspect(connection)
    if _table_exists(inspector, "organization_health_report"):
        op.execute(text("DELETE FROM organization_health_report"))
        if _column_exists(inspector, "organization_health_report", "engagement_id"):
            for fk in inspector.get_foreign_keys("organization_health_report"):
                if "engagement_id" in fk.get("constrained_columns", []):
                    op.drop_constraint(fk["name"], "organization_health_report", type_="foreignkey")
            op.drop_column("organization_health_report", "engagement_id")

        inspector = inspect(connection)
        if not _column_exists(inspector, "organization_health_report", "camp_no"):
            op.add_column("organization_health_report", sa.Column("camp_no", sa.Integer(), nullable=True))

        inspector = inspect(connection)
        if not _column_exists(inspector, "organization_health_report", "camp_report"):
            op.add_column("organization_health_report", sa.Column("camp_report", sa.JSON(), nullable=True))

        inspector = inspect(connection)
        indexes = {idx["name"] for idx in inspector.get_indexes("organization_health_report")}
        if "uq_organization_health_report_org_camp" not in indexes:
            op.create_index(
                "uq_organization_health_report_org_camp",
                "organization_health_report",
                ["organization_id", "camp_no"],
                unique=True,
            )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
