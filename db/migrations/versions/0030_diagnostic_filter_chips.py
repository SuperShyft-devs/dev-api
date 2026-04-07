"""Rename diagnostic filters to filter chips, drop filter_type, add package-chip links."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text
from sqlalchemy.exc import NoSuchTableError


revision = "0030_diag_filter_chips"
down_revision = "0029_payments_tables"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    try:
        for ix in inspector.get_indexes(table_name):
            if ix.get("name") == index_name:
                return True
    except NoSuchTableError:
        return False
    return False


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    # 1) Rename legacy filters table -> filter chips catalog (idempotent)
    if _table_exists(inspector, "diagnostic_package_filters") and not _table_exists(
        inspector, "diagnostic_package_filters_chips"
    ):
        if _index_exists(inspector, "diagnostic_package_filters", "ix_diagnostic_package_filters_status_order"):
            op.drop_index("ix_diagnostic_package_filters_status_order", table_name="diagnostic_package_filters")

        op.rename_table("diagnostic_package_filters", "diagnostic_package_filters_chips")

        op.execute(
            text("ALTER TABLE diagnostic_package_filters_chips RENAME COLUMN filter_id TO filter_chip_id")
        )
        op.execute(
            text("ALTER TABLE diagnostic_package_filters_chips RENAME COLUMN filter_key TO chip_key")
        )
        op.drop_column("diagnostic_package_filters_chips", "filter_type")

        op.execute(
            text(
                "ALTER SEQUENCE IF EXISTS diagnostic_package_filters_filter_id_seq "
                "RENAME TO diagnostic_package_filters_chips_filter_chip_id_seq"
            )
        )

        op.create_index(
            "ix_diagnostic_package_filters_chips_status_order",
            "diagnostic_package_filters_chips",
            ["status", "display_order"],
        )
        op.create_unique_constraint(
            "uq_diagnostic_package_filters_chips_chip_key",
            "diagnostic_package_filters_chips",
            ["chip_key"],
        )

    inspector = inspect(connection)

    # 2) Junction: package <-> filter chip
    if not _table_exists(inspector, "diagnostic_package_filter_chip_links"):
        op.create_table(
            "diagnostic_package_filter_chip_links",
            sa.Column("link_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("diagnostic_package_id", sa.Integer(), nullable=False),
            sa.Column("filter_chip_id", sa.Integer(), nullable=False),
            sa.Column("display_order", sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(
                ["diagnostic_package_id"],
                ["diagnostic_package.diagnostic_package_id"],
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["filter_chip_id"],
                ["diagnostic_package_filters_chips.filter_chip_id"],
                ondelete="CASCADE",
            ),
            sa.UniqueConstraint(
                "diagnostic_package_id",
                "filter_chip_id",
                name="uq_diag_pkg_filter_chip_links_pkg_chip",
            ),
        )
        op.create_index(
            "ix_diag_pkg_filter_chip_links_package_id",
            "diagnostic_package_filter_chip_links",
            ["diagnostic_package_id"],
        )
        op.create_index(
            "ix_diag_pkg_filter_chip_links_chip_id",
            "diagnostic_package_filter_chip_links",
            ["filter_chip_id"],
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
