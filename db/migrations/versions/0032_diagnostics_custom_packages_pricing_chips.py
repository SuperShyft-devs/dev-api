"""Diagnostics: custom packages, pricing on groups/tests, chip_for, group filter links.

Revision ID: 0032_diag_custom_pkg
Revises: 0031_order_bookings
Create Date: 2026-04-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0032_diag_custom_pkg"
down_revision = "0031_order_bookings"
branch_labels = None
depends_on = None


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
    return any(idx.get("name") == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "diagnostic_package"):
        if not _column_exists(inspector, "diagnostic_package", "created_by_user_id"):
            op.add_column(
                "diagnostic_package",
                sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            )
            op.create_foreign_key(
                "fk_diagnostic_package_created_by_user_id",
                "diagnostic_package",
                "users",
                ["created_by_user_id"],
                ["user_id"],
                ondelete="SET NULL",
            )
        if _column_exists(inspector, "diagnostic_package", "no_of_tests"):
            op.drop_column("diagnostic_package", "no_of_tests")

    inspector = inspect(connection)

    if _table_exists(inspector, "diagnostic_test_groups"):
        if not _column_exists(inspector, "diagnostic_test_groups", "price"):
            op.add_column("diagnostic_test_groups", sa.Column("price", sa.Numeric(10, 2), nullable=True))
        if not _column_exists(inspector, "diagnostic_test_groups", "original_price"):
            op.add_column("diagnostic_test_groups", sa.Column("original_price", sa.Numeric(10, 2), nullable=True))
        if not _column_exists(inspector, "diagnostic_test_groups", "is_most_popular"):
            op.add_column(
                "diagnostic_test_groups",
                sa.Column("is_most_popular", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            )
        if not _column_exists(inspector, "diagnostic_test_groups", "gender_suitability"):
            op.add_column("diagnostic_test_groups", sa.Column("gender_suitability", sa.String(), nullable=True))
        if not _constraint_exists(connection, "ck_diagnostic_test_groups_gender_allowed"):
            op.create_check_constraint(
                "ck_diagnostic_test_groups_gender_allowed",
                "diagnostic_test_groups",
                "gender_suitability IS NULL OR gender_suitability IN ('male', 'female', 'both')",
            )

    inspector = inspect(connection)

    if _table_exists(inspector, "health_parameters"):
        if not _column_exists(inspector, "health_parameters", "price"):
            op.add_column("health_parameters", sa.Column("price", sa.Numeric(10, 2), nullable=True))
        if not _column_exists(inspector, "health_parameters", "original_price"):
            op.add_column("health_parameters", sa.Column("original_price", sa.Numeric(10, 2), nullable=True))
        if not _column_exists(inspector, "health_parameters", "is_most_popular"):
            op.add_column(
                "health_parameters",
                sa.Column("is_most_popular", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            )
        if not _column_exists(inspector, "health_parameters", "gender_suitability"):
            op.add_column("health_parameters", sa.Column("gender_suitability", sa.String(), nullable=True))
        if not _constraint_exists(connection, "ck_health_parameters_diag_gender_allowed"):
            op.create_check_constraint(
                "ck_health_parameters_diag_gender_allowed",
                "health_parameters",
                "gender_suitability IS NULL OR gender_suitability IN ('male', 'female', 'both')",
            )

    inspector = inspect(connection)

    if _table_exists(inspector, "diagnostic_package_filters_chips"):
        if not _column_exists(inspector, "diagnostic_package_filters_chips", "chip_for"):
            op.add_column(
                "diagnostic_package_filters_chips",
                sa.Column(
                    "chip_for",
                    sa.String(32),
                    nullable=False,
                    server_default=sa.text("'public_package'"),
                ),
            )
        if not _constraint_exists(connection, "ck_diagnostic_filters_chips_chip_for_allowed"):
            op.create_check_constraint(
                "ck_diagnostic_filters_chips_chip_for_allowed",
                "diagnostic_package_filters_chips",
                "chip_for IN ('public_package', 'custom_package')",
            )

    if _table_exists(inspector, "diagnostic_package_filter_chip_links"):
        if _constraint_exists(connection, "uq_diag_pkg_filter_chip_links_pkg_chip"):
            op.drop_constraint(
                "uq_diag_pkg_filter_chip_links_pkg_chip",
                "diagnostic_package_filter_chip_links",
                type_="unique",
            )

        op.alter_column(
            "diagnostic_package_filter_chip_links",
            "diagnostic_package_id",
            existing_type=sa.Integer(),
            nullable=True,
        )

        inspector = inspect(connection)
        if not _column_exists(inspector, "diagnostic_package_filter_chip_links", "group_id"):
            op.add_column(
                "diagnostic_package_filter_chip_links",
                sa.Column("group_id", sa.Integer(), nullable=True),
            )
            op.create_foreign_key(
                "fk_diag_filter_chip_links_group_id",
                "diagnostic_package_filter_chip_links",
                "diagnostic_test_groups",
                ["group_id"],
                ["group_id"],
                ondelete="CASCADE",
            )

        if not _constraint_exists(connection, "ck_diag_filter_chip_links_pkg_xor_group"):
            op.create_check_constraint(
                "ck_diag_filter_chip_links_pkg_xor_group",
                "diagnostic_package_filter_chip_links",
                "(diagnostic_package_id IS NOT NULL AND group_id IS NULL) OR "
                "(diagnostic_package_id IS NULL AND group_id IS NOT NULL)",
            )

        inspector = inspect(connection)
        if not _index_exists(inspector, "diagnostic_package_filter_chip_links", "uq_diag_filter_chip_links_pkg_chip"):
            op.create_index(
                "uq_diag_filter_chip_links_pkg_chip",
                "diagnostic_package_filter_chip_links",
                ["diagnostic_package_id", "filter_chip_id"],
                unique=True,
                postgresql_where=sa.text("diagnostic_package_id IS NOT NULL"),
            )
        if not _index_exists(inspector, "diagnostic_package_filter_chip_links", "uq_diag_filter_chip_links_grp_chip"):
            op.create_index(
                "uq_diag_filter_chip_links_grp_chip",
                "diagnostic_package_filter_chip_links",
                ["group_id", "filter_chip_id"],
                unique=True,
                postgresql_where=sa.text("group_id IS NOT NULL"),
            )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
