"""Expand diagnostics schema with package metadata and child entities.

Revision ID: 0009_diagnostics_expansion
Revises: 0008_users_add_age_nullable_dob
Create Date: 2026-03-16
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


revision = "0009_diagnostics_expansion"
down_revision = "0008_users_add_age_nullable_dob"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


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


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "diagnostic_package"):
        return

    # Expand existing diagnostic_package to target structure.
    if not _column_exists(inspector, "diagnostic_package", "report_duration_hours"):
        op.add_column("diagnostic_package", sa.Column("report_duration_hours", sa.Integer(), nullable=True))

    if not _column_exists(inspector, "diagnostic_package", "collection_type"):
        op.add_column("diagnostic_package", sa.Column("collection_type", sa.String(), nullable=True))

    if not _column_exists(inspector, "diagnostic_package", "about_text"):
        op.add_column("diagnostic_package", sa.Column("about_text", sa.Text(), nullable=True))

    if not _column_exists(inspector, "diagnostic_package", "bookings_count"):
        op.add_column(
            "diagnostic_package",
            sa.Column("bookings_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )

    if not _column_exists(inspector, "diagnostic_package", "price"):
        op.add_column("diagnostic_package", sa.Column("price", sa.Numeric(10, 2), nullable=True))

    if not _column_exists(inspector, "diagnostic_package", "original_price"):
        op.add_column("diagnostic_package", sa.Column("original_price", sa.Numeric(10, 2), nullable=True))

    if not _column_exists(inspector, "diagnostic_package", "is_most_popular"):
        op.add_column(
            "diagnostic_package",
            sa.Column("is_most_popular", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )

    if not _column_exists(inspector, "diagnostic_package", "gender_suitability"):
        op.add_column("diagnostic_package", sa.Column("gender_suitability", sa.String(), nullable=True))

    # Backfill and enforce nullable/default rules for key columns.
    connection.execute(
        text(
            """
            UPDATE diagnostic_package
            SET package_name = 'Untitled Package'
            WHERE package_name IS NULL OR trim(package_name) = ''
            """
        )
    )
    op.alter_column("diagnostic_package", "package_name", existing_type=sa.String(), nullable=False)

    connection.execute(
        text(
            """
            UPDATE diagnostic_package
            SET status = 'active'
            WHERE status IS NULL OR trim(status) = ''
            """
        )
    )
    op.alter_column(
        "diagnostic_package",
        "status",
        existing_type=sa.String(),
        nullable=True,
        server_default=sa.text("'active'"),
    )

    # Explicitly ensure defaults are present even when columns pre-exist.
    op.alter_column(
        "diagnostic_package",
        "bookings_count",
        existing_type=sa.Integer(),
        nullable=False,
        server_default=sa.text("0"),
    )
    op.alter_column(
        "diagnostic_package",
        "is_most_popular",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("false"),
    )

    # Add optional check constraints for constrained values.
    if not _constraint_exists(connection, "ck_diagnostic_package_collection_type_allowed"):
        op.create_check_constraint(
            "ck_diagnostic_package_collection_type_allowed",
            "diagnostic_package",
            "collection_type IS NULL OR collection_type IN ('home_collection', 'centre_visit')",
        )
    if not _constraint_exists(connection, "ck_diagnostic_package_gender_allowed"):
        op.create_check_constraint(
            "ck_diagnostic_package_gender_allowed",
            "diagnostic_package",
            "gender_suitability IS NULL OR gender_suitability IN ('male', 'female', 'both')",
        )
    if not _constraint_exists(connection, "ck_diagnostic_package_status_allowed"):
        op.create_check_constraint(
            "ck_diagnostic_package_status_allowed",
            "diagnostic_package",
            "status IS NULL OR status IN ('active', 'inactive')",
        )

    # Table 2: diagnostic_package_filters
    if not _table_exists(inspector, "diagnostic_package_filters"):
        op.create_table(
            "diagnostic_package_filters",
            sa.Column("filter_id", sa.Integer(), primary_key=True),
            sa.Column("filter_key", sa.String(), nullable=False),
            sa.Column("display_name", sa.String(), nullable=False),
            sa.Column("display_order", sa.Integer(), nullable=True),
            sa.Column("filter_type", sa.String(), nullable=True),
            sa.Column("status", sa.String(), nullable=True, server_default=sa.text("'active'")),
        )

    # Table 3: diagnostic_package_reasons
    if not _table_exists(inspector, "diagnostic_package_reasons"):
        op.create_table(
            "diagnostic_package_reasons",
            sa.Column("reason_id", sa.Integer(), primary_key=True),
            sa.Column("diagnostic_package_id", sa.Integer(), nullable=False),
            sa.Column("display_order", sa.Integer(), nullable=True),
            sa.Column("reason_text", sa.Text(), nullable=False),
            sa.ForeignKeyConstraint(
                ["diagnostic_package_id"],
                ["diagnostic_package.diagnostic_package_id"],
                ondelete="CASCADE",
            ),
        )

    # Table 4: diagnostic_package_tags
    if not _table_exists(inspector, "diagnostic_package_tags"):
        op.create_table(
            "diagnostic_package_tags",
            sa.Column("tag_id", sa.Integer(), primary_key=True),
            sa.Column("diagnostic_package_id", sa.Integer(), nullable=False),
            sa.Column("tag_name", sa.String(), nullable=False),
            sa.Column("display_order", sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(
                ["diagnostic_package_id"],
                ["diagnostic_package.diagnostic_package_id"],
                ondelete="CASCADE",
            ),
        )

    # Table 5: diagnostic_test_groups
    if not _table_exists(inspector, "diagnostic_test_groups"):
        op.create_table(
            "diagnostic_test_groups",
            sa.Column("group_id", sa.Integer(), primary_key=True),
            sa.Column("diagnostic_package_id", sa.Integer(), nullable=False),
            sa.Column("group_name", sa.String(), nullable=False),
            sa.Column("test_count", sa.Integer(), nullable=True),
            sa.Column("display_order", sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(
                ["diagnostic_package_id"],
                ["diagnostic_package.diagnostic_package_id"],
                ondelete="CASCADE",
            ),
        )

    # Table 6: diagnostic_tests
    if not _table_exists(inspector, "diagnostic_tests"):
        op.create_table(
            "diagnostic_tests",
            sa.Column("test_id", sa.Integer(), primary_key=True),
            sa.Column("group_id", sa.Integer(), nullable=False),
            sa.Column("test_name", sa.String(), nullable=False),
            sa.Column("display_order", sa.Integer(), nullable=True),
            sa.Column("is_available", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.ForeignKeyConstraint(
                ["group_id"],
                ["diagnostic_test_groups.group_id"],
                ondelete="CASCADE",
            ),
        )

    # Table 7: diagnostic_package_samples
    if not _table_exists(inspector, "diagnostic_package_samples"):
        op.create_table(
            "diagnostic_package_samples",
            sa.Column("sample_id", sa.Integer(), primary_key=True),
            sa.Column("diagnostic_package_id", sa.Integer(), nullable=False),
            sa.Column("sample_type", sa.String(), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("display_order", sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(
                ["diagnostic_package_id"],
                ["diagnostic_package.diagnostic_package_id"],
                ondelete="CASCADE",
            ),
        )

    # Table 8: diagnostic_package_preparations
    if not _table_exists(inspector, "diagnostic_package_preparations"):
        op.create_table(
            "diagnostic_package_preparations",
            sa.Column("preparation_id", sa.Integer(), primary_key=True),
            sa.Column("diagnostic_package_id", sa.Integer(), nullable=False),
            sa.Column("preparation_title", sa.String(), nullable=False),
            sa.Column("steps", sa.JSON(), nullable=True),
            sa.Column("display_order", sa.Integer(), nullable=True),
            sa.ForeignKeyConstraint(
                ["diagnostic_package_id"],
                ["diagnostic_package.diagnostic_package_id"],
                ondelete="CASCADE",
            ),
        )

    # Refresh inspector before index checks after table creation.
    inspector = inspect(connection)

    # Filter/list query indexes.
    if not _index_exists(inspector, "diagnostic_package", "ix_diagnostic_package_status_gender"):
        op.create_index(
            "ix_diagnostic_package_status_gender",
            "diagnostic_package",
            ["status", "gender_suitability"],
        )
    if not _index_exists(inspector, "diagnostic_package_tags", "ix_diagnostic_package_tags_tag_name"):
        op.create_index("ix_diagnostic_package_tags_tag_name", "diagnostic_package_tags", ["tag_name"])
    if not _index_exists(inspector, "diagnostic_package_tags", "ix_diagnostic_package_tags_package_id"):
        op.create_index(
            "ix_diagnostic_package_tags_package_id",
            "diagnostic_package_tags",
            ["diagnostic_package_id"],
        )
    if not _index_exists(inspector, "diagnostic_package_reasons", "ix_diagnostic_package_reasons_package_id"):
        op.create_index(
            "ix_diagnostic_package_reasons_package_id",
            "diagnostic_package_reasons",
            ["diagnostic_package_id"],
        )
    if not _index_exists(inspector, "diagnostic_test_groups", "ix_diagnostic_test_groups_package_id"):
        op.create_index(
            "ix_diagnostic_test_groups_package_id",
            "diagnostic_test_groups",
            ["diagnostic_package_id"],
        )
    if not _index_exists(inspector, "diagnostic_tests", "ix_diagnostic_tests_group_id"):
        op.create_index("ix_diagnostic_tests_group_id", "diagnostic_tests", ["group_id"])
    if not _index_exists(inspector, "diagnostic_package_samples", "ix_diagnostic_package_samples_package_id"):
        op.create_index(
            "ix_diagnostic_package_samples_package_id",
            "diagnostic_package_samples",
            ["diagnostic_package_id"],
        )
    if not _index_exists(
        inspector,
        "diagnostic_package_preparations",
        "ix_diagnostic_package_preparations_package_id",
    ):
        op.create_index(
            "ix_diagnostic_package_preparations_package_id",
            "diagnostic_package_preparations",
            ["diagnostic_package_id"],
        )
    if not _index_exists(inspector, "diagnostic_package_filters", "ix_diagnostic_package_filters_status_order"):
        op.create_index(
            "ix_diagnostic_package_filters_status_order",
            "diagnostic_package_filters",
            ["status", "display_order"],
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
