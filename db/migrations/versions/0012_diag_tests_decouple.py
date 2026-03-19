"""Decouple diagnostic tests and groups from packages.

Revision ID: 0012_diag_tests_decouple
Revises: 0011_users_profile_photo
Create Date: 2026-03-19
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0012_diag_tests_decouple"
down_revision = "0011_users_profile_photo"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: Drop old package-owned tables.
    op.execute("DROP TABLE IF EXISTS diagnostic_tests CASCADE;")
    op.execute("DROP TABLE IF EXISTS diagnostic_test_groups CASCADE;")

    # Step 2: Create standalone groups table.
    op.create_table(
        "diagnostic_test_groups",
        sa.Column("group_id", sa.Integer(), primary_key=True),
        sa.Column("group_name", sa.String(), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=True),
    )

    # Step 3: Create standalone tests table.
    op.create_table(
        "diagnostic_tests",
        sa.Column("test_id", sa.Integer(), primary_key=True),
        sa.Column("test_name", sa.String(), nullable=False),
        sa.Column("is_available", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("display_order", sa.Integer(), nullable=True),
    )

    # Step 4: Create join table for tests assigned to groups.
    op.create_table(
        "diagnostic_test_group_tests",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column("test_id", sa.Integer(), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["group_id"], ["diagnostic_test_groups.group_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["test_id"], ["diagnostic_tests.test_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("group_id", "test_id", name="uq_diagnostic_test_group_tests_group_test"),
    )

    # Step 5: Create join table for groups assigned to packages.
    op.create_table(
        "diagnostic_package_test_groups",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("diagnostic_package_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["diagnostic_package_id"],
            ["diagnostic_package.diagnostic_package_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["group_id"], ["diagnostic_test_groups.group_id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "diagnostic_package_id",
            "group_id",
            name="uq_diagnostic_package_test_groups_package_group",
        ),
    )

    # Supporting indexes for assignment and read paths.
    op.create_index(
        "ix_diagnostic_test_group_tests_group_order",
        "diagnostic_test_group_tests",
        ["group_id", "display_order"],
    )
    op.create_index(
        "ix_diagnostic_test_group_tests_test_id",
        "diagnostic_test_group_tests",
        ["test_id"],
    )
    op.create_index(
        "ix_diagnostic_package_test_groups_package_order",
        "diagnostic_package_test_groups",
        ["diagnostic_package_id", "display_order"],
    )
    op.create_index(
        "ix_diagnostic_package_test_groups_group_id",
        "diagnostic_package_test_groups",
        ["group_id"],
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
