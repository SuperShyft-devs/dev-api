"""Create platform_settings for B2C onboarding defaults."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0022_platform_settings"
down_revision = "0021_eng_dx_pkg_not_null"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "platform_settings"):
        return

    op.create_table(
        "platform_settings",
        sa.Column("settings_id", sa.Integer(), primary_key=True),
        sa.Column("b2c_default_assessment_package_id", sa.Integer(), nullable=False),
        sa.Column("b2c_default_diagnostic_package_id", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["b2c_default_assessment_package_id"],
            ["assessment_packages.package_id"],
        ),
        sa.ForeignKeyConstraint(
            ["b2c_default_diagnostic_package_id"],
            ["diagnostic_package.diagnostic_package_id"],
        ),
        sa.ForeignKeyConstraint(
            ["updated_by_user_id"],
            ["users.user_id"],
            ondelete="SET NULL",
        ),
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
