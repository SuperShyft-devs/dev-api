"""Make engagements.diagnostic_package_id NOT NULL.

Rules:
- B2C engagements default to diagnostic_package_id=1 (service layer).
- B2B engagements must be explicitly assigned by admin.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


# NOTE: alembic_version.version_num is VARCHAR(32) in this project.
# Keep revision ids <= 32 chars.
revision = "0021_eng_dx_pkg_not_null"
down_revision = "0020_diag_tests_blood_meta"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "engagements"):
        return
    if not _column_exists(inspector, "engagements", "diagnostic_package_id"):
        return

    # Backfill NULLs so we can safely enforce NOT NULL.
    # Assumes diagnostic_package_id=1 exists (B2C default).
    connection.execute(text("UPDATE engagements SET diagnostic_package_id = 1 WHERE diagnostic_package_id IS NULL"))

    op.alter_column(
        "engagements",
        "diagnostic_package_id",
        existing_type=sa.Integer(),
        nullable=False,
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")

