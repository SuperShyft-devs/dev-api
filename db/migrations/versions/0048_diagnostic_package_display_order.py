"""Add display_order to diagnostic_package.

Revision ID: 0048_dx_pkg_display_order
Revises: 0047_health_areas_covered
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


# NOTE: `alembic_version.version_num` is VARCHAR(32) in this project,
# so revision ids must be <= 32 characters.
revision = "0048_dx_pkg_display_order"
down_revision = "0047_health_areas_covered"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("diagnostic_package", sa.Column("display_order", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("diagnostic_package", "display_order")
