"""Add display_order to diagnostic_package.

Revision ID: 0048_diagnostic_package_display_order
Revises: 0047_health_areas_covered
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0048_diagnostic_package_display_order"
down_revision = "0047_health_areas_covered"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("diagnostic_package", sa.Column("display_order", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("diagnostic_package", "display_order")
