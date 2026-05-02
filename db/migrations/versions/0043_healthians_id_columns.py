"""Add healthians_parameter_id to health_parameters and healthians_camp_id to diagnostic_package.

Revision ID: 0043_healthians_id_columns
Revises: 0042_metsights_sync_flags
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0043_healthians_id_columns"
down_revision = "0042_metsights_sync_flags"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "health_parameters",
        sa.Column("healthians_parameter_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "diagnostic_package",
        sa.Column("healthians_camp_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    """Downgrades are intentionally disabled."""
    raise RuntimeError("Downgrade is not supported")
