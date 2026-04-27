"""Replace simple range columns with risk-level range columns on health_parameters.

Old columns: lower_range_male, higher_range_male, lower_range_female, higher_range_female
New columns (per gender): low_risk_lower_range, low_risk_higher_range,
    moderate_risk_lower_range, moderate_risk_higher_range,
    high_risk_lower_range, high_risk_higher_range

Existing data is migrated into the low_risk columns.

Revision ID: 0039_health_params_risk_ranges
Revises: 0038_diagnostic_package_for
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0039_health_params_risk_ranges"
down_revision = "0038_diagnostic_package_for"


def _has_column(table: str, column: str) -> bool:
    bind = op.get_bind()
    insp = inspect(bind)
    return any(c["name"] == column for c in insp.get_columns(table))


_NEW_COLS = [
    "low_risk_lower_range_male",
    "low_risk_higher_range_male",
    "moderate_risk_lower_range_male",
    "moderate_risk_higher_range_male",
    "high_risk_lower_range_male",
    "high_risk_higher_range_male",
    "low_risk_lower_range_female",
    "low_risk_higher_range_female",
    "moderate_risk_lower_range_female",
    "moderate_risk_higher_range_female",
    "high_risk_lower_range_female",
    "high_risk_higher_range_female",
]

_OLD_COLS = [
    "lower_range_male",
    "higher_range_male",
    "lower_range_female",
    "higher_range_female",
]


def upgrade() -> None:
    for col in _NEW_COLS:
        if not _has_column("health_parameters", col):
            op.add_column(
                "health_parameters",
                sa.Column(col, sa.Numeric(12, 4), nullable=True),
            )

    op.execute(
        """
        UPDATE health_parameters
        SET low_risk_lower_range_male = lower_range_male,
            low_risk_higher_range_male = higher_range_male,
            low_risk_lower_range_female = lower_range_female,
            low_risk_higher_range_female = higher_range_female
        WHERE lower_range_male IS NOT NULL
           OR higher_range_male IS NOT NULL
           OR lower_range_female IS NOT NULL
           OR higher_range_female IS NOT NULL
        """
    )

    for col in _OLD_COLS:
        if _has_column("health_parameters", col):
            op.drop_column("health_parameters", col)


def downgrade() -> None:
    for col in _OLD_COLS:
        if not _has_column("health_parameters", col):
            op.add_column(
                "health_parameters",
                sa.Column(col, sa.Numeric(12, 4), nullable=True),
            )

    op.execute(
        """
        UPDATE health_parameters
        SET lower_range_male = low_risk_lower_range_male,
            higher_range_male = low_risk_higher_range_male,
            lower_range_female = low_risk_lower_range_female,
            higher_range_female = low_risk_higher_range_female
        """
    )

    for col in _NEW_COLS:
        if _has_column("health_parameters", col):
            op.drop_column("health_parameters", col)
