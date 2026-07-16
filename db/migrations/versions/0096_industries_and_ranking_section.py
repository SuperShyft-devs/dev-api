"""Create industries table, add industry_key to organizations, seed ranking section.

Revision ID: 0096_industries_and_ranking
Revises: 0095_participant_consult_done
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0096_industries_and_ranking"
down_revision = "0095_participant_consult_done"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


_INDUSTRIES = [
    ("information_technology", "Information Technology"),
    ("manufacturing", "Manufacturing"),
    ("healthcare", "Healthcare"),
    ("fmcg", "FMCG"),
    ("bfsi", "BFSI"),
    ("education", "Education"),
    ("retail", "Retail"),
    ("logistics", "Logistics"),
    ("real_estate", "Real Estate"),
    ("hospitality", "Hospitality"),
    ("pharma", "Pharma"),
    ("media_entertainment", "Media & Entertainment"),
    ("other", "Other"),
]


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    # 1. Create industries table
    if not _table_exists(inspector, "industries"):
        op.create_table(
            "industries",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("industry_key", sa.String(100), nullable=False, unique=True),
            sa.Column("industry", sa.String(100), nullable=False),
        )
        # Seed industries
        for industry_key, industry in _INDUSTRIES:
            connection.execute(
                text(
                    "INSERT INTO industries (industry_key, industry) VALUES (:k, :v)"
                    " ON CONFLICT (industry_key) DO NOTHING"
                ),
                {"k": industry_key, "v": industry},
            )

    # 2. Add industry_key column to organizations
    if not _column_exists(inspector, "organizations", "industry_key"):
        op.add_column(
            "organizations",
            sa.Column(
                "industry_key",
                sa.String(100),
                sa.ForeignKey("industries.industry_key"),
                nullable=True,
            ),
        )

    # 3. Seed ranking row in camp_report_sections
    connection.execute(
        text(
            """
            INSERT INTO camp_report_sections (section_key, section, description)
            VALUES (
                'ranking',
                'Ranking',
                'Organization health rank in its city based on average metabolic risk score of camp participants.'
            )
            ON CONFLICT (section_key) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
