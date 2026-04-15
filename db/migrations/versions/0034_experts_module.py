"""Experts module: doctors and nutritionists, tags, reviews.

Revision ID: 0034_experts_module
Revises: 0033_engagements_kind_enum
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0034_experts_module"
down_revision = "0033_engagements_kind_enum"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()

    inspector = inspect(connection)
    if not _table_exists(inspector, "experts"):
        op.create_table(
            "experts",
            sa.Column("expert_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True),
            sa.Column("expert_type", sa.String(), nullable=False),
            sa.Column("display_name", sa.String(), nullable=False),
            sa.Column("profile_photo", sa.String(), nullable=True),
            sa.Column("rating", sa.Numeric(3, 2), nullable=False, server_default="0"),
            sa.Column("review_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("patient_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("experience_years", sa.Integer(), nullable=True),
            sa.Column("qualifications", sa.String(), nullable=True),
            sa.Column("about_text", sa.Text(), nullable=True),
            sa.Column("consultation_modes", sa.JSON(), nullable=True),
            sa.Column("languages", sa.JSON(), nullable=True),
            sa.Column("session_duration_mins", sa.Integer(), nullable=True),
            sa.Column("appointment_fee_paise", sa.Integer(), nullable=True),
            sa.Column("original_fee_paise", sa.Integer(), nullable=True),
            sa.Column("status", sa.String(), nullable=False, server_default="active"),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
        )

    inspector = inspect(connection)
    if not _table_exists(inspector, "expert_expertise_tags"):
        op.create_table(
            "expert_expertise_tags",
            sa.Column("tag_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("expert_id", sa.Integer(), sa.ForeignKey("experts.expert_id", ondelete="CASCADE"), nullable=False),
            sa.Column("tag_name", sa.String(), nullable=False),
            sa.Column("display_order", sa.Integer(), nullable=True),
        )
        op.create_index("ix_expert_expertise_tags_expert_id", "expert_expertise_tags", ["expert_id"])

    inspector = inspect(connection)
    if not _table_exists(inspector, "expert_reviews"):
        op.create_table(
            "expert_reviews",
            sa.Column("review_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("expert_id", sa.Integer(), sa.ForeignKey("experts.expert_id", ondelete="CASCADE"), nullable=False),
            sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False),
            sa.Column("rating", sa.Numeric(2, 1), nullable=False),
            sa.Column("review_text", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        )
        op.create_index("ix_expert_reviews_expert_id", "expert_reviews", ["expert_id"])
        op.create_unique_constraint("uq_expert_review_per_user", "expert_reviews", ["expert_id", "user_id"])


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
