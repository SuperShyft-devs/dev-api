"""Add questionnaire_healthy_habit_rules for report overview positive wins."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0028_qnr_healthy_habits"
down_revision = "0027_health_parameters_rename"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "questionnaire_healthy_habit_rules"):
        return

    op.create_table(
        "questionnaire_healthy_habit_rules",
        sa.Column("rule_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("question_id", sa.Integer(), sa.ForeignKey("questionnaire_definitions.question_id", ondelete="CASCADE"), nullable=False),
        sa.Column("habit_key", sa.String(length=200), nullable=True),
        sa.Column("habit_label", sa.String(length=500), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=True),
        sa.Column("condition_type", sa.String(length=50), nullable=False),
        sa.Column("matched_option_values", sa.JSON(), nullable=True),
        sa.Column("scale_min", sa.Numeric(20, 8), nullable=True),
        sa.Column("scale_max", sa.Numeric(20, 8), nullable=True),
        sa.Column("scale_unit", sa.String(length=200), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default=sa.text("'active'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column(
            "updated_employee_id",
            sa.Integer(),
            sa.ForeignKey("employee.employee_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_questionnaire_healthy_habit_rules_question_id",
        "questionnaire_healthy_habit_rules",
        ["question_id"],
    )
    op.create_index(
        "ix_questionnaire_healthy_habit_rules_status",
        "questionnaire_healthy_habit_rules",
        ["status"],
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
