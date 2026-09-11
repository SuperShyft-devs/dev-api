"""Allow admin employees on onboarding_assistant_assignment alongside partners.

Revision ID: 0134_oa_assignment_employee_id
Revises: 0133_partners_employee_auth
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0134_oa_assignment_employee_id"
down_revision = "0133_partners_employee_auth"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "onboarding_assistant_assignment",
        sa.Column("employee_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_oa_assignment_employee_id",
        "onboarding_assistant_assignment",
        "employee",
        ["employee_id"],
        ["employee_id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_onboarding_assistant_assignment_employee_id",
        "onboarding_assistant_assignment",
        ["employee_id"],
    )
    op.create_index(
        "uq_oa_assignment_engagement_employee",
        "onboarding_assistant_assignment",
        ["engagement_id", "employee_id"],
        unique=True,
        postgresql_where=sa.text("employee_id IS NOT NULL"),
    )

    # partner_id was required; employee-only rows need it nullable
    op.alter_column(
        "onboarding_assistant_assignment",
        "partner_id",
        existing_type=sa.Integer(),
        nullable=True,
    )

    op.create_check_constraint(
        "ck_oa_assignment_partner_xor_employee",
        "onboarding_assistant_assignment",
        "(partner_id IS NOT NULL AND employee_id IS NULL) OR "
        "(partner_id IS NULL AND employee_id IS NOT NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_oa_assignment_partner_xor_employee",
        "onboarding_assistant_assignment",
        type_="check",
    )
    op.execute(
        "DELETE FROM onboarding_assistant_assignment WHERE partner_id IS NULL"
    )
    op.alter_column(
        "onboarding_assistant_assignment",
        "partner_id",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.drop_index(
        "uq_oa_assignment_engagement_employee",
        table_name="onboarding_assistant_assignment",
    )
    op.drop_index(
        "ix_onboarding_assistant_assignment_employee_id",
        table_name="onboarding_assistant_assignment",
    )
    op.drop_constraint(
        "fk_oa_assignment_employee_id",
        "onboarding_assistant_assignment",
        type_="foreignkey",
    )
    op.drop_column("onboarding_assistant_assignment", "employee_id")
