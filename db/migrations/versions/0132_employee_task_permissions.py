"""Add optional task-level Inferior Admin grants.

Revision ID: 0132_task_permissions
Revises: 0131_employee_permissions
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0132_task_permissions"
down_revision = "0131_employee_permissions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "employee_task_permissions",
        sa.Column(
            "employee_id",
            sa.Integer(),
            sa.ForeignKey("employee.employee_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "category_key",
            sa.String(length=50),
            sa.ForeignKey("permission_categories.category_key", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("task_key", sa.String(length=80), primary_key=True),
        sa.Column("can_view", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("can_edit", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "granted_by_employee_id",
            sa.Integer(),
            sa.ForeignKey("employee.employee_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "NOT can_edit OR can_view",
            name="ck_employee_task_permissions_edit_requires_view",
        ),
    )
    op.create_index(
        "ix_employee_task_permissions_employee_id",
        "employee_task_permissions",
        ["employee_id"],
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
