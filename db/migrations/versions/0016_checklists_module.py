"""Checklists module tables.

Revision ID: 0016_checklists_module
Revises: 0015_qnr_visibility_prefill
Create Date: 2026-03-22
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0016_checklists_module"
down_revision = "0015_qnr_visibility_prefill"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "checklist_templates",
        sa.Column("template_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_employee_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["created_employee_id"],
            ["employee.employee_id"],
            ondelete="SET NULL",
        ),
    )

    op.create_table(
        "checklist_template_items",
        sa.Column("item_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("template_id", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("display_order", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["template_id"],
            ["checklist_templates.template_id"],
            ondelete="CASCADE",
        ),
    )

    op.create_table(
        "engagement_checklists",
        sa.Column("checklist_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("engagement_id", sa.Integer(), nullable=False),
        sa.Column("template_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_employee_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["engagement_id"],
            ["engagements.engagement_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["template_id"],
            ["checklist_templates.template_id"],
        ),
        sa.ForeignKeyConstraint(
            ["created_employee_id"],
            ["employee.employee_id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("engagement_id", "template_id", name="uq_engagement_checklists_engagement_template"),
    )

    op.create_table(
        "engagement_checklist_tasks",
        sa.Column("task_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("checklist_id", sa.Integer(), nullable=False),
        sa.Column("item_id", sa.Integer(), nullable=False),
        sa.Column("assigned_employee_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("due_date", sa.Date(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_by_employee_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["checklist_id"],
            ["engagement_checklists.checklist_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["item_id"],
            ["checklist_template_items.item_id"],
        ),
        sa.ForeignKeyConstraint(
            ["assigned_employee_id"],
            ["employee.employee_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["completed_by_employee_id"],
            ["employee.employee_id"],
            ondelete="SET NULL",
        ),
    )

    op.create_index(None, "engagement_checklist_tasks", ["checklist_id"])
    op.create_index(None, "engagement_checklist_tasks", ["assigned_employee_id"])
    op.create_index(None, "engagement_checklists", ["engagement_id"])


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
