"""Create normalized Inferior Admin category grants.

Revision ID: 0131_employee_permissions
Revises: 0130_inferior_admin_role
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


revision = "0131_employee_permissions"
down_revision = "0130_inferior_admin_role"
branch_labels = None
depends_on = None


CATEGORIES = (
    ("users", "Users", "Users and participant journeys"),
    ("organizations", "Organizations", "Organizations, industries, and camps"),
    ("engagements", "Engagements", "Engagement administration"),
    ("engagement_console", "Engagement Console", "Scoped engagement operations"),
    ("assessments", "Assessments", "Assessment and questionnaire libraries"),
    ("diagnostics", "Diagnostics", "Diagnostic packages and health parameters"),
    ("reports", "Reports", "Reports, exports, and report jobs"),
    ("experts", "Experts", "Expert administration"),
    ("payments_bookings", "Payments & Bookings", "Payment and booking administration"),
    ("notifications", "Notifications", "Notifications and notification events"),
    ("checklists_tasks", "Checklists & Tasks", "Checklist templates and tasks"),
    ("support", "Support", "Support ticket administration"),
    ("employees", "Employees", "Non-privileged employee administration"),
    ("platform_settings", "Platform Settings", "Platform and engagement type settings"),
    ("system_monitoring", "System Monitoring", "Health, audit, backup, and maintenance"),
)


def upgrade() -> None:
    connection = op.get_bind()
    duplicates = connection.execute(
        text(
            "SELECT user_id, COUNT(*) AS count FROM employee "
            "GROUP BY user_id HAVING COUNT(*) > 1 ORDER BY user_id"
        )
    ).fetchall()
    if duplicates:
        details = ", ".join(f"user_id={row[0]} count={row[1]}" for row in duplicates)
        raise RuntimeError(
            "Cannot add employee.user_id uniqueness; resolve duplicate employee rows first: "
            + details
        )

    op.add_column(
        "employee",
        sa.Column("permissions_version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.create_unique_constraint("uq_employee_user_id", "employee", ["user_id"])

    op.create_table(
        "permission_categories",
        sa.Column("category_key", sa.String(length=50), primary_key=True),
        sa.Column("display_name", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=False, unique=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.create_index(
        "ix_permission_categories_active_order",
        "permission_categories",
        ["is_active", "display_order"],
    )

    category_table = sa.table(
        "permission_categories",
        sa.column("category_key", sa.String),
        sa.column("display_name", sa.String),
        sa.column("description", sa.String),
        sa.column("display_order", sa.Integer),
        sa.column("is_active", sa.Boolean),
    )
    op.bulk_insert(
        category_table,
        [
            {
                "category_key": key,
                "display_name": name,
                "description": description,
                "display_order": position,
                "is_active": True,
            }
            for position, (key, name, description) in enumerate(CATEGORIES, start=1)
        ],
    )

    op.create_table(
        "employee_category_permissions",
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
            name="ck_employee_category_permissions_edit_requires_view",
        ),
    )
    op.create_index(
        "ix_employee_category_permissions_employee_id",
        "employee_category_permissions",
        ["employee_id"],
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
