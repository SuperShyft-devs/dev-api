"""employee_role_enum

Convert employee.role from free-form String to a PostgreSQL enum
with values: admin, onboarding_assistant.

Revision ID: 0061_employee_role_enum
Revises: 0060_category_key_category_of
Create Date: 2026-06-22 22:45:00.000000

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import ENUM


revision = "0061_employee_role_enum"
down_revision = "0060_category_key_category_of"
branch_labels = None
depends_on = None


def _table_exists(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "employee"):
        return

    employee_role = ENUM(
        "admin",
        "onboarding_assistant",
        name="employee_role",
        create_type=True,
    )
    employee_role.create(connection, checkfirst=True)

    op.execute(
        text(
            "ALTER TABLE employee ALTER COLUMN role TYPE employee_role "
            "USING (LOWER(TRIM(role)))::employee_role"
        )
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
