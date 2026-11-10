"""Employee departments M2M, notification recipient migration, drop default OA IDs."""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text
from sqlalchemy.dialects import postgresql

revision = "0193_employee_departments"
down_revision = "0192_integration_provider"
branch_labels = None
depends_on = None

_DEPARTMENT_ENUM = "employee_department_enum"
_TABLE = "employee_department"
_PLATFORM_COL = "default_onboarding_assistant_employee_ids"


def _parse_ids(raw: str | None) -> list[int]:
    if not raw:
        return []
    out: list[int] = []
    for part in re.split(r"[,;\s]+", raw.strip()):
        if not part:
            continue
        try:
            val = int(part)
        except ValueError:
            continue
        if val > 0 and val not in out:
            out.append(val)
    return out


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    op.execute(
        text(
            f"""
            DO $$ BEGIN
                CREATE TYPE {_DEPARTMENT_ENUM} AS ENUM ('tech', 'design', 'ops');
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            """
        )
    )
    dept_enum = postgresql.ENUM(
        "tech", "design", "ops", name=_DEPARTMENT_ENUM, create_type=False
    )

    if _TABLE not in inspector.get_table_names():
        op.create_table(
            _TABLE,
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "employee_id",
                sa.Integer(),
                sa.ForeignKey("employee.employee_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("department", dept_enum, nullable=False),
            sa.UniqueConstraint("employee_id", "department", name="uq_employee_department"),
        )
        op.create_index("ix_employee_department_department", _TABLE, ["department"])

    if "notification_events" in inspector.get_table_names():
        op.execute(
            text(
                "UPDATE notification_events SET recipient_type = 'departments.ops' "
                "WHERE recipient_type IN ('employees', 'admin')"
            )
        )

    if "platform_settings" in inspector.get_table_names():
        cols = {c["name"] for c in inspector.get_columns("platform_settings")}
        if _PLATFORM_COL in cols:
            row = connection.execute(
                text(
                    f"SELECT {_PLATFORM_COL} FROM platform_settings "
                    "WHERE settings_id = 1 LIMIT 1"
                )
            ).fetchone()
            raw = row[0] if row else None
            for eid in _parse_ids(raw):
                connection.execute(
                    text(
                        f"""
                        INSERT INTO {_TABLE} (employee_id, department)
                        VALUES (:eid, 'ops')
                        ON CONFLICT (employee_id, department) DO NOTHING
                        """
                    ),
                    {"eid": eid},
                )
            op.drop_column("platform_settings", _PLATFORM_COL)


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if "platform_settings" in inspector.get_table_names():
        cols = {c["name"] for c in inspector.get_columns("platform_settings")}
        if _PLATFORM_COL not in cols:
            op.add_column(
                "platform_settings",
                sa.Column(_PLATFORM_COL, sa.String(length=500), nullable=True),
            )

    if _TABLE in inspector.get_table_names():
        op.drop_index("ix_employee_department_department", table_name=_TABLE)
        op.drop_table(_TABLE)

    sa.Enum(name=_DEPARTMENT_ENUM).drop(connection, checkfirst=True)
