"""Idempotent support_tickets resolution columns when 0185 was not applied on DB."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0194_support_resolution_fixup"
down_revision = "0193_employee_departments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
            ALTER TABLE support_tickets
            ADD COLUMN IF NOT EXISTS resolved_by_employee_id INTEGER
            """
        )
    )
    op.execute(
        text(
            """
            ALTER TABLE support_tickets
            ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ
            """
        )
    )
    op.execute(
        text(
            """
            ALTER TABLE support_tickets
            ADD COLUMN IF NOT EXISTS resolution_notes TEXT
            """
        )
    )
    op.execute(
        text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'support_tickets_resolved_by_employee_id_fkey'
                ) THEN
                    ALTER TABLE support_tickets
                    ADD CONSTRAINT support_tickets_resolved_by_employee_id_fkey
                    FOREIGN KEY (resolved_by_employee_id)
                    REFERENCES employee (employee_id)
                    ON DELETE SET NULL;
                END IF;
            END $$;
            """
        )
    )


def downgrade() -> None:
    pass
