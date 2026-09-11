"""Rewire admin phlebo-partner OA assignments back to employee_id.

During 0133, staff admins who were on onboarding_assistant_assignment were
copied into partners(role=phlebo). Restore those rows as employee assignees.

Revision ID: 0135_fix_admin_oa_assignees
Revises: 0134_oa_assignment_employee_id
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0135_fix_admin_oa_assignees"
down_revision = "0134_oa_assignment_employee_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Convert phlebo-partner assignments that match admin/inferior_admin employees
    # (by phone or email) into employee_id XOR rows.
    conn.execute(
        sa.text(
            """
            UPDATE onboarding_assistant_assignment a
            SET
              employee_id = e.employee_id,
              partner_id = NULL
            FROM partners p
            JOIN employee e
              ON e.role::text IN ('admin', 'inferior_admin')
             AND e.status = 'active'
             AND (
               (p.phone IS NOT NULL AND e.phone IS NOT NULL AND btrim(p.phone) <> '' AND p.phone = e.phone)
               OR (
                 p.email IS NOT NULL AND e.email IS NOT NULL
                 AND btrim(p.email) <> ''
                 AND lower(btrim(p.email)) = lower(btrim(e.email))
               )
             )
            WHERE a.partner_id = p.partner_id
              AND a.employee_id IS NULL
              AND p.role = 'phlebo'
              AND NOT EXISTS (
                SELECT 1
                FROM onboarding_assistant_assignment a2
                WHERE a2.engagement_id = a.engagement_id
                  AND a2.employee_id = e.employee_id
              )
            """
        )
    )

    # Drop duplicate phlebo partners that only existed as mirrors of staff admins
    # (no remaining assignments, not linked to experts).
    conn.execute(
        sa.text(
            """
            DELETE FROM partners p
            WHERE p.role = 'phlebo'
              AND EXISTS (
                SELECT 1 FROM employee e
                WHERE e.role::text IN ('admin', 'inferior_admin')
                  AND e.status = 'active'
                  AND (
                    (p.phone IS NOT NULL AND e.phone IS NOT NULL AND p.phone = e.phone)
                    OR (
                      p.email IS NOT NULL AND e.email IS NOT NULL
                      AND lower(btrim(p.email)) = lower(btrim(e.email))
                    )
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM onboarding_assistant_assignment a
                WHERE a.partner_id = p.partner_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM experts ex WHERE ex.partner_id = p.partner_id
              )
            """
        )
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported for 0135_fix_admin_oa_assignees")
