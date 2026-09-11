"""Move organization_manager from employees to partners.

- Expand partners.role CHECK to allow organization_manager
- Backfill partners from organization_manager employees (reuse by phone/email)
- Remap organizations.contact_person_user_ids employee_id → partner_id
- Null FKs pointing at those employees, then delete the employee rows
- Do NOT remove organization_manager from PG employee_role enum

Revision ID: 0136_org_manager_partners
Revises: 0135_fix_admin_oa_assignees
"""

from __future__ import annotations

import json

from alembic import op
import sqlalchemy as sa


revision = "0136_org_manager_partners"
down_revision = "0135_fix_admin_oa_assignees"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # ── partners.role CHECK: allow organization_manager ───────────
    op.drop_constraint("ck_partners_role", "partners", type_="check")
    op.create_check_constraint(
        "ck_partners_role",
        "partners",
        "role IN ('phlebo', 'expert', 'organization_manager')",
    )

    # ── backfill partners from organization_manager employees ─────
    emp_rows = conn.execute(
        sa.text(
            """
            SELECT employee_id, name, phone, email, status
            FROM employee
            WHERE role::text = 'organization_manager'
            ORDER BY employee_id
            """
        )
    ).fetchall()

    emp_to_partner: dict[int, int] = {}
    for employee_id, name, phone, email, status in emp_rows:
        phone_val = phone if phone and str(phone).strip() else None
        email_val = email.strip().lower() if email and str(email).strip() else None
        name_val = (name or "").strip() or f"Org Manager {employee_id}"
        status_val = (status or "active").strip().lower() or "active"
        if status_val not in {"active", "inactive", "archived"}:
            status_val = "active"
        if not phone_val and not email_val:
            phone_val = f"migrated-org-manager-{employee_id}"

        existing = None
        if phone_val:
            existing = conn.execute(
                sa.text("SELECT partner_id, role FROM partners WHERE phone = :phone"),
                {"phone": phone_val},
            ).fetchone()
        if existing is None and email_val:
            existing = conn.execute(
                sa.text(
                    "SELECT partner_id, role FROM partners WHERE lower(btrim(email)) = :email"
                ),
                {"email": email_val},
            ).fetchone()

        if existing:
            partner_id = int(existing[0])
            conn.execute(
                sa.text(
                    """
                    UPDATE partners
                    SET role = 'organization_manager',
                        status = CASE
                          WHEN lower(status) = 'active' THEN :status
                          ELSE status
                        END,
                        name = COALESCE(NULLIF(btrim(name), ''), :name)
                    WHERE partner_id = :pid
                    """
                ),
                {"pid": partner_id, "status": status_val, "name": name_val},
            )
        else:
            partner_id = conn.execute(
                sa.text(
                    """
                    INSERT INTO partners (name, phone, email, role, status)
                    VALUES (:name, :phone, :email, 'organization_manager', :status)
                    RETURNING partner_id
                    """
                ),
                {
                    "name": name_val,
                    "phone": phone_val,
                    "email": email_val,
                    "status": status_val,
                },
            ).scalar()
            partner_id = int(partner_id)

        emp_to_partner[int(employee_id)] = partner_id

    # ── remap contact_person_user_ids employee_id → partner_id ────
    org_rows = conn.execute(
        sa.text(
            "SELECT organization_id, contact_person_user_ids FROM organizations WHERE contact_person_user_ids IS NOT NULL"
        )
    ).fetchall()
    for org_id, raw in org_rows:
        data = raw if isinstance(raw, dict) else json.loads(raw) if isinstance(raw, str) else None
        if not isinstance(data, dict):
            continue

        def map_id(uid: int) -> int | None:
            if uid in emp_to_partner:
                return emp_to_partner[uid]
            # Already a partner id, or unknown leftover — keep if partner exists
            row = conn.execute(
                sa.text(
                    "SELECT partner_id FROM partners WHERE partner_id = :pid AND role = 'organization_manager'"
                ),
                {"pid": uid},
            ).fetchone()
            if row:
                return int(row[0])
            return None

        def map_list(vals):
            out: list[int] = []
            if not isinstance(vals, list):
                return out
            seen: set[int] = set()
            for v in vals:
                try:
                    mapped = map_id(int(v))
                except (TypeError, ValueError):
                    continue
                if mapped is not None and mapped not in seen:
                    seen.add(mapped)
                    out.append(mapped)
            return out

        new_data: dict = {}
        for key, val in data.items():
            if key == "organization_managers":
                new_data[key] = map_list(val)
            elif isinstance(val, dict):
                city: dict = {}
                for ck, cv in val.items():
                    city[ck] = map_list(cv) if isinstance(cv, list) else cv
                new_data[key] = city
            else:
                new_data[key] = val

        conn.execute(
            sa.text(
                """
                UPDATE organizations
                SET contact_person_user_ids = CAST(:data AS json)
                WHERE organization_id = :oid
                """
            ),
            {"data": json.dumps(new_data), "oid": org_id},
        )

    if not emp_to_partner:
        return

    emp_ids = list(emp_to_partner.keys())

    # ── null FKs pointing at organization_manager employees ───────
    for table, column in (
        ("organizations", "created_employee_id"),
        ("organizations", "updated_employee_id"),
        ("organizations", "bd_employee_id"),
        ("engagement_checklist_tasks", "assigned_employee_id"),
        ("engagement_checklist_tasks", "completed_by_employee_id"),
        ("engagement_checklists", "created_employee_id"),
        ("checklist_templates", "created_employee_id"),
        ("questionnaire_healthy_habit_rules", "updated_employee_id"),
        ("employee_category_permissions", "granted_by_employee_id"),
        ("employee_task_permissions", "granted_by_employee_id"),
        ("onboarding_assistant_assignment", "employee_id"),
    ):
        conn.execute(
            sa.text(
                f"""
                UPDATE {table}
                SET {column} = NULL
                WHERE {column} = ANY(:ids)
                """
            ),
            {"ids": emp_ids},
        )

    conn.execute(
        sa.text(
            """
            DELETE FROM employee_task_permissions
            WHERE employee_id = ANY(:ids)
            """
        ),
        {"ids": emp_ids},
    )
    conn.execute(
        sa.text(
            """
            DELETE FROM employee_category_permissions
            WHERE employee_id = ANY(:ids)
            """
        ),
        {"ids": emp_ids},
    )
    conn.execute(
        sa.text(
            """
            DELETE FROM employee_auth_otp_sessions
            WHERE employee_id = ANY(:ids)
            """
        ),
        {"ids": emp_ids},
    )
    conn.execute(
        sa.text(
            """
            DELETE FROM employee_auth_tokens
            WHERE employee_id = ANY(:ids)
            """
        ),
        {"ids": emp_ids},
    )
    conn.execute(
        sa.text(
            """
            DELETE FROM employee
            WHERE employee_id = ANY(:ids)
              AND role::text = 'organization_manager'
            """
        ),
        {"ids": emp_ids},
    )


def downgrade() -> None:
    # Irreversible data migration; restore CHECK only for schema symmetry.
    op.drop_constraint("ck_partners_role", "partners", type_="check")
    op.create_check_constraint(
        "ck_partners_role",
        "partners",
        "role IN ('phlebo', 'expert')",
    )
