"""Partners table, employee/partner auth, detach employee from users.

Revision ID: 0133_partners_employee_auth
Revises: 0132_task_permissions
"""

from __future__ import annotations

import json

from alembic import op
import sqlalchemy as sa


revision = "0133_partners_employee_auth"
down_revision = "0132_task_permissions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── partners ──────────────────────────────────────────────────
    op.create_table(
        "partners",
        sa.Column("partner_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("phone", sa.String(), nullable=True),
        sa.Column("email", sa.String(), nullable=True),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="active"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "(phone IS NOT NULL AND btrim(phone) <> '') OR (email IS NOT NULL AND btrim(email) <> '')",
            name="ck_partners_phone_or_email",
        ),
        sa.CheckConstraint(
            "role IN ('phlebo', 'expert')",
            name="ck_partners_role",
        ),
    )
    op.create_index("ix_partners_phone", "partners", ["phone"], unique=True)
    op.create_index("ix_partners_email", "partners", ["email"], unique=True)
    op.create_index("ix_partners_role", "partners", ["role"])
    op.create_index("ix_partners_status", "partners", ["status"])

    # ── partner auth ──────────────────────────────────────────────
    op.create_table(
        "partner_auth_otp_sessions",
        sa.Column("session_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "partner_id",
            sa.Integer(),
            sa.ForeignKey("partners.partner_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("otp_hash", sa.String(), nullable=False),
        sa.Column("otp_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("failed_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index("ix_partner_auth_otp_sessions_partner_id", "partner_auth_otp_sessions", ["partner_id"])

    op.create_table(
        "partner_auth_tokens",
        sa.Column("token_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "partner_id",
            sa.Integer(),
            sa.ForeignKey("partners.partner_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("refresh_token_hash", sa.String(), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_partner_auth_tokens_partner_id", "partner_auth_tokens", ["partner_id"])

    # ── employee auth ─────────────────────────────────────────────
    op.create_table(
        "employee_auth_otp_sessions",
        sa.Column("session_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "employee_id",
            sa.Integer(),
            sa.ForeignKey("employee.employee_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("otp_hash", sa.String(), nullable=False),
        sa.Column("otp_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("failed_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index(
        "ix_employee_auth_otp_sessions_employee_id",
        "employee_auth_otp_sessions",
        ["employee_id"],
    )

    op.create_table(
        "employee_auth_tokens",
        sa.Column("token_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "employee_id",
            sa.Integer(),
            sa.ForeignKey("employee.employee_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("refresh_token_hash", sa.String(), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_employee_auth_tokens_employee_id", "employee_auth_tokens", ["employee_id"])

    # ── employee identity columns ─────────────────────────────────
    op.add_column("employee", sa.Column("name", sa.String(), nullable=True))
    op.add_column("employee", sa.Column("phone", sa.String(), nullable=True))
    op.add_column("employee", sa.Column("email", sa.String(), nullable=True))

    conn = op.get_bind()

    # Backfill employee name/phone/email from users
    conn.execute(
        sa.text(
            """
            UPDATE employee e
            SET
              name = NULLIF(btrim(concat_ws(' ', u.first_name, u.last_name)), ''),
              phone = u.phone,
              email = u.email
            FROM users u
            WHERE e.user_id = u.user_id
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE employee
            SET name = COALESCE(name, 'Employee ' || employee_id::text)
            WHERE name IS NULL OR btrim(name) = ''
            """
        )
    )
    op.alter_column("employee", "name", nullable=False)

    # ── experts.partner_id ────────────────────────────────────────
    op.add_column("experts", sa.Column("partner_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_experts_partner_id",
        "experts",
        "partners",
        ["partner_id"],
        ["partner_id"],
        ondelete="SET NULL",
    )

    # Backfill expert partners from experts.user_id + users
    expert_rows = conn.execute(
        sa.text(
            """
            SELECT ex.expert_id, ex.user_id, u.first_name, u.last_name, u.phone, u.email
            FROM experts ex
            LEFT JOIN users u ON u.user_id = ex.user_id
            WHERE ex.user_id IS NOT NULL
            """
        )
    ).fetchall()
    for row in expert_rows:
        expert_id, user_id, first_name, last_name, phone, email = row
        name = " ".join(p for p in [first_name, last_name] if p).strip() or f"Expert {expert_id}"
        phone_val = phone if phone and str(phone).strip() else None
        email_val = email if email and str(email).strip() else None
        if not phone_val and not email_val:
            phone_val = f"migrated-expert-{expert_id}"
        existing = None
        if phone_val:
            existing = conn.execute(
                sa.text("SELECT partner_id FROM partners WHERE phone = :phone"),
                {"phone": phone_val},
            ).fetchone()
        if existing is None and email_val:
            existing = conn.execute(
                sa.text("SELECT partner_id FROM partners WHERE email = :email"),
                {"email": email_val},
            ).fetchone()
        if existing:
            partner_id = existing[0]
            conn.execute(
                sa.text("UPDATE partners SET role = 'expert', status = 'active' WHERE partner_id = :pid"),
                {"pid": partner_id},
            )
        else:
            partner_id = conn.execute(
                sa.text(
                    """
                    INSERT INTO partners (name, phone, email, role, status)
                    VALUES (:name, :phone, :email, 'expert', 'active')
                    RETURNING partner_id
                    """
                ),
                {"name": name, "phone": phone_val, "email": email_val},
            ).scalar()
        conn.execute(
            sa.text("UPDATE experts SET partner_id = :pid WHERE expert_id = :eid"),
            {"pid": partner_id, "eid": expert_id},
        )

    op.drop_index("ix_experts_user_id", table_name="experts")
    op.drop_constraint("experts_user_id_fkey", "experts", type_="foreignkey")
    op.drop_column("experts", "user_id")
    op.create_index("ix_experts_partner_id", "experts", ["partner_id"])

    # ── assignment → partner_id ───────────────────────────────────
    op.add_column(
        "onboarding_assistant_assignment",
        sa.Column("partner_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_oa_assignment_partner_id",
        "onboarding_assistant_assignment",
        "partners",
        ["partner_id"],
        ["partner_id"],
        ondelete="CASCADE",
    )

    # Map employee_id → partner (phlebo), create partners for OA/expert assignees
    assignment_rows = conn.execute(
        sa.text(
            """
            SELECT DISTINCT a.employee_id, e.user_id, e.role::text,
                   u.first_name, u.last_name, u.phone, u.email,
                   emp.name, emp.phone AS emp_phone, emp.email AS emp_email
            FROM onboarding_assistant_assignment a
            JOIN employee e ON e.employee_id = a.employee_id
            LEFT JOIN users u ON u.user_id = e.user_id
            LEFT JOIN employee emp ON emp.employee_id = a.employee_id
            """
        )
    ).fetchall()

    employee_to_partner: dict[int, int] = {}
    for row in assignment_rows:
        employee_id = row[0]
        role = row[2]
        first_name, last_name, phone, email = row[3], row[4], row[5], row[6]
        emp_name, emp_phone, emp_email = row[7], row[8], row[9]
        name = emp_name or " ".join(p for p in [first_name, last_name] if p).strip() or f"Partner {employee_id}"
        phone_val = emp_phone or phone
        email_val = emp_email or email
        phone_val = phone_val if phone_val and str(phone_val).strip() else None
        email_val = email_val if email_val and str(email_val).strip() else None
        partner_role = "expert" if role == "expert" else "phlebo"
        if not phone_val and not email_val:
            phone_val = f"migrated-partner-{employee_id}"

        existing = None
        if phone_val:
            existing = conn.execute(
                sa.text("SELECT partner_id, role FROM partners WHERE phone = :phone"),
                {"phone": phone_val},
            ).fetchone()
        if existing is None and email_val:
            existing = conn.execute(
                sa.text("SELECT partner_id, role FROM partners WHERE email = :email"),
                {"email": email_val},
            ).fetchone()
        if existing:
            partner_id = existing[0]
        else:
            partner_id = conn.execute(
                sa.text(
                    """
                    INSERT INTO partners (name, phone, email, role, status)
                    VALUES (:name, :phone, :email, :role, 'active')
                    RETURNING partner_id
                    """
                ),
                {
                    "name": name,
                    "phone": phone_val,
                    "email": email_val,
                    "role": partner_role,
                },
            ).scalar()
        employee_to_partner[employee_id] = partner_id

    for employee_id, partner_id in employee_to_partner.items():
        conn.execute(
            sa.text(
                """
                UPDATE onboarding_assistant_assignment
                SET partner_id = :pid
                WHERE employee_id = :eid
                """
            ),
            {"pid": partner_id, "eid": employee_id},
        )

    # Collapse duplicates created when multiple employees mapped to one partner
    conn.execute(
        sa.text(
            """
            DELETE FROM onboarding_assistant_assignment a
            USING onboarding_assistant_assignment b
            WHERE a.partner_id IS NOT NULL
              AND a.partner_id = b.partner_id
              AND a.engagement_id = b.engagement_id
              AND a.onboarding_assistant_id > b.onboarding_assistant_id
            """
        )
    )

    # Backfill remaining OA / expert employees into partners (even if not assigned)
    leftover = conn.execute(
        sa.text(
            """
            SELECT e.employee_id, e.role::text, e.name, e.phone, e.email
            FROM employee e
            WHERE e.role::text IN ('onboarding_assistant', 'expert')
            """
        )
    ).fetchall()
    for employee_id, role, name, phone, email in leftover:
        if employee_id in employee_to_partner:
            continue
        partner_role = "expert" if role == "expert" else "phlebo"
        phone_val = phone if phone and str(phone).strip() else None
        email_val = email if email and str(email).strip() else None
        if not phone_val and not email_val:
            phone_val = f"migrated-partner-{employee_id}"
        existing = None
        if phone_val:
            existing = conn.execute(
                sa.text("SELECT partner_id FROM partners WHERE phone = :phone"),
                {"phone": phone_val},
            ).fetchone()
        if existing is None and email_val:
            existing = conn.execute(
                sa.text("SELECT partner_id FROM partners WHERE email = :email"),
                {"email": email_val},
            ).fetchone()
        if existing:
            partner_id = existing[0]
        else:
            partner_id = conn.execute(
                sa.text(
                    """
                    INSERT INTO partners (name, phone, email, role, status)
                    VALUES (:name, :phone, :email, :role, 'active')
                    RETURNING partner_id
                    """
                ),
                {
                    "name": name or f"Partner {employee_id}",
                    "phone": phone_val,
                    "email": email_val,
                    "role": partner_role,
                },
            ).scalar()
        # Link experts without partner_id yet if this was an expert employee
        if partner_role == "expert":
            conn.execute(
                sa.text(
                    """
                    UPDATE experts
                    SET partner_id = :pid
                    WHERE partner_id IS NULL
                      AND expert_id IN (
                        SELECT ex.expert_id FROM experts ex
                        WHERE ex.partner_id IS NULL
                        LIMIT 0
                      )
                    """
                ),
                {"pid": partner_id},
            )

    # Drop rows that couldn't get partner_id (should be none)
    conn.execute(
        sa.text("DELETE FROM onboarding_assistant_assignment WHERE partner_id IS NULL")
    )
    op.alter_column("onboarding_assistant_assignment", "partner_id", nullable=False)

    op.drop_constraint(
        "uq_onboarding_assistant_assignment",
        "onboarding_assistant_assignment",
        type_="unique",
    )
    op.drop_index(
        "ix_onboarding_assistant_assignment_employee_id",
        table_name="onboarding_assistant_assignment",
    )
    op.drop_constraint(
        "onboarding_assistant_assignment_employee_id_fkey",
        "onboarding_assistant_assignment",
        type_="foreignkey",
    )
    op.drop_column("onboarding_assistant_assignment", "employee_id")
    op.create_unique_constraint(
        "uq_onboarding_assistant_assignment",
        "onboarding_assistant_assignment",
        ["engagement_id", "partner_id"],
    )
    op.create_index(
        "ix_onboarding_assistant_assignment_partner_id",
        "onboarding_assistant_assignment",
        ["partner_id"],
    )

    # ── platform_settings default OAs: employee_ids → partner_ids ─
    settings_row = conn.execute(
        sa.text(
            "SELECT default_onboarding_assistant_employee_ids FROM platform_settings LIMIT 1"
        )
    ).fetchone()
    if settings_row and settings_row[0] is not None:
        raw = settings_row[0]
        ids: list = []
        if isinstance(raw, list):
            ids = raw
        elif isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            s = raw.strip()
            try:
                parsed = json.loads(s)
                ids = parsed if isinstance(parsed, list) else []
            except json.JSONDecodeError:
                # e.g. Postgres array text "{1,2,3}"
                if s.startswith("{") and s.endswith("}"):
                    inner = s[1:-1].strip()
                    ids = [part.strip().strip('"') for part in inner.split(",") if part.strip()]
                else:
                    ids = []
        new_ids: list[int] = []
        for eid in ids:
            try:
                eid_int = int(eid)
            except (TypeError, ValueError):
                continue
            mapped = employee_to_partner.get(eid_int)
            if mapped:
                new_ids.append(mapped)
            else:
                # try leftover OA employee → partner by phone
                prow = conn.execute(
                    sa.text(
                        """
                        SELECT p.partner_id
                        FROM employee e
                        JOIN partners p ON (
                          (e.phone IS NOT NULL AND p.phone = e.phone)
                          OR (e.email IS NOT NULL AND p.email = e.email)
                        )
                        WHERE e.employee_id = :eid AND p.role = 'phlebo'
                        LIMIT 1
                        """
                    ),
                    {"eid": eid_int},
                ).fetchone()
                if prow:
                    new_ids.append(prow[0])
        conn.execute(
            sa.text(
                """
                UPDATE platform_settings
                SET default_onboarding_assistant_employee_ids = CAST(:ids AS json)
                """
            ),
            {"ids": json.dumps(new_ids)},
        )

    # ── org contact_person_user_ids: user_id → employee_id ────────
    org_rows = conn.execute(
        sa.text(
            "SELECT organization_id, contact_person_user_ids FROM organizations WHERE contact_person_user_ids IS NOT NULL"
        )
    ).fetchall()
    for org_id, raw in org_rows:
        data = raw if isinstance(raw, dict) else json.loads(raw) if isinstance(raw, str) else None
        if not isinstance(data, dict):
            continue

        def map_uid(uid: int) -> int | None:
            row = conn.execute(
                sa.text(
                    "SELECT employee_id FROM employee WHERE user_id = :uid LIMIT 1"
                ),
                {"uid": uid},
            ).fetchone()
            return int(row[0]) if row else None

        def map_list(vals):
            out = []
            if not isinstance(vals, list):
                return out
            for v in vals:
                try:
                    mapped = map_uid(int(v))
                except (TypeError, ValueError):
                    continue
                if mapped is not None:
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

    # ── delete OA/expert employee rows (after partners exist) ─────
    # Clear FKs that point at OA/expert employees (orgs, checklists, grants)
    conn.execute(
        sa.text(
            """
            UPDATE organizations
            SET created_employee_id = NULL
            WHERE created_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE organizations
            SET updated_employee_id = NULL
            WHERE updated_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE organizations
            SET bd_employee_id = NULL
            WHERE bd_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE engagement_checklist_tasks
            SET assigned_employee_id = NULL
            WHERE assigned_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE engagement_checklist_tasks
            SET completed_by_employee_id = NULL
            WHERE completed_by_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE engagement_checklists
            SET created_employee_id = NULL
            WHERE created_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE checklist_templates
            SET created_employee_id = NULL
            WHERE created_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE questionnaire_healthy_habit_rules
            SET updated_employee_id = NULL
            WHERE updated_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE employee_category_permissions
            SET granted_by_employee_id = NULL
            WHERE granted_by_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE employee_task_permissions
            SET granted_by_employee_id = NULL
            WHERE granted_by_employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    # Clear task/category perms first for those employees
    conn.execute(
        sa.text(
            """
            DELETE FROM employee_task_permissions
            WHERE employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            DELETE FROM employee_category_permissions
            WHERE employee_id IN (
              SELECT employee_id FROM employee WHERE role::text IN ('onboarding_assistant', 'expert')
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            DELETE FROM employee
            WHERE role::text IN ('onboarding_assistant', 'expert')
            """
        )
    )

    # ── drop employee.user_id ─────────────────────────────────────
    # Use IF EXISTS — wrong names / try-except would abort the PG transaction.
    conn.execute(sa.text("ALTER TABLE employee DROP CONSTRAINT IF EXISTS employee_user_id_fkey"))
    conn.execute(sa.text("ALTER TABLE employee DROP CONSTRAINT IF EXISTS employee_user_id_key"))
    conn.execute(sa.text("ALTER TABLE employee DROP CONSTRAINT IF EXISTS uq_employee_user_id"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_employee_user_id"))
    conn.execute(sa.text("DROP INDEX IF EXISTS uq_employee_user_id"))
    op.drop_column("employee", "user_id")

    op.create_index("ix_employee_phone", "employee", ["phone"], unique=True)
    op.create_index("ix_employee_email", "employee", ["email"], unique=True)

    # ── permission category partners ──────────────────────────────
    conn.execute(
        sa.text(
            """
            INSERT INTO permission_categories (category_key, display_name, description, display_order, is_active)
            VALUES (
              'partners',
              'Partners',
              'Phlebos and experts partner directory',
              (SELECT COALESCE(MAX(display_order), 0) + 1 FROM permission_categories),
              true
            )
            ON CONFLICT (category_key) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported for 0133_partners_employee_auth")
