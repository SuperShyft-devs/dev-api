"""Restructure engagement notifications into normalised tables.

Revision ID: 0107_engagement_notifications_restructure
Revises: 0106_b2c_by_engagement_type
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0107_engagement_notifications_restructure"
down_revision = "0106_b2c_by_engagement_type"
branch_labels = None
depends_on = None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ENGAGEMENT_TYPE_SEEDS = [
    ("bio_ai", "BioAI"),
    ("blood_test", "Blood Test"),
    ("consultation", "Consultation"),
    ("blood_test_with_consultation", "Blood Test with Consultation"),
    ("bio_ai_with_consultation", "BioAI with Consultation"),
]

_ALL_CODES = [code for code, _ in _ENGAGEMENT_TYPE_SEEDS]

_EVENT_SEEDS: list[tuple[str, str, list[str], str]] = [
    (
        "onboarding",
        "Onboarding Notification",
        _ALL_CODES,
        "Notification sent to onboarding assistants when a participant enrolls",
    ),
    (
        "pretest_guidelines",
        "Pretest Guidelines",
        _ALL_CODES,
        "Pretest blood collection guidelines sent day before collection",
    ),
    (
        "questionnaire_reminder_before",
        "Questionnaire Reminder (Day Before)",
        ["bio_ai", "blood_test", "blood_test_with_consultation", "bio_ai_with_consultation"],
        "Reminder for incomplete questionnaire sent day before engagement",
    ),
    (
        "questionnaire_reminder_after",
        "Questionnaire Reminder (Day After)",
        ["bio_ai", "blood_test", "blood_test_with_consultation", "bio_ai_with_consultation"],
        "Reminder for incomplete questionnaire sent day after engagement",
    ),
    (
        "blood_report_ready",
        "Blood Report Notification",
        ["blood_test", "blood_test_with_consultation"],
        "Notification sent when blood report is ready",
    ),
    (
        "bioai_report_ready",
        "BioAI Report Notification",
        ["bio_ai", "bio_ai_with_consultation"],
        "Notification sent when BioAI report is ready",
    ),
    (
        "consultation_ready",
        "Consultation Notification",
        ["consultation", "blood_test_with_consultation", "bio_ai_with_consultation"],
        "Notification sent when report is ready for consultation booking",
    ),
]

_OLD_COL_TO_EVENT: dict[str, str] = {
    "onboarding_notification": "onboarding",
    "pretest_guidelines_notification": "pretest_guidelines",
    "questionnaire_reminder_1": "questionnaire_reminder_before",
    "questionnaire_reminder_2": "questionnaire_reminder_after",
    "blood_report_notification": "blood_report_ready",
    "bioai_report_notification": "bioai_report_ready",
    "notify_users_for_consultation": "consultation_ready",
}

_DEFAULT_COL_TO_EVENT: dict[str, str] = {
    "default_onboarding_notification": "onboarding",
    "default_pretest_guidelines_notification": "pretest_guidelines",
    "default_questionnaire_reminder_1": "questionnaire_reminder_before",
    "default_questionnaire_reminder_2": "questionnaire_reminder_after",
    "default_blood_report_notification": "blood_report_ready",
    "default_bioai_report_notification": "bioai_report_ready",
    "default_notify_users_for_consultation": "consultation_ready",
}


def upgrade() -> None:
    conn = op.get_bind()

    # ------------------------------------------------------------------
    # 1. Create engagement_types
    # ------------------------------------------------------------------
    op.create_table(
        "engagement_types",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("code", sa.String, unique=True, nullable=False),
        sa.Column("display_name", sa.String, nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("true")),
    )

    # ------------------------------------------------------------------
    # 2. Seed engagement_types
    # ------------------------------------------------------------------
    conn.execute(
        sa.text(
            """
            INSERT INTO engagement_types (code, display_name) VALUES
            ('bio_ai', 'BioAI'),
            ('blood_test', 'Blood Test'),
            ('consultation', 'Consultation'),
            ('blood_test_with_consultation', 'Blood Test with Consultation'),
            ('bio_ai_with_consultation', 'BioAI with Consultation')
            """
        )
    )

    # Build a code → id lookup
    rows = conn.execute(
        sa.text("SELECT id, code FROM engagement_types")
    ).fetchall()
    et_id_by_code: dict[str, int] = {row.code: row.id for row in rows}

    # ------------------------------------------------------------------
    # 3. Create auto_notification_events
    # ------------------------------------------------------------------
    op.create_table(
        "auto_notification_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("engagement_type_ids", postgresql.ARRAY(sa.Integer), nullable=False),
        sa.Column("event_code", sa.String, unique=True, nullable=False),
        sa.Column("display_name", sa.String, nullable=False),
        sa.Column("description", sa.Text),
    )

    # ------------------------------------------------------------------
    # 4. Seed auto_notification_events
    # ------------------------------------------------------------------
    for event_code, display_name, type_codes, description in _EVENT_SEEDS:
        type_ids = [et_id_by_code[c] for c in type_codes]
        conn.execute(
            sa.text(
                """
                INSERT INTO auto_notification_events
                    (engagement_type_ids, event_code, display_name, description)
                VALUES (:type_ids, :event_code, :display_name, :description)
                """
            ),
            {
                "type_ids": type_ids,
                "event_code": event_code,
                "display_name": display_name,
                "description": description,
            },
        )

    # Build event_code → id lookup
    evt_rows = conn.execute(
        sa.text("SELECT id, event_code FROM auto_notification_events")
    ).fetchall()
    evt_id_by_code: dict[str, int] = {row.event_code: row.id for row in evt_rows}

    # ------------------------------------------------------------------
    # 5. Create engagement_notifications
    # ------------------------------------------------------------------
    op.create_table(
        "engagement_notifications",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "engagement_id",
            sa.Integer,
            sa.ForeignKey("engagements.engagement_id"),
            nullable=False,
        ),
        sa.Column(
            "notification_event_id",
            sa.Integer,
            sa.ForeignKey("auto_notification_events.id"),
            nullable=False,
        ),
        sa.Column("notification_services", postgresql.ARRAY(sa.String), nullable=False),
        sa.UniqueConstraint("engagement_id", "notification_event_id"),
    )
    op.create_index(
        "ix_engagement_notifications_engagement_id",
        "engagement_notifications",
        ["engagement_id"],
    )

    # ------------------------------------------------------------------
    # 6. Create engagement_notification_defaults
    # ------------------------------------------------------------------
    op.create_table(
        "engagement_notification_defaults",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "engagement_type_id",
            sa.Integer,
            sa.ForeignKey("engagement_types.id"),
            nullable=False,
        ),
        sa.Column(
            "notification_event_id",
            sa.Integer,
            sa.ForeignKey("auto_notification_events.id"),
            nullable=False,
        ),
        sa.Column("notification_services", postgresql.ARRAY(sa.String), nullable=False),
        sa.UniqueConstraint("engagement_type_id", "notification_event_id"),
    )
    op.create_index(
        "ix_engagement_notification_defaults_engagement_type_id",
        "engagement_notification_defaults",
        ["engagement_type_id"],
    )

    # ------------------------------------------------------------------
    # 7. Add engagement_type_id FK column to engagements
    # ------------------------------------------------------------------
    op.add_column(
        "engagements",
        sa.Column(
            "engagement_type_id",
            sa.Integer,
            sa.ForeignKey("engagement_types.id"),
            nullable=True,
        ),
    )

    # ------------------------------------------------------------------
    # 8. Migrate engagement_type enum → integer FK
    # ------------------------------------------------------------------
    conn.execute(
        sa.text(
            """
            UPDATE engagements SET engagement_type_id = et.id
            FROM engagement_types et
            WHERE engagements.engagement_type::text = et.code
            AND engagements.engagement_type IS NOT NULL
            """
        )
    )

    # ------------------------------------------------------------------
    # 9. Migrate notification columns → engagement_notifications
    # ------------------------------------------------------------------
    engagement_rows = conn.execute(
        sa.text(
            """
            SELECT engagement_id,
                   onboarding_notification,
                   pretest_guidelines_notification,
                   questionnaire_reminder_1,
                   questionnaire_reminder_2,
                   blood_report_notification,
                   bioai_report_notification,
                   notify_users_for_consultation
            FROM engagements
            """
        )
    ).fetchall()

    for eng in engagement_rows:
        eid = eng.engagement_id
        for old_col, event_code in _OLD_COL_TO_EVENT.items():
            raw_val = getattr(eng, old_col, None)
            if raw_val is None or str(raw_val).strip() == "":
                continue
            services = [s.strip() for s in str(raw_val).split(",") if s.strip()]
            if not services:
                continue
            conn.execute(
                sa.text(
                    """
                    INSERT INTO engagement_notifications
                        (engagement_id, notification_event_id, notification_services)
                    VALUES (:eid, :evt_id, :services)
                    ON CONFLICT (engagement_id, notification_event_id) DO NOTHING
                    """
                ),
                {
                    "eid": eid,
                    "evt_id": evt_id_by_code[event_code],
                    "services": services,
                },
            )

    # ------------------------------------------------------------------
    # 10. Migrate platform_settings defaults →
    #     engagement_notification_defaults
    # ------------------------------------------------------------------
    default_cols = list(_DEFAULT_COL_TO_EVENT.keys())
    ps_row = conn.execute(
        sa.text(
            "SELECT " + ", ".join(default_cols) + " FROM platform_settings LIMIT 1"
        )
    ).fetchone()

    if ps_row is not None:
        # Build event_code → list of engagement_type_ids from event seeds
        event_type_ids_map: dict[str, list[int]] = {}
        for event_code, _, type_codes, _ in _EVENT_SEEDS:
            event_type_ids_map[event_code] = [et_id_by_code[c] for c in type_codes]

        for default_col, event_code in _DEFAULT_COL_TO_EVENT.items():
            raw_val = getattr(ps_row, default_col, None)
            if raw_val is None or str(raw_val).strip() == "":
                continue
            services = [s.strip() for s in str(raw_val).split(",") if s.strip()]
            if not services:
                continue
            for type_id in event_type_ids_map[event_code]:
                conn.execute(
                    sa.text(
                        """
                        INSERT INTO engagement_notification_defaults
                            (engagement_type_id, notification_event_id, notification_services)
                        VALUES (:type_id, :evt_id, :services)
                        ON CONFLICT (engagement_type_id, notification_event_id) DO NOTHING
                        """
                    ),
                    {
                        "type_id": type_id,
                        "evt_id": evt_id_by_code[event_code],
                        "services": services,
                    },
                )

    # ------------------------------------------------------------------
    # 11. Drop old notification columns from engagements
    # ------------------------------------------------------------------
    for col in _OLD_COL_TO_EVENT:
        op.drop_column("engagements", col)

    # ------------------------------------------------------------------
    # 12. Drop old default columns from platform_settings
    # ------------------------------------------------------------------
    for col in _DEFAULT_COL_TO_EVENT:
        op.drop_column("platform_settings", col)

    # ------------------------------------------------------------------
    # 13. Drop old engagement_type enum column, rename FK column
    # ------------------------------------------------------------------
    op.drop_column("engagements", "engagement_type")
    op.alter_column("engagements", "engagement_type_id", new_column_name="engagement_type")

    # ------------------------------------------------------------------
    # 14. Drop the enum type
    # ------------------------------------------------------------------
    sa.Enum(name="engagement_kind").drop(op.get_bind(), checkfirst=True)


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported for this migration")
