"""Refresh email-otp HTML with high-contrast digit boxes."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0197_email_otp_template"
down_revision = "0196_whatapi_otp_slots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    import sys
    from pathlib import Path

    app_root = Path(__file__).resolve().parents[3]
    if str(app_root) not in sys.path:
        sys.path.insert(0, str(app_root))

    from modules.notifications.email_otp_templates import (
        EMAIL_OTP_BODY_PLAIN,
        EMAIL_OTP_SUBJECT,
        default_email_otp_html,
    )

    conn = op.get_bind()
    conn.execute(
        text(
            """
            UPDATE notification_services
            SET email_subject = :subject,
                email_body = :body,
                email_html = :html
            WHERE service_key = 'email-otp'
            """
        ),
        {
            "subject": EMAIL_OTP_SUBJECT,
            "body": EMAIL_OTP_BODY_PLAIN,
            "html": default_email_otp_html(),
        },
    )


def downgrade() -> None:
    pass
