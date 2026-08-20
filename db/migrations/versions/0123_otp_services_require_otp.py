"""Ensure OTP notification services require and receive OTP codes.

Revision ID: 0123_otp_svc_require_otp
Revises: 0122_qnr_sub_text
Create Date: 2026-08-20

Migration 0054 added require_otp with server_default=false. Existing
whatapi-otp / email-otp rows could keep require_otp=false, which caused
dispatch to omit the OTP from WhatsApp/email payloads and login verify
to fail with 401.
"""

from __future__ import annotations

from alembic import op


revision = "0123_otp_svc_require_otp"
down_revision = "0122_qnr_sub_text"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE notification_services
        SET require_otp = true
        WHERE service_key IN ('whatapi-otp', 'email-otp')
        """
    )
    op.execute(
        """
        INSERT INTO notification_services (
            service_key, display_name, channel, webhook_path, is_active, require_otp
        )
        SELECT 'email-otp', 'OTP via Email', 'email', '/send-otp-email-v3', true, true
        WHERE NOT EXISTS (
            SELECT 1 FROM notification_services WHERE service_key = 'email-otp'
        )
        """
    )
    op.execute(
        """
        INSERT INTO notification_services (
            service_key, display_name, channel, webhook_path, is_active, require_otp
        )
        SELECT 'whatapi-otp', 'OTP via WhatsApp', 'whatsapp', '/send-otp-whatsapp-v3', true, true
        WHERE NOT EXISTS (
            SELECT 1 FROM notification_services WHERE service_key = 'whatapi-otp'
        )
        """
    )


def downgrade() -> None:
    # Do not force require_otp back to false; prior default was ambient.
    pass
