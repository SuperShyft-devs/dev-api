"""Tests for n8n workflow parser and migration job helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from db.jobs.migrate_notification_service_content.command import (
    _content_unchanged,
    _updates_for_row,
)
from modules.notifications.models import NotificationService
from modules.notifications.n8n_workflow_parser import (
    default_workflows_dir,
    parse_message_csv_template,
    parse_workflow_file,
)

WORKFLOWS = default_workflows_dir()


def test_parse_message_csv_template_otp():
    name, slots = parse_message_csv_template("otp,123456")
    assert name == "otp"
    assert slots == [{"index": 1, "source": "literal", "value": "123456"}]


def test_whatapi_otp_workflow():
    parsed = parse_workflow_file(WORKFLOWS / "whatapi-otp.json")
    assert parsed.webhook_path == "/send-otp-whatsapp-v3"
    assert parsed.template_name == "otp"
    assert parsed.template_variables["slots"][0]["source"] == "otp"


def test_pretest_whatsapp_workflow():
    parsed = parse_workflow_file(WORKFLOWS / "pretest-whatsapp.json")
    assert parsed.template_name == "camp14"
    assert parsed.template_variables["slots"][0]["source"] == "user.first_name"


def test_pretest_whatsapp_session_medialink():
    parsed = parse_workflow_file(WORKFLOWS / "pretest-whatsapp-session.json")
    assert parsed.template_name == "camp15"
    assert parsed.template_variables.get("header", {}).get("type") == "document"
    assert "medialink" in (parsed.template_variables.get("attachments") or {})


def test_family_onboard_receiver_pdflink():
    parsed = parse_workflow_file(WORKFLOWS / "family-onboard-receiver-whatsapp.json")
    assert parsed.template_name == "camp15"
    attachments = parsed.template_variables.get("attachments") or {}
    assert "pdflink" in attachments


def test_email_otp_workflow():
    parsed = parse_workflow_file(WORKFLOWS / "email-otp.json")
    assert parsed.channel == "email"
    assert parsed.email_html == "n8n_dynamic_html_template"
    assert parsed.email_subject or parsed.parse_notes == []


def test_booking_alert_orchestrator():
    parsed = parse_workflow_file(
        WORKFLOWS / "booking-alert-email+whatsapp+participant-welcome-metsights.json"
    )
    assert parsed.kind == "orchestrator"
    assert parsed.template_variables["kind"] == "orchestrator"
    assert parsed.chained_dispatch_urls


def test_updates_for_row_and_idempotency():
    parsed = parse_workflow_file(WORKFLOWS / "whatapi-otp.json")
    svc = NotificationService(
        service_key="whatapi-otp",
        display_name="OTP",
        channel="whatsapp",
        webhook_path="/send-otp-whatsapp-v3",
        is_active=True,
    )
    updates = _updates_for_row(svc, parsed)
    for key, value in updates.items():
        setattr(svc, key, value)
    assert _content_unchanged(svc, updates) is True


@pytest.mark.asyncio
async def test_migration_job_dry_run_writes_report(tmp_path, monkeypatch):
    from db.jobs.migrate_notification_service_content import command as job_command

    out_json = tmp_path / "report.json"
    out_md = tmp_path / "report.md"
    monkeypatch.setattr(job_command, "OUTPUT_JSON", out_json)
    monkeypatch.setattr(job_command, "OUTPUT_MD", out_md)

    payload = await job_command._run_migration(
        dry_run=True,
        yes=False,
        validate_templates=False,
        workflows_dir=WORKFLOWS,
    )
    assert payload["workflow_files_parsed"] >= 60
    assert out_json.exists()
    assert out_md.exists()
