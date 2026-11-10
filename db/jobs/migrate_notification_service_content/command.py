"""Backfill notification_services content from n8n workflow JSON (idempotent).

Entrypoint: ``python -m db.jobs.migrate_notification_service_content --dry-run``
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.config import settings
from db.engine import create_job_engine, job_session_factory
from modules.notifications.models import NotificationService
from modules.notifications.n8n_workflow_parser import (
    ParsedWorkflow,
    default_workflows_dir,
    normalize_webhook_path,
    parse_all_workflows,
)
from modules.notifications.whatapi_client import WhatApiClient
from modules.notifications.whatapi_sync_utils import validate_template_approved

try:
    from db.seed.data import DEFAULT_NOTIFICATION_SERVICES
except ImportError:  # pragma: no cover
    DEFAULT_NOTIFICATION_SERVICES = ()  # type: ignore[misc, assignment]

REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_JSON = REPO_ROOT / "scripts" / "output" / "notification_migration_report.json"
OUTPUT_MD = REPO_ROOT / "docs" / "notification_service_migration_report.md"


def _normalized_json(value: Any) -> str:
    if value is None:
        return "null"
    return json.dumps(value, sort_keys=True, default=str)


def _content_unchanged(svc: NotificationService, updates: dict[str, Any]) -> bool:
    for key, new_val in updates.items():
        current = getattr(svc, key, None)
        if key == "template_variables":
            if _normalized_json(current) != _normalized_json(new_val):
                return False
        elif current != new_val:
            return False
    return True


def _updates_for_row(svc: NotificationService, parsed: ParsedWorkflow) -> dict[str, Any]:
    if parsed.kind == "orchestrator":
        return {"template_variables": parsed.template_variables}

    if svc.channel == "whatsapp":
        out: dict[str, Any] = {}
        if parsed.template_name:
            out["template_name"] = parsed.template_name
        if parsed.template_variables:
            out["template_variables"] = parsed.template_variables
        return out

    if svc.channel == "email":
        out = {}
        if parsed.email_subject:
            out["email_subject"] = parsed.email_subject
        if parsed.email_body:
            out["email_body"] = parsed.email_body
        if parsed.email_html:
            out["email_html"] = parsed.email_html
        return out

    return {
        k: v
        for k, v in parsed.to_content_payload().items()
        if v is not None
    }


def build_catalog_report(workflows_dir: Path) -> dict[str, Any]:
    """Build a report from seed defaults + workflow parser (no DB)."""
    parsed_index = parse_all_workflows(workflows_dir)
    rows: list[dict[str, Any]] = []
    matched_paths: set[str] = set()

    for seed in DEFAULT_NOTIFICATION_SERVICES:
        wp = normalize_webhook_path(seed.webhook_path)
        parsed = parsed_index.get(wp)
        row = {
            "service_key": seed.service_key,
            "channel": seed.channel,
            "template_name": parsed.template_name if parsed else None,
            "webhook_path": wp,
            "workflow_file": parsed.workflow_file if parsed else None,
            "status": "catalog_match" if parsed else "no_workflow",
            "legacy_event_hint": parsed.legacy_event_hint if parsed else None,
            "notes": "; ".join(parsed.parse_notes) if parsed else "",
        }
        if parsed:
            matched_paths.add(wp)
        rows.append(row)

    unmapped_workflows = [
        f"{p.workflow_file} ({wp})"
        for wp, p in parsed_index.items()
        if wp not in matched_paths
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": True,
        "catalog_only": True,
        "workflow_files_parsed": len(parsed_index),
        "rows": rows,
        "unmapped_workflows": sorted(unmapped_workflows),
        "unmapped_db_services": [],
        "parse_failures": [],
    }


def _write_report(payload: dict[str, Any]) -> None:
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    rows = payload.get("rows") or []
    lines = [
        "# Notification service migration report",
        "",
        f"Generated: {payload.get('generated_at', '')}",
        "",
        "| service_key | channel | template_name | webhook_path | workflow_file | status | legacy_event_hint | notes |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {service_key} | {channel} | {template_name} | {webhook_path} | {workflow_file} | {status} | {legacy_event_hint} | {notes} |".format(
                **{k: str(row.get(k) or "").replace("|", "/") for k in row}
            )
        )

    for section in (
        "unmapped_workflows",
        "unmapped_db_services",
        "parse_failures",
    ):
        items = payload.get(section) or []
        if items:
            lines.extend(["", f"## {section}", ""])
            for item in items:
                lines.append(f"- {item}")

    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _run_migration(
    *,
    dry_run: bool,
    yes: bool,
    validate_templates: bool,
    workflows_dir: Path,
) -> dict[str, Any]:
    settings.validate()
    parsed_index = parse_all_workflows(workflows_dir)

    engine = create_job_engine()
    session_factory = job_session_factory(engine)
    client = WhatApiClient() if validate_templates else None

    rows: list[dict[str, Any]] = []
    unmapped_workflows: list[str] = []
    unmapped_db: list[str] = []
    parse_failures: list[str] = []
    matched_webhook_paths: set[str] = set()
    updated_count = 0
    skipped_count = 0

    async with session_factory() as db:
        result = await db.execute(
            select(NotificationService).order_by(
                NotificationService.notification_service_id.asc()
            )
        )
        services = list(result.scalars().all())

        for svc in services:
            wp = normalize_webhook_path(svc.webhook_path)
            parsed = parsed_index.get(wp)
            base = {
                "service_key": svc.service_key,
                "channel": svc.channel,
                "template_name": svc.template_name,
                "webhook_path": wp,
                "workflow_file": None,
                "legacy_event_hint": None,
                "notes": "",
            }

            if parsed is None:
                unmapped_db.append(f"{svc.service_key} ({wp})")
                rows.append({**base, "status": "no_workflow"})
                continue

            matched_webhook_paths.add(wp)
            base["workflow_file"] = parsed.workflow_file
            base["legacy_event_hint"] = parsed.legacy_event_hint
            base["template_name"] = parsed.template_name or base["template_name"]
            base["notes"] = "; ".join(parsed.parse_notes)

            if parsed.parse_notes and any(n.startswith("parse_error") for n in parsed.parse_notes):
                parse_failures.append(f"{parsed.workflow_file}: {base['notes']}")
                rows.append({**base, "status": "unparsed"})
                continue

            updates = _updates_for_row(svc, parsed)
            if not updates:
                rows.append({**base, "status": "unparsed", "notes": "no_content_extracted"})
                continue

            if _content_unchanged(svc, updates):
                status = "skipped_unchanged"
                skipped_count += 1
                rows.append({**base, "status": status})
                continue

            status = "orchestrator" if parsed.kind == "orchestrator" else "updated"

            if validate_templates and client and parsed.template_name and svc.channel == "whatsapp":
                ok, note = await validate_template_approved(client, template_name=parsed.template_name)
                base["notes"] = f"{base['notes']}; template_validation:{note}".strip("; ")

            if not dry_run and yes and status in {"updated", "orchestrator"}:
                for key, value in updates.items():
                    setattr(svc, key, value)
                db.add(svc)
                updated_count += 1

            rows.append({**base, "status": status})

        for wp, parsed in parsed_index.items():
            if wp not in matched_webhook_paths:
                unmapped_workflows.append(f"{parsed.workflow_file} ({wp})")

        if not dry_run and yes:
            await db.commit()
        else:
            await db.rollback()

    await engine.dispose()

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "updated_count": updated_count,
        "skipped_unchanged_count": skipped_count,
        "workflow_files_parsed": len(parsed_index),
        "rows": rows,
        "unmapped_workflows": sorted(unmapped_workflows),
        "unmapped_db_services": sorted(unmapped_db),
        "parse_failures": sorted(parse_failures),
    }
    _write_report(payload)
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill notification_services content from n8n workflows.",
    )
    parser.add_argument(
        "--workflows-dir",
        type=Path,
        default=default_workflows_dir(),
        help="Directory containing n8n workflow JSON files",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB commit")
    parser.add_argument("--yes", action="store_true", help="Apply updates to the database")
    parser.add_argument(
        "--validate-templates",
        action="store_true",
        help="Call WhatAPI to verify WhatsApp template APPROVED status",
    )
    parser.add_argument(
        "--catalog-only",
        action="store_true",
        help="Write migration report from seed + workflows without DB access",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.catalog_only:
        result = build_catalog_report(args.workflows_dir)
        _write_report(result)
        print(json.dumps(result, indent=2, default=str))
        return 0

    if not args.dry_run and not args.yes:
        parser.error("Refusing to run without --dry-run or --yes")

    result = asyncio.run(
        _run_migration(
            dry_run=args.dry_run,
            yes=args.yes,
            validate_templates=args.validate_templates,
            workflows_dir=args.workflows_dir,
        )
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
