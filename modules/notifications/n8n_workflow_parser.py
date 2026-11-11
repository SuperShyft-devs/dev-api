"""Parse n8n workflow JSON into notification_services content fields."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_WHATAPI_HOST = "webhook.whatapi.in"
_MESSAGE_BACKTICK_RE = re.compile(
    r"(?:const|let)\s+message\s*=\s*(?:\n)?`([^`]+)`",
    re.DOTALL,
)
_SUBJECT_RE = re.compile(
    r"(?:const\s+subject\s*=\s*|subject\s*:\s*)['\"`]([^'\"`]+)['\"`]",
    re.IGNORECASE,
)
_SUBJECT_JSON_RE = re.compile(
    r"subject\s*:\s*'([^']+)'",
    re.IGNORECASE,
)
_TEXT_BODY_RE = re.compile(
    r"(?:const\s+textBody\s*=\s*|textBody\s*:\s*)['\"`]([^'\"`]+)['\"`]",
    re.DOTALL,
)


def normalize_webhook_path(path: str) -> str:
    cleaned = (path or "").strip()
    if not cleaned:
        return ""
    return cleaned if cleaned.startswith("/") else f"/{cleaned}"


def canonicalize_slot_source(expr: str) -> str:
    raw = (expr or "").strip()
    lowered = raw.lower()

    if not raw:
        return "literal"

    if raw == "otp":
        return "otp"
    if raw in {"name", "first_name"}:
        return "user.first_name"

    rules: list[tuple[str, str]] = [
        (r"member\.otp|\.otp\b", "otp"),
        (r"first_name", "user.first_name"),
        (r"last_name", "user.last_name"),
        (r"member\.phone|\.phone\b", "user.phone"),
        (r"member\.email|\.email\b", "user.email"),
        (r"session_details\?\.slot|session_details\.slot|consultation_time|collectiontime", "session.time"),
        (r"session_details\?\.date|session_details\.date|collectiondate", "session.date"),
        (r"session_details\?\.cabin|session_details\.cabin|cabinname|cabin_name", "session.cabin"),
        (r"blood_report_url", "report.blood_report_url"),
        (r"bio_ai_report_url|bioai_report_url", "report.bio_ai_report_url"),
        (r"face_scan_report_url", "report.face_scan_report_url"),
        (r"participant_details", "participant_details"),
        (r"booker_name|pd\.booker_name", "gift.booker_name"),
        (r"booker_phone|pd\.booker_phone", "gift.booker_phone"),
        (r"recipient_name|pd\.recipient_name", "gift.recipient_name"),
        (r"relationship|pd\.relationship", "gift.relationship"),
        (r"gift_url|pd\.gift_url", "gift.gift_url"),
        (r"medialink", "report.blood_report_url"),
        (r"pdflink", "gift.gift_card_url"),
        (r"external_link", "external_link"),
    ]
    for pattern, source in rules:
        if re.search(pattern, lowered):
            return source
    return f"expression:{raw[:120]}"


def _split_message_slots(rest: str) -> list[dict[str, Any]]:
    slots: list[dict[str, Any]] = []
    idx = 1
    pos = 0
    length = len(rest)

    while pos < length:
        if rest.startswith("${", pos):
            end = rest.find("}", pos)
            if end == -1:
                break
            expr = rest[pos + 2 : end]
            slots.append({"index": idx, "source": canonicalize_slot_source(expr)})
            idx += 1
            pos = end + 1
            if pos < length and rest[pos] == ",":
                pos += 1
            continue

        next_expr = rest.find("${", pos)
        next_comma = rest.find(",", pos)
        if next_expr != -1 and (next_comma == -1 or next_expr < next_comma):
            literal = rest[pos:next_expr].strip().rstrip(",")
            if literal:
                slots.append({"index": idx, "source": "literal", "value": literal})
                idx += 1
            pos = next_expr
            continue

        if next_comma != -1:
            literal = rest[pos:next_comma].strip()
            if literal:
                slots.append({"index": idx, "source": "literal", "value": literal})
                idx += 1
            pos = next_comma + 1
            continue

        literal = rest[pos:].strip()
        if literal:
            slots.append({"index": idx, "source": "literal", "value": literal})
        break

    return slots


def parse_message_csv_template(message_template: str) -> tuple[str | None, list[dict[str, Any]]]:
    template = (message_template or "").strip()
    if not template:
        return None, []
    comma = template.find(",")
    if comma == -1:
        return template, []
    template_name = template[:comma].strip()
    rest = template[comma + 1 :]
    return template_name, _split_message_slots(rest)


def _legacy_event_hint(workflow_file: str, webhook_path: str) -> str | None:
    blob = f"{workflow_file} {webhook_path}".lower()
    hints: list[tuple[str, str]] = [
        ("whatapi-otp", "auth.otp_sent"),
        ("send-otp-whatsapp", "auth.otp_sent"),
        ("email-otp", "auth.otp_sent"),
        ("send-otp-email", "auth.otp_sent"),
        ("pretest-", "reminders.pretest"),
        ("welcome-", "users.onboarded"),
        ("booking-confirmation", "bookings.confirmed"),
        ("booking-alert", "users.onboarded"),
        ("phlebo-assigned", "bookings.phlebo_assigned"),
        ("phlebo-reassigned", "bookings.phlebo_reassigned"),
        ("phlebo-enroute", "bookings.phlebo_enroute"),
        ("phlebo-delay", "bookings.phlebo_delayed"),
        ("send-blood-report", "reports.blood_ready"),
        ("send-bioai", "reports.bioai_ready"),
        ("consultation-remainder", "reminders.consultation_reminder"),
        ("consultation-booking-alert", "consultations.booked"),
        ("family-onboard-sender", "family.gift_sent"),
        ("family-onboard-receiver", "family.gift_received"),
        ("notify-admin-contact", "support.ticket_created"),
        ("cpu-overload", "system.cpu_threshold"),
        ("questionnaire", "reminders.questionnaire_before"),
        ("assessment-reminder", "reminders.questionnaire_before"),
    ]
    for key, hint in hints:
        if key in blob:
            return hint
    return None


@dataclass
class ParsedWorkflow:
    workflow_file: str
    webhook_path: str
    filename_stem: str
    channel: str = "unknown"
    kind: str = "leaf"
    template_name: str | None = None
    template_variables: dict[str, Any] | None = None
    email_subject: str | None = None
    email_body: str | None = None
    email_html: str | None = None
    chained_dispatch_urls: list[str] = field(default_factory=list)
    legacy_event_hint: str | None = None
    parse_notes: list[str] = field(default_factory=list)

    def to_content_payload(self) -> dict[str, Any]:
        return {
            "template_name": self.template_name,
            "template_variables": self.template_variables,
            "email_subject": self.email_subject,
            "email_body": self.email_body,
            "email_html": self.email_html,
        }


def _iter_nodes(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = workflow.get("nodes")
    if not isinstance(nodes, list):
        return []
    return [n for n in nodes if isinstance(n, dict)]


def _node_jscodes(nodes: list[dict[str, Any]]) -> list[str]:
    codes: list[str] = []
    for node in nodes:
        if node.get("type") != "n8n-nodes-base.code":
            continue
        js = (node.get("parameters") or {}).get("jsCode")
        if isinstance(js, str) and js.strip():
            codes.append(js)
    return codes


def _extract_webhook_path(nodes: list[dict[str, Any]]) -> str | None:
    for node in nodes:
        if node.get("type") != "n8n-nodes-base.webhook":
            continue
        path = (node.get("parameters") or {}).get("path")
        if isinstance(path, str) and path.strip():
            return normalize_webhook_path(path)
    return None


def _extract_http_requests(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for node in nodes:
        if node.get("type") != "n8n-nodes-base.httpRequest":
            continue
        params = node.get("parameters") or {}
        out.append(
            {
                "name": node.get("name"),
                "url": str(params.get("url") or ""),
                "query": params.get("queryParameters") or {},
            }
        )
    return out


def _has_email_send(nodes: list[dict[str, Any]]) -> bool:
    return any(n.get("type") == "n8n-nodes-base.emailSend" for n in nodes)


def _pick_message_template(jscodes: list[str]) -> tuple[str | None, list[dict[str, Any]]]:
    best_name: str | None = None
    best_slots: list[dict[str, Any]] = []
    for code in jscodes:
        for match in _MESSAGE_BACKTICK_RE.finditer(code):
            name, slots = parse_message_csv_template(match.group(1))
            if name:
                best_name = name
                best_slots = slots
    return best_name, best_slots


def _extract_attachments(http_nodes: list[dict[str, Any]]) -> dict[str, str]:
    attachments: dict[str, str] = {}
    for req in http_nodes:
        if _WHATAPI_HOST not in req.get("url", ""):
            continue
        params = req.get("query") or {}
        raw_params = params.get("parameters")
        if not isinstance(raw_params, list):
            continue
        for item in raw_params:
            if not isinstance(item, dict):
                continue
            key = item.get("name")
            value = item.get("value")
            if key in {"medialink", "pdflink"} and isinstance(value, str):
                attachments[str(key)] = value
    return attachments


def _canonical_attachment_source(key: str, value: str) -> str:
    if "blood_report" in value.lower():
        return "report.blood_report_url"
    if "pdflink" in key.lower() or "gift" in value.lower():
        return "gift.gift_card_url"
    if "bio" in value.lower():
        return "report.bio_ai_report_url"
    if "http" in value.lower():
        return "literal_url"
    return canonicalize_slot_source(value)


def _build_template_variables(
    *,
    kind: str,
    slots: list[dict[str, Any]],
    attachments: dict[str, str],
    chained_dispatch_urls: list[str],
    chained_n8n_paths: list[str],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "format": "POSITIONAL",
        "kind": kind,
        "slots": slots,
    }
    if attachments.get("medialink"):
        payload["header"] = {
            "type": "document",
            "source": _canonical_attachment_source("medialink", attachments["medialink"]),
        }
        payload.setdefault("attachments", {})["medialink"] = attachments["medialink"]
    if attachments.get("pdflink"):
        payload["header"] = {
            "type": "document",
            "source": _canonical_attachment_source("pdflink", attachments["pdflink"]),
        }
        payload.setdefault("attachments", {})["pdflink"] = attachments["pdflink"]
    if kind == "orchestrator":
        payload["chained_dispatch_urls"] = chained_dispatch_urls
        payload["chained_n8n_webhook_paths"] = chained_n8n_paths
    return payload


def _extract_email_fields(jscodes: list[str], nodes: list[dict[str, Any]]) -> tuple[str | None, str | None, str | None]:
    subject: str | None = None
    text_body: str | None = None
    html_snippet: str | None = None

    for code in jscodes:
        m = _SUBJECT_RE.search(code) or _SUBJECT_JSON_RE.search(code)
        if m and not subject:
            subject = m.group(1).strip()
        tb = _TEXT_BODY_RE.search(code)
        if tb and not text_body:
            text_body = tb.group(1).strip()
        if "htmlBody" in code and html_snippet is None:
            if "verification code" in code.lower() or "otp" in code.lower():
                html_snippet = "n8n_dynamic_html_template"
            elif "<html" in code.lower():
                html_snippet = "n8n_dynamic_html_template"

    has_email = _has_email_send(nodes)
    if has_email and not subject:
        subject = "n8n_email_subject_from_workflow"

    return subject, text_body, html_snippet


def parse_workflow_file(path: Path) -> ParsedWorkflow:
    workflow = json.loads(path.read_text(encoding="utf-8"))
    nodes = _iter_nodes(workflow)
    jscodes = _node_jscodes(nodes)
    http_nodes = _extract_http_requests(nodes)

    webhook_path = _extract_webhook_path(nodes) or ""
    filename_stem = path.stem

    parsed = ParsedWorkflow(
        workflow_file=path.name,
        webhook_path=webhook_path,
        filename_stem=filename_stem,
        legacy_event_hint=_legacy_event_hint(path.name, webhook_path),
    )

    chained_dispatch = [
        r["url"]
        for r in http_nodes
        if "/notifications/dispatch" in r.get("url", "")
    ]
    chained_n8n = [
        normalize_webhook_path(u.split("/webhook/", 1)[-1]) if "/webhook/" in u else u
        for r in http_nodes
        for u in [r.get("url", "")]
        if "n8n" in u.lower() and "/webhook/" in u
    ]

    has_whatapi = any(_WHATAPI_HOST in r.get("url", "") for r in http_nodes)
    has_email = _has_email_send(nodes)
    is_orchestrator = bool(chained_dispatch)

    if has_email and not has_whatapi:
        parsed.channel = "email"
    elif has_whatapi:
        parsed.channel = "whatsapp"
    elif has_email:
        parsed.channel = "email"

    if is_orchestrator:
        parsed.kind = "orchestrator"
        parsed.chained_dispatch_urls = chained_dispatch
        parsed.template_variables = _build_template_variables(
            kind="orchestrator",
            slots=[],
            attachments=_extract_attachments(http_nodes),
            chained_dispatch_urls=chained_dispatch,
            chained_n8n_paths=chained_n8n,
        )
        parsed.parse_notes.append("classified_as_orchestrator")
        if has_email:
            subj, body, html = _extract_email_fields(jscodes, nodes)
            parsed.email_subject = subj
            parsed.email_body = body
            parsed.email_html = html
        return parsed

    template_name, slots = _pick_message_template(jscodes)
    attachments = _extract_attachments(http_nodes)

    if template_name:
        parsed.template_name = template_name
        parsed.kind = "leaf"
        parsed.channel = parsed.channel if parsed.channel != "unknown" else "whatsapp"
        parsed.template_variables = _build_template_variables(
            kind="leaf",
            slots=slots,
            attachments=attachments,
            chained_dispatch_urls=[],
            chained_n8n_paths=[],
        )
    elif has_email:
        parsed.channel = "email"
        parsed.kind = "leaf"
        subj, body, html = _extract_email_fields(jscodes, nodes)
        parsed.email_subject = subj
        parsed.email_body = body
        parsed.email_html = html
        if not subj and not body and not html:
            parsed.parse_notes.append("email_workflow_no_extracted_content")
    else:
        parsed.kind = "leaf"
        parsed.parse_notes.append("no_whatapi_message_or_email_found")

    return parsed


def parse_all_workflows(directory: Path) -> dict[str, ParsedWorkflow]:
    index: dict[str, ParsedWorkflow] = {}
    for path in sorted(directory.glob("*.json")):
        try:
            parsed = parse_workflow_file(path)
        except (json.JSONDecodeError, OSError) as exc:
            parsed = ParsedWorkflow(
                workflow_file=path.name,
                webhook_path="",
                filename_stem=path.stem,
                parse_notes=[f"parse_error:{exc}"],
            )
        if parsed.webhook_path:
            index[parsed.webhook_path] = parsed
    return index


def default_workflows_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "n8n" / "workflows"
