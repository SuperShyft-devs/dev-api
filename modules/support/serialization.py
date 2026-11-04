"""Support ticket API payloads."""

from __future__ import annotations

from typing import Any

from modules.support.models import SupportTicket


def ticket_to_dict(
    ticket: SupportTicket,
    *,
    resolved_by_employee_name: str | None = None,
) -> dict[str, Any]:
    return {
        "ticket_id": ticket.ticket_id,
        "user_id": ticket.user_id,
        "contact_input": ticket.contact_input,
        "query_text": ticket.query_text,
        "status": ticket.status,
        "created_at": ticket.created_at,
        "resolved_by_employee_id": ticket.resolved_by_employee_id,
        "resolved_at": ticket.resolved_at,
        "resolution_notes": ticket.resolution_notes,
        "resolved_by_employee_name": resolved_by_employee_name,
    }
