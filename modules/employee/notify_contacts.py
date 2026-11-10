"""Map employee rows to notification dispatch contacts."""

from __future__ import annotations

from modules.employee.models import Employee


def employees_to_notify_contacts(employees: list[Employee]) -> list[dict]:
    contacts: list[dict] = []
    for emp in employees:
        phone = (emp.phone or "").strip()
        email = (emp.email or "").strip()
        if not phone and not email:
            continue
        name = (emp.name or "").strip()
        parts = name.split(None, 1)
        contacts.append(
            {
                "first_name": parts[0] if parts else "",
                "last_name": parts[1] if len(parts) > 1 else "",
                "phone": phone,
                "email": email,
            }
        )
    return contacts
