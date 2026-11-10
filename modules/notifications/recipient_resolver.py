"""Resolve dispatch targets from recipient_type and emit context."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession

from modules.employee.department_codes import department_for_recipient_type
from modules.employee.notify_contacts import employees_to_notify_contacts
from modules.employee.repository import EmployeeRepository
from modules.engagements.repository import EngagementsRepository


@dataclass
class DispatchTarget:
    user_ids: list[int] = field(default_factory=list)
    contacts: list[dict] = field(default_factory=list)


class RecipientResolver:
    def __init__(
        self,
        engagements_repository: EngagementsRepository | None = None,
        employee_repository: EmployeeRepository | None = None,
    ) -> None:
        self._engagements = engagements_repository or EngagementsRepository()
        self._employees = employee_repository or EmployeeRepository()

    async def resolve(
        self,
        db: AsyncSession,
        *,
        recipient_type: str,
        context: dict,
        engagement_id: int | None = None,
    ) -> DispatchTarget:
        rt = (recipient_type or "").strip()

        if rt == "user":
            uid = context.get("user_id") or context.get("participant_user_id")
            uids = context.get("user_ids")
            if uids:
                return DispatchTarget(user_ids=[int(x) for x in uids])
            if uid is not None:
                return DispatchTarget(user_ids=[int(uid)])
            return DispatchTarget()

        if rt in ("assistants", "assistants.phlebo", "assistants.expert", "assistants.staff"):
            preset = context.get("assistant_contacts")
            if isinstance(preset, list) and preset:
                return DispatchTarget(contacts=preset)
            if engagement_id is None:
                return DispatchTarget()
            include_employees = rt == "assistants"
            role_filter = None
            if rt == "assistants.phlebo":
                role_filter = "phlebo"
            elif rt == "assistants.expert":
                role_filter = "expert"
            elif rt == "assistants.staff":
                role_filter = "staff"
            contacts = await self._engagements.list_onboarding_assistant_notify_contacts(
                db,
                engagement_id=int(engagement_id),
                include_employees=include_employees,
            )
            if role_filter:
                contacts = [c for c in contacts if (c.get("role") or "") == role_filter]
            return DispatchTarget(contacts=contacts)

        dept = department_for_recipient_type(rt)
        if dept is not None or rt == "departments":
            employees = await self._employees.list_active_employees_for_department(
                db, department=dept
            )
            return DispatchTarget(contacts=employees_to_notify_contacts(employees))

        if rt == "members":
            members = context.get("members") or context.get("member_contacts") or []
            if not isinstance(members, list):
                return DispatchTarget()
            user_ids: list[int] = []
            contacts: list[dict] = []
            for m in members:
                if isinstance(m, dict):
                    if m.get("user_id"):
                        user_ids.append(int(m["user_id"]))
                    elif m.get("phone") or m.get("email"):
                        contacts.append(m)
            if user_ids:
                return DispatchTarget(user_ids=user_ids)
            return DispatchTarget(contacts=contacts)

        if rt == "booker":
            uid = context.get("booker_user_id")
            if uid is not None:
                return DispatchTarget(user_ids=[int(uid)])
            contact = context.get("booker_contact")
            if isinstance(contact, dict):
                return DispatchTarget(contacts=[contact])
            return DispatchTarget()

        if rt == "custom":
            contacts = context.get("custom_contacts") or []
            if isinstance(contacts, list):
                return DispatchTarget(contacts=contacts)
            return DispatchTarget()

        return DispatchTarget()
