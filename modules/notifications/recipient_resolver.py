"""Resolve dispatch targets from recipient_type and emit context."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession

from modules.employee.department_codes import department_for_recipient_type
from modules.employee.notify_contacts import employees_to_notify_contacts
from modules.employee.repository import EmployeeRepository
from modules.engagements.repository import EngagementsRepository
from modules.users.repository import UsersRepository


@dataclass
class DispatchTarget:
    user_ids: list[int] = field(default_factory=list)
    contacts: list[dict] = field(default_factory=list)


class RecipientResolver:
    def __init__(
        self,
        engagements_repository: EngagementsRepository | None = None,
        employee_repository: EmployeeRepository | None = None,
        users_repository: UsersRepository | None = None,
    ) -> None:
        self._engagements = engagements_repository or EngagementsRepository()
        self._employees = employee_repository or EmployeeRepository()
        self._users = users_repository or UsersRepository()

    async def resolve(
        self,
        db: AsyncSession,
        *,
        recipient_type: str,
        context: dict,
        engagement_id: int | None = None,
        recipient_config: dict | None = None,
    ) -> DispatchTarget:
        rt = (recipient_type or "").strip()

        if rt == "user":
            cfg = recipient_config if isinstance(recipient_config, dict) else {}
            preferred = (cfg.get("user_context_key") or "").strip()
            if preferred == "user_ids":
                uids = context.get("user_ids")
                if isinstance(uids, list) and uids:
                    return DispatchTarget(user_ids=[int(x) for x in uids])
            elif preferred in ("phone", "email"):
                lookup = await self._user_id_from_contact(
                    db, preferred, context.get(preferred)
                )
                if lookup is not None:
                    return DispatchTarget(user_ids=[lookup])
                contact = self._contact_from_context(context, preferred)
                if contact:
                    return DispatchTarget(contacts=[contact])
            elif preferred:
                uid = context.get(preferred)
                if uid is not None:
                    return DispatchTarget(user_ids=[int(uid)])
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

    def _contact_from_context(self, context: dict, kind: str) -> dict | None:
        pd = context.get("participant_details")
        staff = pd.get("staff") if isinstance(pd, dict) else None
        if not isinstance(staff, dict):
            staff = {}
        raw = context.get(kind) or staff.get(kind)
        if raw is None:
            return None
        value = str(raw).strip()
        if not value:
            return None
        name = str(staff.get("name") or "").strip()
        parts = name.split(None, 1) if name else []
        first_name = parts[0] if parts else ""
        last_name = parts[1] if len(parts) > 1 else ""
        contact: dict = {
            "first_name": first_name,
            "last_name": last_name,
            "phone": value if kind == "phone" else str(staff.get("phone") or context.get("phone") or "").strip(),
            "email": value if kind == "email" else str(staff.get("email") or context.get("email") or "").strip(),
        }
        if staff.get("employee_id") is not None:
            contact["employee_id"] = int(staff["employee_id"])
        if staff.get("partner_id") is not None:
            contact["partner_id"] = int(staff["partner_id"])
        if kind == "phone" and not contact["phone"]:
            return None
        if kind == "email" and not contact["email"]:
            return None
        return contact

    async def _user_id_from_contact(
        self,
        db: AsyncSession,
        kind: str,
        raw: object,
    ) -> int | None:
        if raw is None:
            return None
        value = str(raw).strip()
        if not value:
            return None
        if kind == "phone":
            user = await self._users.get_user_by_phone(db, value)
        elif kind == "email":
            user = await self._users.get_user_by_email(db, value.strip().lower())
        else:
            return None
        return int(user.user_id) if user is not None else None
