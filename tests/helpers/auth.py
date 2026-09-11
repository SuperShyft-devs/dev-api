"""Shared JWT and seed helpers for staff / partner / user auth in tests."""

from __future__ import annotations

from core.subject_auth import JwtSubjectTyp, create_access_token
from modules.employee.models import Employee
from modules.partners.models import Partner


def auth_header(subject_id: int, typ: JwtSubjectTyp = "employee") -> dict[str, str]:
    """Authorization header for a subject JWT (employee / partner / user)."""
    return {"Authorization": f"Bearer {create_access_token(subject_id, typ)}"}


def employee_auth_header(employee_id: int) -> dict[str, str]:
    return auth_header(employee_id, "employee")


def partner_auth_header(partner_id: int) -> dict[str, str]:
    return auth_header(partner_id, "partner")


def user_auth_header(user_id: int) -> dict[str, str]:
    return auth_header(user_id, "user")


def make_employee(
    *,
    employee_id: int,
    role: str = "admin",
    status: str = "active",
    name: str | None = None,
    phone: str | None = None,
    email: str | None = None,
) -> Employee:
    """Build an Employee row (no users linkage)."""
    return Employee(
        employee_id=employee_id,
        name=name or f"Employee {employee_id}",
        phone=phone if phone is not None else f"{employee_id:010d}"[:15],
        email=email if email is not None else f"employee{employee_id}@test.example",
        role=role,
        status=status,
    )


def make_partner(
    *,
    partner_id: int | None = None,
    role: str = "phlebo",
    status: str = "active",
    name: str | None = None,
    phone: str | None = None,
    email: str | None = None,
) -> Partner:
    """Build a Partner row. Omit partner_id to let the DB assign one."""
    kwargs: dict = {
        "name": name or f"Partner {role}",
        "phone": phone if phone is not None else None,
        "email": email if email is not None else None,
        "role": role,
        "status": status,
    }
    if partner_id is not None:
        kwargs["partner_id"] = partner_id
    if kwargs["phone"] is None and kwargs["email"] is None:
        pid = partner_id or 0
        kwargs["phone"] = f"9{pid:09d}"[:15]
    return Partner(**kwargs)


async def seed_employee(
    db,
    *,
    employee_id: int,
    role: str = "admin",
    status: str = "active",
    name: str | None = None,
    phone: str | None = None,
    email: str | None = None,
    commit: bool = True,
) -> Employee:
    existing = await db.get(Employee, employee_id)
    if existing is not None:
        existing.name = name or existing.name or f"Employee {employee_id}"
        if phone is not None:
            existing.phone = phone
        elif not existing.phone:
            existing.phone = f"{employee_id:010d}"[:15]
        if email is not None:
            existing.email = email
        elif not existing.email:
            existing.email = f"employee{employee_id}@test.example"
        existing.role = role
        existing.status = status
        employee = existing
    else:
        employee = make_employee(
            employee_id=employee_id,
            role=role,
            status=status,
            name=name,
            phone=phone,
            email=email,
        )
        db.add(employee)
    if commit:
        await db.commit()
    else:
        await db.flush()
    return employee


async def seed_partner(
    db,
    *,
    partner_id: int | None = None,
    role: str = "phlebo",
    status: str = "active",
    name: str | None = None,
    phone: str | None = None,
    email: str | None = None,
    commit: bool = True,
) -> Partner:
    if partner_id is not None:
        existing = await db.get(Partner, partner_id)
        if existing is not None:
            existing.name = name or existing.name or f"Partner {role}"
            if phone is not None:
                existing.phone = phone
            if email is not None:
                existing.email = email
            if existing.phone is None and existing.email is None:
                existing.phone = f"9{partner_id:09d}"[:15]
            existing.role = role
            existing.status = status
            partner = existing
            if commit:
                await db.commit()
            else:
                await db.flush()
            return partner

    partner = make_partner(
        partner_id=partner_id,
        role=role,
        status=status,
        name=name,
        phone=phone,
        email=email,
    )
    db.add(partner)
    if commit:
        await db.commit()
    else:
        await db.flush()
    return partner
