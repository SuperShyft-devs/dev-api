"""Recipient type strings for department-based notifications."""

from __future__ import annotations

from modules.employee.models import EmployeeDepartmentName

RECIPIENT_DEPARTMENTS_ALL = "departments"
RECIPIENT_DEPARTMENTS_OPS = "departments.ops"
RECIPIENT_DEPARTMENTS_TECH = "departments.tech"
RECIPIENT_DEPARTMENTS_DESIGN = "departments.design"

DEPARTMENT_RECIPIENT_TYPES = frozenset(
    {
        RECIPIENT_DEPARTMENTS_ALL,
        RECIPIENT_DEPARTMENTS_OPS,
        RECIPIENT_DEPARTMENTS_TECH,
        RECIPIENT_DEPARTMENTS_DESIGN,
    }
)


def department_for_recipient_type(recipient_type: str) -> EmployeeDepartmentName | None:
    rt = (recipient_type or "").strip()
    if rt == RECIPIENT_DEPARTMENTS_ALL:
        return None
    if rt == RECIPIENT_DEPARTMENTS_OPS:
        return EmployeeDepartmentName.ops
    if rt == RECIPIENT_DEPARTMENTS_TECH:
        return EmployeeDepartmentName.tech
    if rt == RECIPIENT_DEPARTMENTS_DESIGN:
        return EmployeeDepartmentName.design
    return None
