"""Employee service.

This module owns employee business rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.models import (
    Employee,
    EmployeeCategoryPermission,
    EmployeeRole,
    EmployeeTaskPermission,
)
from modules.employee.permissions import (
    PERMISSION_CATEGORY_KEYS,
    TASK_CATALOG,
    PermissionAction,
    PermissionGrant,
    RouteCapability,
    context_has_capability,
    frozen_grants,
    full_admin_only,
)
from modules.employee.repository import EmployeeRepository
from modules.employee.schemas import (
    CategoryGrantRequest,
    EmployeeCreateRequest,
    EmployeeUpdateRequest,
)


@dataclass(frozen=True)
class EmployeeContext:
    """Authenticated employee context."""

    employee_id: int
    user_id: int
    role: EmployeeRole
    permissions_version: int = 1
    permissions: Mapping[str, PermissionGrant] = field(
        default_factory=lambda: MappingProxyType({})
    )
    task_permissions: Mapping[str, PermissionGrant] = field(
        default_factory=lambda: MappingProxyType({})
    )
    capability: RouteCapability | None = None


_ALLOWED_EMPLOYEE_STATUS = {"active", "inactive", "archived"}
_ALLOWED_EMPLOYEE_STATUS_UPDATE = {"active", "inactive"}
_ALWAYS_ACTIVE_EMPLOYEE_ID = 1


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


class EmployeeService:
    """Employee service layer."""

    def __init__(self, repository: EmployeeRepository, audit_service: AuditService | None = None):
        self._repository = repository
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def _ensure_admin(
        self,
        employee: EmployeeContext | None,
        *,
        action: PermissionAction = PermissionAction.edit,
    ) -> None:
        self._ensure_employee_access(employee)
        if employee.role == EmployeeRole.admin:
            return
        if employee.role == EmployeeRole.inferior_admin and context_has_capability(
            employee, "employees", action
        ):
            return
        if employee.role != EmployeeRole.admin:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def get_active_employee_by_user_id(
        self,
        db: AsyncSession,
        user_id: int,
        *,
        capability: RouteCapability | None = None,
    ) -> EmployeeContext:
        employee = await self._repository.get_by_user_id(db, user_id)
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        if (employee.status or "").lower() != "active":
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        rows = await self._repository.list_permissions(db, employee.employee_id)
        task_rows = await self._repository.list_task_permissions(db, employee.employee_id)
        permissions = frozen_grants(
            {
                row.category_key: PermissionGrant(
                    can_view=bool(row.can_view),
                    can_edit=bool(row.can_edit and row.can_view),
                )
                for row in rows
                if row.category_key in PERMISSION_CATEGORY_KEYS
            }
        )
        return EmployeeContext(
            employee_id=employee.employee_id,
            user_id=employee.user_id,
            role=employee.role,
            permissions_version=employee.permissions_version,
            permissions=permissions,
            task_permissions=MappingProxyType(
                {
                    f"{row.category_key}.{row.task_key}": PermissionGrant(
                        can_view=bool(row.can_view),
                        can_edit=bool(row.can_edit and row.can_view),
                    )
                    for row in task_rows
                    if row.category_key in PERMISSION_CATEGORY_KEYS
                }
            ),
            capability=capability,
        )

    def _ensure_target_allowed(
        self, actor: EmployeeContext, target: Employee, *, changing_privileges: bool = False
    ) -> None:
        if target.employee_id == _ALWAYS_ACTIVE_EMPLOYEE_ID and changing_privileges:
            raise AppError(
                status_code=403,
                error_code="PROTECTED_ADMIN",
                message="This administrator is protected",
            )
        if actor.employee_id == target.employee_id and changing_privileges:
            raise AppError(
                status_code=403,
                error_code="PROTECTED_ADMIN",
                message="You cannot change your own role or permissions",
            )
        if actor.role == EmployeeRole.inferior_admin and target.role in {
            EmployeeRole.admin,
            EmployeeRole.inferior_admin,
        }:
            raise full_admin_only()

    async def _validate_grants(
        self,
        db: AsyncSession,
        grants: list[CategoryGrantRequest],
    ) -> list[CategoryGrantRequest]:
        keys = [item.category_key.strip() for item in grants]
        if len(keys) != len(set(keys)):
            raise AppError(
                status_code=400,
                error_code="INVALID_PERMISSION_CONFIGURATION",
                message="Permission categories must be unique",
            )
        known = {row.category_key for row in await self._repository.list_active_categories(db)}
        if any(key not in known for key in keys):
            raise AppError(
                status_code=400,
                error_code="INVALID_PERMISSION_CONFIGURATION",
                message="Unknown or inactive permission category",
            )
        if any(item.can_edit and not item.can_view for item in grants):
            raise AppError(
                status_code=400,
                error_code="INVALID_PERMISSION_CONFIGURATION",
                message="Edit permission requires View permission",
            )
        for item in grants:
            if item.tasks is None:
                continue
            task_keys = [task.task_key.strip() for task in item.tasks]
            if len(task_keys) != len(set(task_keys)):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_PERMISSION_CONFIGURATION",
                    message="Task permissions must be unique within a category",
                )
            known_tasks = {task[0] for task in TASK_CATALOG[item.category_key.strip()]}
            if set(task_keys) != known_tasks:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_PERMISSION_CONFIGURATION",
                    message="A complete valid task permission snapshot is required",
                )
            if any(task.can_edit and not task.can_view for task in item.tasks):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_PERMISSION_CONFIGURATION",
                    message="Task Edit permission requires View permission",
                )
            has_view = any(task.can_view or task.can_edit for task in item.tasks)
            has_edit = any(task.can_edit for task in item.tasks)
            if item.can_view != has_view or item.can_edit != has_edit:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_PERMISSION_CONFIGURATION",
                    message="Category access must match its configured task permissions",
                )
        return grants

    async def _write_grants(
        self,
        db: AsyncSession,
        *,
        target: Employee,
        actor: EmployeeContext,
        grants: list[CategoryGrantRequest],
    ) -> None:
        validated = await self._validate_grants(db, grants)
        await self._repository.replace_permissions(
            db,
            employee_id=target.employee_id,
            grants=[
                EmployeeCategoryPermission(
                    employee_id=target.employee_id,
                    category_key=item.category_key.strip(),
                    can_view=item.can_view,
                    can_edit=item.can_edit,
                    granted_by_employee_id=actor.employee_id,
                )
                for item in validated
                if item.can_view or item.can_edit
            ],
        )
        await self._repository.replace_task_permissions(
            db,
            employee_id=target.employee_id,
            grants=[
                EmployeeTaskPermission(
                    employee_id=target.employee_id,
                    category_key=item.category_key.strip(),
                    task_key=task.task_key.strip(),
                    can_view=task.can_view,
                    can_edit=task.can_edit,
                    granted_by_employee_id=actor.employee_id,
                )
                for item in validated
                if item.tasks is not None
                for task in item.tasks
            ],
        )

    async def _recheck_management_actor(
        self,
        db: AsyncSession,
        actor: EmployeeContext,
        *,
        full_admin_only_required: bool = False,
    ) -> Employee:
        """Lock and revalidate the actor so concurrent revocation wins."""
        current = await self._repository.get_by_id_for_update(db, actor.employee_id)
        if current is None or (current.status or "").lower() != "active":
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
        if current.role == EmployeeRole.admin:
            return current
        if full_admin_only_required:
            raise full_admin_only()
        if current.role != EmployeeRole.inferior_admin or actor.capability is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
        rows = await self._repository.list_permissions(db, current.employee_id)
        current_grant = next(
            (row for row in rows if row.category_key == "employees"), None
        )
        if current_grant is None or not current_grant.can_edit:
            from modules.employee.permissions import permission_denied

            raise permission_denied("employees", PermissionAction.edit)
        return current

    async def create_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: EmployeeCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Employee:
        self._ensure_admin(employee)
        await self._recheck_management_actor(db, employee)

        if employee.role == EmployeeRole.inferior_admin and payload.role in {
            EmployeeRole.admin,
            EmployeeRole.inferior_admin,
        }:
            raise full_admin_only()
        if payload.role == EmployeeRole.inferior_admin and payload.permissions is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_PERMISSION_CONFIGURATION",
                message="A complete permission snapshot is required",
            )
        if payload.role != EmployeeRole.inferior_admin and payload.permissions is not None:
            raise AppError(
                status_code=400,
                error_code="INVALID_EMPLOYEE_ROLE",
                message="Permissions are only valid for Inferior Admin employees",
            )

        status_value = _normalize_status(payload.status)
        if status_value not in _ALLOWED_EMPLOYEE_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        existing = await self._repository.get_by_user_id(db, payload.user_id)
        if existing is not None:
            raise AppError(status_code=409, error_code="EMPLOYEE_ALREADY_EXISTS", message="Employee already exists")

        row = Employee(user_id=payload.user_id, role=payload.role, status=status_value)
        try:
            row = await self._repository.create(db, row)
        except IntegrityError as exc:
            raise AppError(
                status_code=409,
                error_code="EMPLOYEE_ALREADY_EXISTS",
                message="Employee already exists",
            ) from exc
        if payload.role == EmployeeRole.inferior_admin:
            await self._write_grants(
                db, target=row, actor=employee, grants=payload.permissions or []
            )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_EMPLOYEE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row

    async def list_employees(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        status: str | None,
        role: str | None,
        user_id: int | None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[tuple[Employee, str | None, str | None]], int]:
        self._ensure_admin(employee, action=PermissionAction.view)

        status_value = None
        if status is not None:
            normalized = _normalize_status(status)
            if normalized not in _ALLOWED_EMPLOYEE_STATUS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            status_value = normalized

        employees = await self._repository.list_employees(
            db,
            page=page,
            limit=limit,
            status=status_value,
            role=role,
            user_id=user_id,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        total = await self._repository.count_employees(
            db,
            status=status_value,
            role=role,
            user_id=user_id,
            search=search,
        )

        return employees, total

    async def get_employee_details(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        employee_id: int,
    ) -> tuple[Employee, str | None, str | None]:
        self._ensure_admin(employee, action=PermissionAction.view)

        row = await self._repository.get_by_id_with_user_names(db, employee_id)
        if row is None:
            raise AppError(status_code=404, error_code="EMPLOYEE_NOT_FOUND", message="Employee does not exist")

        return row

    async def update_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        employee_id: int,
        payload: EmployeeUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Employee:
        self._ensure_admin(employee)

        await self._recheck_management_actor(db, employee)
        row = await self._repository.get_by_id_for_update(db, employee_id)
        if row is None:
            raise AppError(status_code=404, error_code="EMPLOYEE_NOT_FOUND", message="Employee does not exist")
        role_changes = payload.role != row.role
        grants_change = payload.permissions is not None
        self._ensure_target_allowed(
            employee,
            row,
            changing_privileges=(
                role_changes or grants_change or payload.user_id != row.user_id
            ),
        )
        if employee.role == EmployeeRole.inferior_admin and payload.role in {
            EmployeeRole.admin,
            EmployeeRole.inferior_admin,
        }:
            raise full_admin_only()
        if payload.role == EmployeeRole.inferior_admin and role_changes and payload.permissions is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_PERMISSION_CONFIGURATION",
                message="A complete permission snapshot is required",
            )
        if payload.role != EmployeeRole.inferior_admin and payload.permissions is not None:
            raise AppError(
                status_code=400,
                error_code="INVALID_EMPLOYEE_ROLE",
                message="Permissions are only valid for Inferior Admin employees",
            )
        if payload.permissions is not None and payload.expected_version is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_PERMISSION_CONFIGURATION",
                message="expected_version is required when replacing permissions",
            )
        if payload.expected_version is not None and payload.expected_version != row.permissions_version:
            raise AppError(
                status_code=409,
                error_code="PERMISSIONS_VERSION_CONFLICT",
                message="Permissions changed elsewhere",
            )
        if row.role == EmployeeRole.admin and payload.role != EmployeeRole.admin:
            active_admins = await self._repository.lock_active_admins(db)
            if len(active_admins) <= 1:
                raise AppError(
                    status_code=403,
                    error_code="PROTECTED_ADMIN",
                    message="The last active administrator is protected",
                )

        if payload.user_id != row.user_id:
            existing = await self._repository.get_by_user_id(db, payload.user_id)
            if existing is not None and existing.employee_id != row.employee_id:
                raise AppError(status_code=409, error_code="EMPLOYEE_ALREADY_EXISTS", message="Employee already exists")
            row.user_id = payload.user_id

        row.role = payload.role
        if payload.role == EmployeeRole.inferior_admin and payload.permissions is not None:
            await self._write_grants(
                db, target=row, actor=employee, grants=payload.permissions
            )
            row.permissions_version += 1
        elif payload.role != EmployeeRole.inferior_admin:
            await self._repository.replace_permissions(
                db, employee_id=row.employee_id, grants=[]
            )
            await self._repository.replace_task_permissions(
                db, employee_id=row.employee_id, grants=[]
            )
            if role_changes:
                row.permissions_version += 1
        row = await self._repository.update(db, row)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_EMPLOYEE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row

    async def change_employee_status(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        employee_id: int,
        status: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Employee:
        self._ensure_admin(employee)

        await self._recheck_management_actor(db, employee)
        row = await self._repository.get_by_id_for_update(db, employee_id)
        if row is None:
            raise AppError(status_code=404, error_code="EMPLOYEE_NOT_FOUND", message="Employee does not exist")

        normalized = _normalize_status(status)
        if normalized not in _ALLOWED_EMPLOYEE_STATUS_UPDATE:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        changing = normalized != (row.status or "").lower()
        self._ensure_target_allowed(employee, row, changing_privileges=changing)
        if row.role == EmployeeRole.admin and normalized != "active":
            active_admins = await self._repository.lock_active_admins(db)
            if len(active_admins) <= 1:
                raise AppError(
                    status_code=403,
                    error_code="PROTECTED_ADMIN",
                    message="The last active administrator is protected",
                )

        row.status = normalized
        row = await self._repository.update(db, row)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_EMPLOYEE_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row

    async def list_permission_categories(
        self, db: AsyncSession, *, employee: EmployeeContext
    ):
        if employee.role != EmployeeRole.admin:
            raise full_admin_only()
        return await self._repository.list_active_categories(db)

    async def get_employee_permissions(
        self, db: AsyncSession, *, employee: EmployeeContext, employee_id: int
    ) -> tuple[
        Employee,
        list[EmployeeCategoryPermission],
        list[EmployeeTaskPermission],
    ]:
        if employee.role != EmployeeRole.admin:
            raise full_admin_only()
        target = await self._repository.get_by_id(db, employee_id)
        if target is None:
            raise AppError(
                status_code=404,
                error_code="EMPLOYEE_NOT_FOUND",
                message="Employee does not exist",
            )
        return (
            target,
            await self._repository.list_permissions(db, employee_id),
            await self._repository.list_task_permissions(db, employee_id),
        )

    async def replace_employee_permissions(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        employee_id: int,
        expected_version: int,
        permissions: list[CategoryGrantRequest],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Employee:
        if employee.role != EmployeeRole.admin:
            raise full_admin_only()
        await self._recheck_management_actor(
            db, employee, full_admin_only_required=True
        )
        target = await self._repository.get_by_id_for_update(db, employee_id)
        if target is None:
            raise AppError(
                status_code=404,
                error_code="EMPLOYEE_NOT_FOUND",
                message="Employee does not exist",
            )
        self._ensure_target_allowed(employee, target, changing_privileges=True)
        if target.role != EmployeeRole.inferior_admin:
            raise AppError(
                status_code=400,
                error_code="INVALID_EMPLOYEE_ROLE",
                message="Permissions are only valid for Inferior Admin employees",
            )
        if target.permissions_version != expected_version:
            raise AppError(
                status_code=409,
                error_code="PERMISSIONS_VERSION_CONFLICT",
                message="Permissions changed elsewhere",
            )
        await self._write_grants(
            db, target=target, actor=employee, grants=permissions
        )
        target.permissions_version += 1
        await self._repository.update(db, target)
        await self._require_audit_service().log_event(
            db,
            action=(
                "EMPLOYEE_REPLACE_PERMISSIONS "
                f"target={target.employee_id} version={expected_version}->{target.permissions_version}"
            ),
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return target
