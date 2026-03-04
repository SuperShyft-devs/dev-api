"""Assessment packages service.

These endpoints are employee-only.

Business rules:
- Only employees can manage packages.
- Only allowed status values are accepted.
- Package codes must be unique.
- All mutations must be audit logged.
"""

from __future__ import annotations

from core.exceptions import AppError
from modules.assessments.models import AssessmentPackage
from modules.assessments.repository import AssessmentsRepository
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext


_ALLOWED_PACKAGE_STATUS = {"active", "inactive", "archived"}


def _normalize(value: str | None) -> str:
    return (value or "").strip()


def _normalize_status(value: str | None) -> str:
    return _normalize(value).lower()


class AssessmentPackagesService:
    def __init__(self, repository: AssessmentsRepository, audit_service: AuditService | None = None):
        self._repository = repository
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def create_package_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_code: str,
        display_name: str,
        status: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> AssessmentPackage:
        self._ensure_employee_access(employee)

        code = _normalize(package_code)
        name = _normalize(display_name)
        status_value = _normalize_status(status)

        if not code or not name:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        if status_value not in _ALLOWED_PACKAGE_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        existing = await self._repository.get_package_by_code(db, package_code=code)
        if existing is not None:
            raise AppError(status_code=409, error_code="ASSESSMENT_PACKAGE_ALREADY_EXISTS", message="Package already exists")

        package = AssessmentPackage(package_code=code, display_name=name, status=status_value)
        package = await self._repository.create_package(db, package)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_ASSESSMENT_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return package

    async def list_packages_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        status: str | None,
    ) -> tuple[list[AssessmentPackage], int]:
        self._ensure_employee_access(employee)

        status_value = None
        if status is not None:
            normalized = _normalize_status(status)
            if normalized not in _ALLOWED_PACKAGE_STATUS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            status_value = normalized

        packages = await self._repository.list_packages(db, page=page, limit=limit, status=status_value)
        total = await self._repository.count_packages(db, status=status_value)
        return packages, total

    async def get_package_details_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
    ) -> AssessmentPackage:
        self._ensure_employee_access(employee)

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        return package

    async def update_package_status_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        status: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> AssessmentPackage:
        self._ensure_employee_access(employee)

        status_value = _normalize_status(status)
        if status_value not in _ALLOWED_PACKAGE_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        package.status = status_value
        package = await self._repository.update_package(db, package)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_ASSESSMENT_PACKAGE_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return package

    async def update_package_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        package_code: str,
        display_name: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> AssessmentPackage:
        self._ensure_employee_access(employee)

        code = _normalize(package_code)
        name = _normalize(display_name)

        if not code or not name:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        existing = await self._repository.get_package_by_code(db, package_code=code)
        if existing is not None and existing.package_id != package.package_id:
            raise AppError(status_code=409, error_code="ASSESSMENT_PACKAGE_ALREADY_EXISTS", message="Package already exists")

        package.package_code = code
        package.display_name = name
        package = await self._repository.update_package(db, package)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_ASSESSMENT_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return package
