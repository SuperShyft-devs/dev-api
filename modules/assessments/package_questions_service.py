"""Assessment package category linking service."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.models import AssessmentPackageCategory
from modules.assessments.repository import AssessmentsRepository
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.questionnaire.repository import QuestionnaireRepository


def _normalize_int(value: int) -> int:
    if not isinstance(value, int) or value <= 0:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    return value


class AssessmentPackageCategoriesService:
    """Business logic for package-category linking."""

    def __init__(
        self,
        repository: AssessmentsRepository,
        questionnaire_repository: QuestionnaireRepository,
        audit_service: AuditService | None = None,
    ):
        self._repository = repository
        self._questionnaire_repository = questionnaire_repository
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(status_code=403, error_code="FORBIDDEN", message="You do not have permission to perform this action")

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def list_categories_for_package(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        package_id: int,
    ) -> list[dict]:
        self._ensure_employee_access(employee)

        package_id = _normalize_int(package_id)

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        links = await self._repository.list_package_categories(db, package_id=package_id)
        categories: list[dict] = []
        for link in links:
            category = await self._questionnaire_repository.get_category_by_id(db, link.category_id)
            if category is None:
                continue
            categories.append(
                {
                    "id": link.id,
                    "category_id": category.category_id,
                    "category_key": category.category_key,
                    "display_name": category.display_name,
                }
            )
        return categories

    async def add_categories_to_package(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        package_id: int,
        category_ids: list[int],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)

        package_id = _normalize_int(package_id)

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        if not isinstance(category_ids, list) or len(category_ids) == 0:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        normalized_ids: list[int] = []
        seen: set[int] = set()
        for raw in category_ids:
            category_id = _normalize_int(raw)
            if category_id in seen:
                continue
            seen.add(category_id)
            normalized_ids.append(category_id)

        added: list[int] = []
        skipped: list[int] = []

        for category_id in normalized_ids:
            category = await self._questionnaire_repository.get_category_by_id(db, category_id)
            if category is None:
                raise AppError(
                    status_code=404,
                    error_code="QUESTIONNAIRE_CATEGORY_NOT_FOUND",
                    message="Category does not exist",
                )

            existing = await self._repository.get_package_category_link(db, package_id=package_id, category_id=category_id)
            if existing is not None:
                skipped.append(category_id)
                continue

            link = AssessmentPackageCategory(package_id=package_id, category_id=category_id)
            await self._repository.create_package_category_link(db, link)
            added.append(category_id)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_ADD_ASSESSMENT_PACKAGE_CATEGORIES",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {"package_id": package_id, "added_category_ids": added, "skipped_category_ids": skipped}

    async def remove_category_from_package(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        package_id: int,
        category_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)

        package_id = _normalize_int(package_id)
        category_id = _normalize_int(category_id)

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_PACKAGE_NOT_FOUND", message="Package does not exist")

        deleted = await self._repository.delete_package_category_link(
            db, package_id=package_id, category_id=category_id
        )
        if deleted == 0:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_PACKAGE_CATEGORY_NOT_FOUND",
                message="Category is not attached to this package",
            )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_REMOVE_ASSESSMENT_PACKAGE_CATEGORY",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {"package_id": package_id, "removed_category_id": category_id}
