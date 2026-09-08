"""Route-bound, fail-closed Inferior Admin authorization policy."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from fastapi import Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from core.security import decode_and_verify_jwt
from db.session import get_db
from modules.employee.models import (
    Employee,
    EmployeeCategoryPermission,
    EmployeeRole,
    EmployeeTaskPermission,
)


class PermissionAction(str, Enum):
    view = "view"
    edit = "edit"


CATEGORY_LABELS: Mapping[str, str] = MappingProxyType(
    {
        "users": "Users",
        "organizations": "Organizations",
        "engagements": "Engagements",
        "engagement_console": "Engagement Console",
        "assessments": "Assessments",
        "diagnostics": "Diagnostics",
        "reports": "Reports",
        "experts": "Experts",
        "payments_bookings": "Payments & Bookings",
        "notifications": "Notifications",
        "checklists_tasks": "Checklists & Tasks",
        "support": "Support",
        "employees": "Employees",
        "platform_settings": "Platform Settings",
        "system_monitoring": "System Monitoring",
    }
)
PERMISSION_CATEGORY_KEYS = frozenset(CATEGORY_LABELS)
TASK_CATALOG: Mapping[str, tuple[tuple[str, str, str], ...]] = MappingProxyType(
    {
        "users": (
            ("directory", "User directory", "List and view users"),
            ("profiles", "User profiles", "Create and update user profiles"),
            ("participant_journeys", "Participant journeys", "View participant assessment journeys"),
            ("metsights_sync", "Metsights sync", "Manage user Metsights records and synchronization"),
            ("import_export", "Import & export", "Import or export user records"),
        ),
        "organizations": (
            ("organizations", "Organizations", "Manage organization records and contacts"),
            ("industries", "Industries", "Manage organization industries"),
            ("camps", "Camps", "Manage organization camps"),
            ("participants", "Organization participants", "View organization participant lists"),
            ("assets", "Organization assets", "Manage organization logos and assets"),
            ("geocoding", "Addresses & geocoding", "Resolve and update organization addresses"),
        ),
        "engagements": (
            ("records", "Engagement records", "Manage engagement setup and status"),
            ("participants", "Participants", "Assign and manage engagement participants"),
            ("assessment_assignments", "Assessment assignments", "Assign assessment packages to engagements"),
            ("integrations", "Integrations", "Manage external profiles, questionnaire pushes, and data checks"),
            ("staffing", "Staffing", "Manage onboarding assistants and phlebotomists"),
        ),
        "engagement_console": (
            ("operations", "Console operations", "View and operate the engagement console"),
            ("bookings", "Bookings", "Create, change, and cancel console bookings"),
            ("assessments", "Assessments", "View and submit participant assessments"),
            ("questionnaires", "Questionnaires", "Enter and submit participant questionnaire answers"),
            ("home_collection", "Home collection", "Check availability, lock slots, and book home collections"),
        ),
        "assessments": (
            ("packages", "Packages", "Manage assessment packages"),
            ("categories", "Categories", "Manage questionnaire categories"),
            ("questions", "Questions", "Manage questions, rules, and ordering"),
            ("responses", "Responses", "View and update participant assessment responses"),
            ("integrations", "Integrations", "Import, draft, and synchronize assessment data"),
            ("question_integrations", "Question integrations", "Manage question-level Metsights synchronization"),
            ("healthy_habit_rules", "Healthy habit rules", "Manage question healthy-habit rules"),
            ("category_questions", "Category questions", "Assign and order questions within categories"),
        ),
        "diagnostics": (
            ("packages", "Packages", "Manage diagnostic packages"),
            ("tests_groups", "Tests & groups", "Manage diagnostic tests and test groups"),
            ("filter_chips", "Filter chips", "Manage diagnostic filters"),
            ("integrations", "Healthians mapping", "Manage external diagnostic mappings"),
            ("package_metadata", "Package metadata", "Manage package tags, reasons, samples, and preparations"),
            ("health_parameters", "Health parameters", "Manage diagnostic health parameters"),
            ("package_test_groups", "Package test groups", "Assign and order test groups within packages"),
            ("assets", "Package assets", "Manage diagnostic package images"),
        ),
        "reports": (
            ("camp_reports", "Camp reports", "View, initialize, validate, and refresh camp reports"),
            ("participant_reports", "Participant reports", "Manage participant blood and Bio-AI reports"),
            ("report_sections", "Report sections", "Manage report section definitions"),
            ("import_export", "Import & export", "Import or export report data"),
        ),
        "experts": (
            ("experts", "Experts", "Manage expert records"),
            ("expert_types", "Expert types", "Manage expert type definitions"),
            ("consultations", "Consultations", "Manage consultation availability and bookings"),
            ("tags", "Expert tags", "Manage expert tags"),
            ("reviews", "Expert reviews", "View and manage expert reviews"),
            ("availability", "Availability", "Manage expert availability"),
            ("overrides", "Availability overrides", "Manage expert availability overrides"),
        ),
        "payments_bookings": (
            ("payments", "Payments", "View and manage payment records"),
            ("bookings", "Bookings", "View and manage booking records"),
        ),
        "notifications": (
            ("messages", "Messages", "Send and manage notifications"),
            ("dispatch", "Dispatch", "Dispatch notifications and reminders"),
            ("services", "Services", "Manage notification service integrations"),
            ("events", "Events", "Manage notification event definitions"),
            ("defaults", "Engagement defaults", "Manage engagement notification defaults"),
            ("engagement_configuration", "Engagement configuration", "Configure notifications for an engagement"),
        ),
        "checklists_tasks": (
            ("templates", "Templates", "Manage checklist templates"),
            ("template_items", "Template items", "Manage checklist items inside templates"),
            ("tasks", "Tasks", "Manage checklist tasks and status"),
            ("assignments", "Engagement assignments", "Assign checklists to engagements"),
            ("task_assignment", "Task assignment", "Assign checklist tasks to employees"),
            ("readiness", "Readiness", "View engagement checklist readiness"),
        ),
        "support": (
            ("tickets", "Tickets", "View and manage support tickets"),
        ),
        "employees": (
            ("directory", "Employee directory", "List and view employees"),
            ("create_update", "Create & update", "Create employees and update employee details"),
            ("status", "Employee status", "Activate or deactivate employees"),
        ),
        "platform_settings": (
            ("settings", "Platform settings", "Manage platform-wide settings"),
            ("engagement_types", "Engagement types", "Manage engagement type definitions"),
            ("b2c_onboarding", "B2C onboarding", "Manage B2C onboarding defaults"),
            ("default_assistants", "Default assistants", "Manage default onboarding assistants"),
            ("support_notifications", "Support notifications", "Manage support notification settings"),
            ("metsights_import", "Metsights import", "Import Metsights profiles"),
            ("engagement_sync", "Engagement sync", "Synchronize engagement data"),
        ),
        "system_monitoring": (
            ("server_health", "Server health", "View server and database health"),
            ("audit_logs", "Audit logs", "View administrative audit history"),
            ("database_backup", "Database backup", "Export database backups"),
            ("maintenance", "Maintenance", "Run protected maintenance operations"),
        ),
    }
)


@dataclass(frozen=True)
class PermissionGrant:
    can_view: bool
    can_edit: bool


@dataclass(frozen=True)
class RouteCapability:
    category: str
    task_key: str
    action: PermissionAction
    method: str
    route_template: str


def frozen_grants(
    values: Mapping[str, PermissionGrant] | None = None,
) -> Mapping[str, PermissionGrant]:
    return MappingProxyType(dict(values or {}))


def permission_denied(
    category: str, action: PermissionAction, task_key: str | None = None
) -> AppError:
    label = CATEGORY_LABELS.get(category, category.replace("_", " ").title())
    details = {"category": category, "action": action.value}
    if task_key is not None:
        details["task_key"] = task_key
    return AppError(
        status_code=403,
        error_code="PERMISSION_DENIED",
        message=f"You do not have permission to {action.value} {label}.",
        details=details,
    )


def full_admin_only() -> AppError:
    return AppError(
        status_code=403,
        error_code="PERMISSION_DENIED",
        message="Only administrators can manage roles and permissions.",
        details={"category": "employees", "action": "edit"},
    )


def grant_allows(grant: PermissionGrant | None, action: PermissionAction) -> bool:
    if grant is None:
        return False
    if action is PermissionAction.edit:
        return bool(grant.can_edit)
    return bool(grant.can_view or grant.can_edit)


def context_task_allows(
    employee,
    category: str,
    task_key: str,
    action: PermissionAction,
) -> bool:
    """Check a task grant, falling back to category access for legacy snapshots."""

    if employee.role == EmployeeRole.admin:
        return True
    if employee.role != EmployeeRole.inferior_admin:
        return False
    if not grant_allows(getattr(employee, "permissions", {}).get(category), action):
        return False
    task_permissions = getattr(employee, "task_permissions", {})
    prefix = f"{category}."
    category_tasks = {
        key: grant for key, grant in task_permissions.items() if key.startswith(prefix)
    }
    if not category_tasks:
        return True
    return grant_allows(category_tasks.get(f"{category}.{task_key}"), action)


def context_has_capability(employee, category: str, action: PermissionAction) -> bool:
    if employee.role == EmployeeRole.admin:
        return True
    if employee.role != EmployeeRole.inferior_admin:
        return False
    capability = getattr(employee, "capability", None)
    if capability is None or capability.category != category:
        return False
    if action is PermissionAction.edit and capability.action is not PermissionAction.edit:
        return False
    return context_task_allows(
        employee, category, capability.task_key, action
    )


def _excluded(path: str, method: str) -> bool:
    if path in {"/health", "/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}:
        return True
    if path.startswith(("/auth", "/webhooks", "/media", "/pdf-upload", "/payment-test")):
        return True
    if path.startswith("/experts/portal"):
        return True
    if path in {
        "/experts",
        "/experts/{expert_id}",
        "/experts/consultations/slots",
        "/experts/consultations/book",
        "/experts/consultations/reschedule",
    } and (method == "GET" or "/consultations/" in path):
        return True
    if path == "/expert-types" and method == "GET":
        return True
    if path.startswith("/book"):
        return True
    if path.startswith("/users/public") or path.startswith("/users/code/"):
        return True
    if path.startswith("/users/me") or path.endswith("/book-bio-ai"):
        return True
    if path.endswith("/metsights/sync-records"):
        return True
    if path == "/users" and method == "POST":
        return True
    if path == "/support/tickets" and method == "POST":
        return True
    if path.startswith("/reports") and not path.startswith(
        ("/reports/camps", "/reports/camp-sections")
    ):
        return True
    if path.startswith("/assessments") and not (
        path == "/assessments/{assessment_id}/metsights-record-id"
        and method == "PUT"
    ):
        return True
    if path in {
        "/questionnaire/{assessment_instance_id}/category/{category_id}",
        "/questionnaire/{assessment_instance_id}/category/{category_id}/responses",
    }:
        return True
    if path.startswith(("/engagements/public", "/engagements/code/")):
        return True
    if path == "/engagements/me/{engagement_id}":
        return True
    if path in {
        "/engagements/{engagement_id}/consultation",
        "/engagements/{engagement_id}/consultation/{consultation_id}/consent",
    }:
        return True
    if path == "/engagements/{engagement_id}/checklists" and method == "GET":
        return True
    if path == "/notifications/callback":
        return True
    if path.startswith("/payments") and path not in {
        "/payments/bookings",
    }:
        return True
    if path == "/checklist/my-tasks":
        return True
    if path in {
        "/checklist/tasks/{task_id}/status",
        "/checklist/tasks/{task_id}",
    }:
        return True
    if path in {
        "/uploads/consultation-attachments",
        "/uploads/bio-ai/pdf",
        "/uploads/blood-parameters/pdf",
    }:
        return True
    return False


def _task_for_operation(category: str, path: str, method: str) -> str:
    """Map a classified route to one stable configurable task."""

    if category == "users":
        if "participant-journey" in path:
            return "participant_journeys"
        if "metsights" in path:
            return "metsights_sync"
        if "import" in path or "export" in path:
            return "import_export"
        return "directory" if method == "GET" else "profiles"
    if category == "organizations":
        if "industr" in path:
            return "industries"
        if "/participants" in path:
            return "participants"
        if path.startswith("/uploads/organizations"):
            return "assets"
        if "camp" in path:
            return "camps"
        if path.startswith("/geocode"):
            return "geocoding"
        return "organizations"
    if category == "engagements":
        if "onboarding-assistant" in path or "phlebo" in path:
            return "staffing"
        if "assessment-package" in path:
            return "assessment_assignments"
        if any(
            marker in path
            for marker in (
                "resolve-healthians-zone",
                "create-metsights-profiles",
                "questionnaire-status",
                "data-completeness",
                "push-questionnaires",
                "connect-metsights-records",
            )
        ):
            return "integrations"
        if "participant" in path:
            return "participants"
        return "records"
    if category == "engagement_console":
        if "home-collection" in path or "service-availability" in path:
            return "home_collection"
        if "/assessments" in path and "questionnaire" not in path:
            return "assessments"
        if "/book" in path:
            return "bookings"
        if "questionnaire" in path or "answer" in path:
            return "questionnaires"
        return "operations"
    if category == "assessments":
        if path.startswith("/assessment-packages"):
            return "packages"
        if "healthy-habit-rules" in path:
            return "healthy_habit_rules"
        if "/categories/" in path and "/questions" in path:
            return "category_questions"
        if (
            "metsights-sync" in path
            or "sync-gaps" in path
            or "blood-parameters/reload" in path
        ):
            return "question_integrations"
        if "categor" in path:
            return "categories"
        if "question" in path:
            return "questions"
        if "metsights" in path or "draft" in path or "sync" in path or "push" in path:
            return "integrations"
        return "responses"
    if category == "diagnostics":
        if "filter-chip" in path:
            return "filter_chips"
        if path.startswith("/uploads/packages"):
            return "assets"
        if "/test-groups" in path and path.startswith("/diagnostic-packages"):
            return "package_test_groups"
        if any(marker in path for marker in ("/reasons", "/tags", "/samples", "/preparations")):
            return "package_metadata"
        if "health-parameters" in path:
            return "health_parameters"
        if path.startswith("/healthians"):
            return "integrations"
        if path.startswith("/diagnostic-packages"):
            return "packages"
        return "tests_groups"
    if category == "reports":
        if "camp-sections" in path:
            return "report_sections"
        if "/camps" in path:
            return "camp_reports"
        if "export" in path or "upload" in path:
            return "import_export"
        return "participant_reports"
    if category == "experts":
        if path.startswith("/expert-types"):
            return "expert_types"
        if "/tags" in path:
            return "tags"
        if "/reviews" in path:
            return "reviews"
        if "/overrides" in path:
            return "overrides"
        if "/availability" in path:
            return "availability"
        if "consultation" in path or "slot" in path:
            return "consultations"
        return "experts"
    if category == "payments_bookings":
        return "bookings" if "booking" in path else "payments"
    if category == "notifications":
        if "dispatch" in path or "prepare-reports" in path:
            return "dispatch"
        if "services" in path:
            return "services"
        if "event" in path:
            return "events"
        if path.startswith("/engagements"):
            return "engagement_configuration"
        if "default" in path:
            return "defaults"
        return "messages"
    if category == "checklists_tasks":
        if path.endswith("/readiness"):
            return "readiness"
        if "/tasks/" in path and path.endswith("/assign"):
            return "task_assignment"
        if "/items" in path:
            return "template_items"
        if "template" in path:
            return "templates"
        if path.startswith("/engagements"):
            return "assignments"
        return "tasks"
    if category == "support":
        return "tickets"
    if category == "employees":
        if path.endswith("/status"):
            return "status"
        return "directory" if method == "GET" else "create_update"
    if category == "platform_settings":
        if path.startswith("/engagement-types"):
            return "engagement_types"
        if path.endswith("/b2c-onboarding"):
            return "b2c_onboarding"
        if path.endswith("/default-onboarding-assistants"):
            return "default_assistants"
        if path.endswith("/support-query-notification"):
            return "support_notifications"
        if "metsights-profiles" in path:
            return "metsights_import"
        if "engagements-sync" in path:
            return "engagement_sync"
        return "settings"
    if category == "system_monitoring":
        if path == "/employees/database-backup":
            return "database_backup"
        if path.startswith("/audit"):
            return "audit_logs"
        if path.startswith(("/server-health", "/health/db")):
            return "server_health"
        return "maintenance"
    raise AppError(
        status_code=403,
        error_code="PERMISSION_DENIED",
        message="This operation does not have a configured task permission.",
        details={"category": category, "action": "unclassified_task"},
    )


def classify_operation(route_template: str, method: str) -> RouteCapability | None:
    """Classify every admin operation; ``None`` means an explicit exclusion."""

    path = route_template.rstrip("/") or "/"
    method = method.upper()
    if _excluded(path, method):
        return None

    category: str | None = None
    action = PermissionAction.view if method == "GET" else PermissionAction.edit

    if path == "/employees/database-backup":
        category, action = "system_monitoring", PermissionAction.edit
    elif path.startswith("/employees/users"):
        category = "users"
    elif path.startswith("/employees"):
        category = "employees"
    elif "/console" in path:
        category = "engagement_console"
    elif path.startswith("/engagements") and (
        "/checklists" in path or path.endswith("/readiness")
    ):
        category = "checklists_tasks"
    elif path.startswith("/engagements") and (
        "/participants/load-" in path or "/participants/remove-reports" in path
    ):
        category = "reports"
    elif path.startswith(("/checklist", "/checklist-templates")):
        category = "checklists_tasks"
    elif path.startswith("/engagements") and "/notifications" in path:
        category = "notifications"
    elif path.startswith("/engagements"):
        category = "engagements"
    elif path.startswith("/organizations"):
        category = "organizations"
    elif path.startswith("/geocode"):
        category, action = "organizations", PermissionAction.edit
    elif path.startswith("/users"):
        category = "users"
    elif path.startswith(("/assessments", "/assessment-packages", "/questionnaire")):
        category = "assessments"
    elif path.startswith(
        (
            "/diagnostics",
            "/diagnostic-packages",
            "/diagnostic-test-groups",
            "/healthians",
        )
    ):
        category = "diagnostics"
    elif path.startswith(("/reports", "/bioai-report")):
        category = "reports"
    elif path.startswith("/experts") or path.startswith("/expert-types"):
        category = "experts"
    elif path.startswith("/payments"):
        category = "payments_bookings"
    elif path.startswith(("/notifications", "/notification-events")):
        category = "notifications"
    elif path.startswith("/support"):
        category = "support"
    elif path == "/platform-settings/engagement-notification-defaults":
        category = "notifications"
    elif path.startswith(("/platform-settings", "/engagement-types")):
        category = "platform_settings"
    elif path.startswith(("/server-health", "/health/db", "/audit", "/admin-temp")):
        category = "system_monitoring"
    elif path.startswith("/uploads/users"):
        category = "users"
    elif path.startswith("/uploads/organizations"):
        category = "organizations"
    elif path.startswith("/uploads/packages"):
        category = "diagnostics"
    elif path.startswith("/uploads/experts"):
        category = "experts"
    elif path.startswith("/uploads/"):
        category = "reports"

    if category is None:
        # Matched routes must be deliberately classified or excluded.
        raise AppError(
            status_code=403,
            error_code="PERMISSION_DENIED",
            message="You do not have permission to access this operation.",
            details={"category": "unclassified", "action": action.value},
        )
    return RouteCapability(
        category,
        _task_for_operation(category, path, method),
        action,
        method,
        route_template,
    )


async def authorize_inferior_admin_request(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> None:
    """Authorize an Inferior Admin after FastAPI resolves the route template."""

    raw = request.headers.get("Authorization", "")
    token = raw[7:].strip() if raw.lower().startswith("bearer ") else ""
    if not token and request.url.path == "/employees/database-backup":
        token = (request.query_params.get("access_token") or "").strip()
    if not token:
        return
    if request.headers.get("x-api-key") and request.url.path.startswith("/notifications"):
        return
    try:
        user_id = int(decode_and_verify_jwt(token).get("sub"))
    except Exception:
        return  # The route's normal auth dependency owns the 401 contract.

    result = await db.execute(select(Employee).where(Employee.user_id == user_id))
    employee = result.scalar_one_or_none()
    if employee is None or employee.role != EmployeeRole.inferior_admin:
        return
    if (employee.status or "").lower() != "active":
        return

    route = request.scope.get("route")
    route_template = getattr(route, "path", request.url.path)
    capability = classify_operation(route_template, request.method)
    request.state.rbac_capability = capability
    if capability is None:
        return
    if route_template in {
        "/employees/permission-categories",
        "/employees/{employee_id}/permissions",
    }:
        raise full_admin_only()

    grant_result = await db.execute(
        select(EmployeeCategoryPermission).where(
            EmployeeCategoryPermission.employee_id == employee.employee_id,
            EmployeeCategoryPermission.category_key == capability.category,
        )
    )
    row = grant_result.scalar_one_or_none()
    grant = (
        PermissionGrant(can_view=bool(row.can_view), can_edit=bool(row.can_edit))
        if row is not None
        else None
    )
    if not grant_allows(grant, capability.action):
        raise permission_denied(capability.category, capability.action)

    task_result = await db.execute(
        select(EmployeeTaskPermission).where(
            EmployeeTaskPermission.employee_id == employee.employee_id,
            EmployeeTaskPermission.category_key == capability.category,
        )
    )
    task_rows = list(task_result.scalars().all())
    if task_rows:
        task_row = next(
            (item for item in task_rows if item.task_key == capability.task_key),
            None,
        )
        task_grant = (
            PermissionGrant(
                can_view=bool(task_row.can_view),
                can_edit=bool(task_row.can_edit),
            )
            if task_row is not None
            else None
        )
        if not grant_allows(task_grant, capability.action):
            raise permission_denied(
                capability.category, capability.action, capability.task_key
            )
