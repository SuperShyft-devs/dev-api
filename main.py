from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from core.config import settings
from core.exceptions import add_exception_handlers
from core.logging import configure_logging, request_id_middleware
from core.rate_limit import limiter
from modules.auth.router import router as auth_router
from modules.engagements.router import router as engagements_router
from modules.engagements.assessment_packages_router import (
    router as engagement_assessment_packages_router,
)
from modules.checklists.router import router as checklists_router
from modules.organizations.router import router as organizations_router
from modules.users.router import router as users_router
from modules.employee.router import router as employees_router
from modules.assessments.router import router as assessments_router
from modules.assessments.packages_router import router as assessment_packages_router
from modules.questionnaire.router import router as questionnaire_router
from modules.support.router import router as support_router
from modules.diagnostics.router import router as diagnostics_router
from modules.diagnostics.healthians.router import router as healthians_router
from modules.uploads.router import router as uploads_router
from modules.uploads.user_pdf_router import router as user_pdf_upload_router
from modules.reports.router import router as reports_router
from modules.reports.camp_report_sections_router import router as camp_report_sections_router
from modules.platform_settings.router import router as platform_settings_router
from modules.payments.routes import router as payments_router
from modules.bookings.router import router as bookings_router
from modules.experts.router import router as experts_router
from modules.admin_temp.router import router as admin_temp_router
from modules.notifications.router import router as notifications_router
from modules.audit.router import router as audit_router

# Configure logging
configure_logging()

# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="Supershyft health platform",
    version=settings.APP_VERSION,
    debug=settings.DEBUG,
)

add_exception_handlers(app)

# Rate limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.middleware("http")(request_id_middleware)

# Apply CORS policy from environment configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _max_request_body_bytes() -> int:
    return settings.MAX_REQUEST_BODY_MB * 1024 * 1024


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    if settings.is_production():
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
    return response


@app.middleware("http")
async def request_size_limit_middleware(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > _max_request_body_bytes():
        return Response(status_code=413, content="Request body too large")
    return await call_next(request)


# Include routers
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(organizations_router)
app.include_router(engagements_router)
app.include_router(engagement_assessment_packages_router)
app.include_router(checklists_router)
app.include_router(employees_router)
app.include_router(assessments_router)
app.include_router(assessment_packages_router)
app.include_router(questionnaire_router)
app.include_router(support_router)
app.include_router(diagnostics_router)
app.include_router(healthians_router)
app.include_router(uploads_router)
app.include_router(user_pdf_upload_router)
app.include_router(reports_router)
app.include_router(camp_report_sections_router)
app.include_router(platform_settings_router)
app.include_router(payments_router)
app.include_router(bookings_router)
app.include_router(experts_router)
app.include_router(admin_temp_router)
app.include_router(notifications_router)
app.include_router(audit_router)

_media_root = Path(settings.MEDIA_ROOT)
_media_root.mkdir(parents=True, exist_ok=True)
app.mount(
    "/media",
    StaticFiles(directory=str(_media_root)),
    name="media",
)

_pdf_upload_dir = Path(__file__).resolve().parent / "static" / "pdf-upload"
if _pdf_upload_dir.is_dir():
    app.mount(
        "/pdf-upload",
        StaticFiles(directory=str(_pdf_upload_dir), html=True),
        name="pdf-upload",
    )

_payment_test_dir = Path(__file__).resolve().parent / "static" / "payment-test"
if _payment_test_dir.is_dir():
    app.mount(
        "/payment-test",
        StaticFiles(directory=str(_payment_test_dir), html=True),
        name="payment-test",
    )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}
