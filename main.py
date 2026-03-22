from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import settings
from core.exceptions import add_exception_handlers
from core.logging import configure_logging, request_id_middleware
from modules.auth.router import router as auth_router
from modules.engagements.router import router as engagements_router
from modules.checklists.router import router as checklists_router
from modules.organizations.router import router as organizations_router
from modules.users.router import router as users_router
from modules.employee.router import router as employees_router
from modules.assessments.router import router as assessments_router
from modules.assessments.packages_router import router as assessment_packages_router
from modules.questionnaire.router import router as questionnaire_router
from modules.support.router import router as support_router
from modules.diagnostics.router import router as diagnostics_router
from modules.uploads.router import router as uploads_router

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
app.middleware("http")(request_id_middleware)

# Apply CORS policy from environment configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(organizations_router)
app.include_router(engagements_router)
app.include_router(checklists_router)
app.include_router(employees_router)
app.include_router(assessments_router)
app.include_router(assessment_packages_router)
app.include_router(questionnaire_router)
app.include_router(support_router)
app.include_router(diagnostics_router)
app.include_router(uploads_router)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}
