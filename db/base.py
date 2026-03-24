"""
Database base configuration.

This module provides the declarative base for SQLAlchemy models.
All models across modules will inherit from this base.

IMPORTANT: All models must be imported here so SQLAlchemy can discover them
and resolve foreign key relationships properly.
"""

from sqlalchemy.orm import declarative_base

# Create the declarative base class
# All SQLAlchemy models will inherit from this Base
Base = declarative_base()

# Import all models to ensure SQLAlchemy can discover them
# This is critical for foreign key resolution
from modules.audit import models as _audit_models  # noqa: F401, E402
from modules.auth import models as _auth_models  # noqa: F401, E402
from modules.users import models as _users_models  # noqa: F401, E402
from modules.employee import models as _employee_models  # noqa: F401, E402
from modules.engagements import models as _engagements_models  # noqa: F401, E402
from modules.organizations import models as _organizations_models  # noqa: F401, E402
from modules.assessments import models as _assessments_models  # noqa: F401, E402
from modules.questionnaire import models as _questionnaire_models  # noqa: F401, E402
from modules.diagnostics import models as _diagnostics_models  # noqa: F401, E402
from modules.support import models as _support_models  # noqa: F401, E402
from modules.checklists import models as _checklists_models  # noqa: F401, E402
from modules.reports import models as _reports_models  # noqa: F401, E402
