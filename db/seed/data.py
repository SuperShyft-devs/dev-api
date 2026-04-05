from __future__ import annotations

from datetime import date

from db.seed.metsights_questionnaire_data import (
    METSIGHTS_CATEGORY_QUESTIONS,
    METSIGHTS_CATEGORIES,
    METSIGHTS_OPTIONS,
    METSIGHTS_PACKAGE_CATEGORIES,
    METSIGHTS_QUESTIONS,
)
from db.seed.seed_dataclasses import (
    SeedAssessmentPackage,
    SeedCategory,
    SeedCategoryQuestion,
    SeedEmployee,
    SeedOption,
    SeedPackageCategory,
    SeedQuestion,
    SeedUser,
)

__all__ = [
    "SeedUser",
    "SeedEmployee",
    "SeedAssessmentPackage",
    "SeedCategory",
    "SeedQuestion",
    "SeedCategoryQuestion",
    "SeedOption",
    "SeedPackageCategory",
    "DEFAULT_USERS",
    "DEFAULT_EMPLOYEES",
    "DEFAULT_ASSESSMENT_PACKAGES",
    "DEFAULT_CATEGORIES",
    "DEFAULT_QUESTIONS",
    "DEFAULT_CATEGORY_QUESTIONS",
    "DEFAULT_OPTIONS",
    "DEFAULT_PACKAGE_CATEGORIES",
]

DEFAULT_USERS: tuple[SeedUser, ...] = (
    SeedUser(
        user_id=1,
        first_name="Rishi",
        last_name="Nagar",
        age=31,
        phone="7770081606",
        email="rishi@supershyft.com",
        date_of_birth=date(1995, 4, 4),
        gender="male",
        address="Sher-E-Punjab, Pump House, Andheri East",
        pin_code="400059",
        city="Mumbai",
        state="Maharashtra",
        country="India",
        referred_by=None,
        is_participant=False,
        status="active",
    ),
    SeedUser(
        user_id=2,
        first_name="Harshili",
        last_name="Gada",
        age=30,
        phone="9769422110",
        email="harshili.fitnastic@gmail.com",
        date_of_birth=date(1996, 4, 4),
        gender="female",
        address="",
        pin_code="",
        city="",
        state="",
        country="India",
        referred_by=None,
        is_participant=False,
        status="active",
    ),
)

DEFAULT_EMPLOYEES: tuple[SeedEmployee, ...] = (
    SeedEmployee(employee_id=1, user_id=1, role="admin", status="active"),
    SeedEmployee(employee_id=2, user_id=2, role="admin", status="active"),
)

DEFAULT_ASSESSMENT_PACKAGES: tuple[SeedAssessmentPackage, ...] = (
    SeedAssessmentPackage(
        package_id=1,
        package_code="METSIGHTS_BASIC",
        display_name="Metsights Basic",
        assessment_type_code="1",
        status="active",
    ),
    SeedAssessmentPackage(
        package_id=2,
        package_code="METSIGHTS_PRO",
        display_name="Metsights Pro",
        assessment_type_code="2",
        status="active",
    ),
    SeedAssessmentPackage(
        package_id=3,
        package_code="MY_FITNESS_PRINT",
        display_name="FitPrint Full",
        assessment_type_code="7",
        status="active",
    ),
)

DEFAULT_CATEGORIES: tuple[SeedCategory, ...] = METSIGHTS_CATEGORIES
DEFAULT_QUESTIONS: tuple[SeedQuestion, ...] = METSIGHTS_QUESTIONS
DEFAULT_CATEGORY_QUESTIONS: tuple[SeedCategoryQuestion, ...] = METSIGHTS_CATEGORY_QUESTIONS
DEFAULT_OPTIONS: tuple[SeedOption, ...] = METSIGHTS_OPTIONS
DEFAULT_PACKAGE_CATEGORIES: tuple[SeedPackageCategory, ...] = METSIGHTS_PACKAGE_CATEGORIES
