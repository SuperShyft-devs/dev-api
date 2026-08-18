"""Unit tests for organization contact_person_user_ids helpers."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from modules.organizations.contact_person import (
    build_camp_report_access,
    iter_contact_person_user_ids,
    parse_contact_person_user_ids,
    remove_user_from_contact_person_user_ids,
    resolve_org_manager_scope,
)
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository


def test_parse_and_resolve_org_manager_scope():
    raw = {
        "organization_managers": [101],
        "Mumbai": {
            "managers": [201],
            "sales": [301],
        },
    }
    parsed = parse_contact_person_user_ids(raw)
    assert parsed is not None
    assert iter_contact_person_user_ids(parsed) == {101, 201, 301}

    org_scope = resolve_org_manager_scope(parsed, 101)
    assert org_scope is not None
    assert org_scope.is_org_manager is True
    assert org_scope.can_access_camp_report("Mumbai", "sales")

    city_scope = resolve_org_manager_scope(parsed, 201)
    assert city_scope is not None
    assert city_scope.can_access_camp_report("Mumbai", None)
    assert city_scope.can_access_camp_report("Mumbai", "sales")

    dept_scope = resolve_org_manager_scope(parsed, 301)
    assert dept_scope is not None
    assert dept_scope.can_access_camp_report("Mumbai", "sales") is True
    assert dept_scope.can_access_camp_report("Pune", "sales") is False


def test_remove_user_from_contact_person_user_ids():
    raw = {
        "organization_managers": [101, 102],
        "Mumbai": {"managers": [201], "sales": [301, 101]},
    }
    updated = remove_user_from_contact_person_user_ids(raw, 101)
    assert updated == {
        "organization_managers": [102],
        "Mumbai": {"managers": [201], "sales": [301]},
    }


def test_build_camp_report_access_for_org_manager():
    raw = {"organization_managers": [101], "Mumbai": {"managers": [201], "sales": [301]}}
    access = build_camp_report_access(
        raw,
        101,
        camp_cities=["Mumbai"],
        reported_dept_slugs=["sales"],
    )
    assert access["organization_manager"] is True
    assert access["Mumbai"]["manager"] is True
    assert access["Mumbai"]["sales"] is True


def test_contact_person_membership_sql_binds_uid_json():
    clause = OrganizationsRepository._contact_person_user_ids_contains_user(7410)
    compiled = select(Organization).where(clause).compile(dialect=postgresql.dialect())
    assert "uid_json" in compiled.params
    assert compiled.params["uid_json"] == "[7410]"
    assert ":jsonb" not in str(compiled)
