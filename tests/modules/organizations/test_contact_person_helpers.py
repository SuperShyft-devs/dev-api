"""Unit tests for organization contact_person_user_ids helpers."""

from __future__ import annotations

from modules.organizations.contact_person import (
    build_camp_report_access,
    iter_contact_person_user_ids,
    parse_contact_person_user_ids,
    remove_user_from_contact_person_user_ids,
    resolve_org_manager_scope,
)


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
