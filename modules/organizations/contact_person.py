"""Parse and resolve organization contact_person_user_ids JSON."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from core.exceptions import AppError

ORG_MANAGERS_KEY = "organization_managers"
CITY_MANAGERS_KEY = "managers"


@dataclass
class OrgManagerScope:
    is_org_manager: bool = False
    city_manager_cities: set[str] = field(default_factory=set)
    dept_slugs_by_city: dict[str, set[str]] = field(default_factory=dict)

    def has_any_access(self) -> bool:
        return self.is_org_manager or bool(self.city_manager_cities) or bool(self.dept_slugs_by_city)

    def allowed_cities_for_engagements(self) -> list[str] | None:
        """None means no city filter (org-level manager)."""
        if self.is_org_manager:
            return None
        cities = set(self.city_manager_cities) | set(self.dept_slugs_by_city.keys())
        return sorted(cities)

    def _city_matches(self, stored_city: str, engagement_city: str | None) -> bool:
        return _city_keys_match(stored_city, engagement_city)

    def can_access_engagement_city(self, city: str | None) -> bool:
        if self.is_org_manager:
            return True
        normalized = normalize_city_key(city)
        if not normalized:
            return False
        for stored in self.city_manager_cities:
            if self._city_matches(stored, normalized):
                return True
        for stored in self.dept_slugs_by_city:
            if self._city_matches(stored, normalized):
                return True
        return False

    def participant_department_slugs_for_city(self, city: str | None) -> set[str] | None:
        """None = no department filter within the engagement."""
        if self.is_org_manager:
            return None
        normalized = normalize_city_key(city)
        if not normalized:
            return set()
        for stored in self.city_manager_cities:
            if self._city_matches(stored, normalized):
                return None
        for stored, slugs in self.dept_slugs_by_city.items():
            if self._city_matches(stored, normalized):
                return set(slugs)
        return set()

    def can_access_camp_report(self, city: str | None, department: str | None) -> bool:
        if self.is_org_manager:
            return True
        normalized_city = normalize_city_key(city)
        normalized_dept = (department or "").strip() or None

        if not normalized_city:
            return False

        for stored in self.city_manager_cities:
            if self._city_matches(stored, normalized_city):
                return True

        if normalized_dept:
            for stored, slugs in self.dept_slugs_by_city.items():
                if self._city_matches(stored, normalized_city) and normalized_dept in slugs:
                    return True

        return False


def normalize_city_key(city: str | None) -> str | None:
    value = (city or "").strip()
    return value if value else None


def _normalize_user_ids(raw_ids: Any) -> list[int]:
    if raw_ids is None:
        return []
    if not isinstance(raw_ids, list):
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    result: list[int] = []
    seen: set[int] = set()
    for item in raw_ids:
        if not isinstance(item, int) or item <= 0:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def parse_contact_person_user_ids(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    normalized: dict[str, Any] = {
        ORG_MANAGERS_KEY: _normalize_user_ids(raw.get(ORG_MANAGERS_KEY, [])),
    }

    for key, value in raw.items():
        if key == ORG_MANAGERS_KEY:
            continue
        if not isinstance(key, str) or not key.strip():
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        city_key = key.strip()
        if not isinstance(value, dict):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        city_payload: dict[str, list[int]] = {
            CITY_MANAGERS_KEY: _normalize_user_ids(value.get(CITY_MANAGERS_KEY, [])),
        }
        for dept_key, dept_ids in value.items():
            if dept_key == CITY_MANAGERS_KEY:
                continue
            if not isinstance(dept_key, str) or not dept_key.strip():
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            city_payload[dept_key.strip()] = _normalize_user_ids(dept_ids)

        normalized[city_key] = city_payload

    if not normalized[ORG_MANAGERS_KEY] and len(normalized) == 1:
        has_city_assignments = any(
            key != ORG_MANAGERS_KEY
            and (
                normalized[key].get(CITY_MANAGERS_KEY)
                or any(k != CITY_MANAGERS_KEY for k in normalized[key])
            )
            for key in normalized
        )
        if not has_city_assignments:
            return None

    return normalized


def iter_contact_person_user_ids(raw: Any) -> set[int]:
    parsed = parse_contact_person_user_ids(raw)
    if parsed is None:
        return set()

    user_ids: set[int] = set()
    org_ids = parsed.get(ORG_MANAGERS_KEY, [])
    if isinstance(org_ids, list):
        user_ids.update(int(uid) for uid in org_ids if isinstance(uid, int))

    for key, value in parsed.items():
        if key == ORG_MANAGERS_KEY or not isinstance(value, dict):
            continue
        for nested_key, nested_value in value.items():
            if isinstance(nested_value, list):
                user_ids.update(int(uid) for uid in nested_value if isinstance(uid, int))

    return user_ids


def _city_keys_match(stored_key: str, engagement_city: str | None) -> bool:
    city = normalize_city_key(engagement_city)
    if city is None:
        return False
    if stored_key == city:
        return True
    return stored_key.casefold() == city.casefold()


def resolve_org_manager_scope(raw: Any, user_id: int) -> OrgManagerScope | None:
    parsed = parse_contact_person_user_ids(raw)
    if parsed is None:
        return None

    scope = OrgManagerScope()
    org_managers = parsed.get(ORG_MANAGERS_KEY, [])
    if isinstance(org_managers, list) and user_id in org_managers:
        scope.is_org_manager = True

    for city_key, city_val in parsed.items():
        if city_key == ORG_MANAGERS_KEY or not isinstance(city_val, dict):
            continue
        managers = city_val.get(CITY_MANAGERS_KEY, [])
        if isinstance(managers, list) and user_id in managers:
            scope.city_manager_cities.add(city_key)
        dept_slugs: set[str] = set()
        for dept_key, dept_ids in city_val.items():
            if dept_key == CITY_MANAGERS_KEY:
                continue
            if isinstance(dept_ids, list) and user_id in dept_ids:
                dept_slugs.add(dept_key)
        if dept_slugs:
            scope.dept_slugs_by_city[city_key] = dept_slugs

    if not scope.has_any_access():
        return None
    return scope


def user_has_any_org_contact_role(raw: Any, user_id: int) -> bool:
    return resolve_org_manager_scope(raw, user_id) is not None


def remove_user_from_contact_person_user_ids(raw: Any, user_id: int) -> dict[str, Any] | None:
    parsed = parse_contact_person_user_ids(raw)
    if parsed is None:
        return None

    updated: dict[str, Any] = {
        ORG_MANAGERS_KEY: [uid for uid in parsed.get(ORG_MANAGERS_KEY, []) if uid != user_id],
    }

    for city_key, city_val in parsed.items():
        if city_key == ORG_MANAGERS_KEY or not isinstance(city_val, dict):
            continue
        city_payload: dict[str, list[int]] = {
            CITY_MANAGERS_KEY: [
                uid for uid in city_val.get(CITY_MANAGERS_KEY, []) if uid != user_id
            ],
        }
        for dept_key, dept_ids in city_val.items():
            if dept_key == CITY_MANAGERS_KEY:
                continue
            filtered = [uid for uid in dept_ids if uid != user_id]
            if filtered:
                city_payload[dept_key] = filtered
        if city_payload[CITY_MANAGERS_KEY] or any(k != CITY_MANAGERS_KEY for k in city_payload):
            updated[city_key] = city_payload

    if not updated[ORG_MANAGERS_KEY] and len(updated) == 1:
        return None
    return updated


def validate_contact_person_department_slugs(
    parsed: dict[str, Any] | None,
    allowed_slugs: set[str],
) -> None:
    if parsed is None:
        return
    for city_key, city_val in parsed.items():
        if city_key == ORG_MANAGERS_KEY or not isinstance(city_val, dict):
            continue
        for dept_key in city_val:
            if dept_key == CITY_MANAGERS_KEY:
                continue
            if dept_key not in allowed_slugs:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")


def build_camp_report_access(
    raw: Any,
    user_id: int,
    *,
    camp_cities: list[str],
    reported_dept_slugs: list[str],
    is_admin: bool = False,
) -> dict[str, Any]:
    if is_admin:
        access: dict[str, Any] = {"organization_manager": True}
        for city in camp_cities:
            city_access: dict[str, bool] = {"manager": True}
            for slug in reported_dept_slugs:
                city_access[slug] = True
            access[city] = city_access
        return access

    scope = resolve_org_manager_scope(raw, user_id)
    access = {"organization_manager": bool(scope and scope.is_org_manager)}

    for city in camp_cities:
        city_access: dict[str, bool] = {"manager": False}
        if scope is None:
            access[city] = city_access
            continue

        if scope.is_org_manager:
            city_access["manager"] = True
            for slug in reported_dept_slugs:
                city_access[slug] = True
        else:
            city_access["manager"] = any(
                _city_keys_match(stored, city) for stored in scope.city_manager_cities
            )
            dept_slugs = set()
            for stored_city, slugs in scope.dept_slugs_by_city.items():
                if _city_keys_match(stored_city, city):
                    dept_slugs.update(slugs)
            for slug in reported_dept_slugs:
                city_access[slug] = slug in dept_slugs

        access[city] = city_access

    return access
