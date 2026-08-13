"""Unit tests for discount engine math and helpers."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from modules.discounts.engine import DiscountEngine, round_to_nearest_rupee_paise
from modules.discounts.abuse import sanitize_code, map_engine_outcome, CODE_PATTERN


def test_round_to_nearest_rupee_paise():
    assert round_to_nearest_rupee_paise(0) == 0
    assert round_to_nearest_rupee_paise(149) == 100
    assert round_to_nearest_rupee_paise(150) == 200
    assert round_to_nearest_rupee_paise(199) == 200


def test_sanitize_code():
    assert sanitize_code("  health20 ") == "HEALTH20"
    assert sanitize_code("ab") is None
    assert sanitize_code("BAD CODE!") is None
    assert CODE_PATTERN.fullmatch("NVIDIA20")


def test_map_engine_outcome():
    assert map_engine_outcome(True, None) == "ok"
    assert map_engine_outcome(False, "not_found") == "invalid"
    assert map_engine_outcome(False, "scope_camp") == "ineligible"


def test_compute_percentage_capped():
    engine = DiscountEngine()
    code = SimpleNamespace(
        discount_type="percentage_capped",
        percent_value=Decimal("20"),
        fixed_amount_paise=None,
        max_discount_paise=80000,  # Rs 800
        hard_ceiling_paise=None,
        min_price_protection=True,
    )
    lines = [
        SimpleNamespace(amount_paise=1000000, min_price_paise=0),  # Rs 10,000 → 20% = 2000 capped to 800
    ]
    total, line_offs = engine._compute_discount(code, lines, 1000000)
    assert total == 80000
    assert line_offs == [80000]


def test_min_price_clamp():
    engine = DiscountEngine()
    code = SimpleNamespace(
        discount_type="fixed",
        percent_value=None,
        fixed_amount_paise=50000,
        max_discount_paise=None,
        hard_ceiling_paise=None,
        min_price_protection=True,
    )
    lines = [
        SimpleNamespace(amount_paise=60000, min_price_paise=40000),
    ]
    total, line_offs = engine._compute_discount(code, lines, 60000)
    # max off = 60000-40000 = 20000
    assert total == 20000
    assert line_offs == [20000]


def test_filter_packages_include_exclude():
    engine = DiscountEngine()
    include_code = SimpleNamespace(
        include_addons=True,
        package_apply_mode="include",
        packages=[SimpleNamespace(diagnostic_package_id=1, mode="include")],
    )
    exclude_code = SimpleNamespace(
        include_addons=True,
        package_apply_mode="exclude",
        packages=[SimpleNamespace(diagnostic_package_id=2, mode="exclude")],
    )
    lines = [
        SimpleNamespace(index=0, entity_id=1, is_addon=False),
        SimpleNamespace(index=1, entity_id=2, is_addon=False),
        SimpleNamespace(index=2, entity_id=3, is_addon=True),
    ]
    assert [l.entity_id for l in engine._filter_packages(include_code, lines)] == [1]
    assert [l.entity_id for l in engine._filter_packages(exclude_code, lines)] == [1, 3]

    no_addon = SimpleNamespace(
        include_addons=False,
        package_apply_mode="all",
        packages=[],
    )
    assert [l.entity_id for l in engine._filter_packages(no_addon, lines)] == [1, 2]


def test_check_scope():
    engine = DiscountEngine()
    code = SimpleNamespace(
        scope_mode="camp",
        scopes=[SimpleNamespace(scope_type="camp", scope_key="101")],
    )
    assert engine._check_scope(code, SimpleNamespace(organization_id=None, camp_no="101", engagement_id=None)) is None
    assert engine._check_scope(code, SimpleNamespace(organization_id=None, camp_no="999", engagement_id=None)) == "scope_camp"

    general = SimpleNamespace(scope_mode="general", scopes=[])
    assert engine._check_scope(general, SimpleNamespace(organization_id=1, camp_no=None, engagement_id=None)) is None
