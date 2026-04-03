"""Unit tests for excel_db_export helpers."""

from __future__ import annotations

from common.excel_db_export import _cell_value


def test_cell_value_serializes_dict_for_excel():
    payload = {"id": "x", "n": 1.5, "flag": True}
    out = _cell_value(payload)
    assert isinstance(out, str)
    assert '"id"' in out
    assert "x" in out


def test_cell_value_serializes_list():
    assert _cell_value([1, "a"]) == '[1, "a"]'


def test_cell_value_primitives_unchanged():
    assert _cell_value(True) is True
    assert _cell_value(42) == 42
    assert _cell_value(3.5) == 3.5
