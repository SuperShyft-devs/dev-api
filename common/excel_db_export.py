"""Export all database tables to a single .xlsx workbook (one sheet per table)."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timezone
from decimal import Decimal
from io import BytesIO
from uuid import UUID

from openpyxl import Workbook
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

# Excel/OpenXML disallows ASCII control characters in shared strings.
_STR_SANITIZE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _list_tables(connection: Connection) -> list[str]:
    insp = inspect(connection)
    dialect = connection.dialect.name
    if dialect == "sqlite":
        names = insp.get_table_names()
    else:
        names = insp.get_table_names(schema="public")
    return sorted(names)


def _sanitize_sheet_title(raw: str, used: set[str]) -> str:
    """Excel sheet name: max 31 chars, no []:*?/\\"""
    s = re.sub(r'[\[\]:*?/\\]', "_", raw)
    s = s.strip() or "sheet"
    base = s[:31]
    title = base
    n = 2
    while title in used:
        suffix = f"_{n}"
        title = (base[: 31 - len(suffix)] + suffix) if len(base) + len(suffix) > 31 else base + suffix
        n += 1
    used.add(title)
    return title


def _cell_value(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, str):
        return _STR_SANITIZE.sub("", value)
    return value


def export_public_schema_to_xlsx_bytes(connection: Connection) -> bytes:
    """Read every user table and return an in-memory .xlsx file."""

    wb = Workbook()
    default = wb.active
    wb.remove(default)

    used_titles: set[str] = set()
    tables = _list_tables(connection)

    for table_name in tables:
        quoted = connection.dialect.identifier_preparer.quote(table_name)
        result = connection.execute(text(f"SELECT * FROM {quoted}"))
        keys = list(result.keys())
        ws = wb.create_sheet(title=_sanitize_sheet_title(table_name, used_titles))
        ws.append(keys)
        for row in result:
            ws.append([_cell_value(v) for v in row])

    buffer = BytesIO()
    wb.save(buffer)
    return buffer.getvalue()
