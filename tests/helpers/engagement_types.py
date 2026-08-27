"""Resolve engagement_type string codes to integer FK ids in tests."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def engagement_type_id(session: AsyncSession, code: str = "bio_ai") -> int:
    result = await session.execute(
        text("SELECT id FROM engagement_types WHERE code = :code LIMIT 1"),
        {"code": code},
    )
    row = result.first()
    if row is None:
        raise RuntimeError(f"engagement_type '{code}' not found — run db.seed")
    return int(row[0])
