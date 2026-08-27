"""Transaction lifecycle helpers for releasing sessions before external I/O."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession


async def release_request_transaction(db: AsyncSession) -> None:
    """End the caller transaction before outbound HTTP or long CPU work.

    Commits when the session has pending writes; otherwise rolls back read-only work.
    Safe to call when no transaction is open.
    """
    if not db.in_transaction():
        return
    if db.dirty or db.new or db.deleted:
        await db.commit()
    else:
        await db.rollback()
