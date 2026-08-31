"""Transaction lifecycle helpers for releasing sessions before external I/O."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession


async def release_request_transaction(db: AsyncSession) -> None:
    """End the caller transaction before outbound HTTP or long CPU work.

    Always commits when a transaction is open. Read-only commits are a no-op for
    data and still release the Postgres ``idle in transaction`` hold.

    Important: Core DML via ``session.execute(update(...))`` (e.g.
    ``set_metsights_record_id``) does **not** populate ``session.dirty`` /
    ``new`` / ``deleted``. A dirty-only check previously took the rollback
    branch and silently undid flushed UPDATEs — e.g. Connect reported
    ``connected: 13`` while only the last row survived the final router commit
    (Synced +1).
    """
    if not db.in_transaction():
        return
    await db.commit()
