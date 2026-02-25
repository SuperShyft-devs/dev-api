from __future__ import annotations

import os
import sys

from alembic import command
from alembic.config import Config
from dotenv import load_dotenv


def _get_database_url() -> str:
    """Return the database URL for migrations."""

    return os.getenv("DATABASE_URL") or ""


def main() -> int:
    """Run migrations to the latest version.

    This is safe for production.
    It does not create tables via ORM.
    """

    database_url = _get_database_url()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 2

    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", database_url)

    command.upgrade(cfg, "head")
    return 0


if __name__ == "__main__":
    load_dotenv(override=False)

    raise SystemExit(main())
