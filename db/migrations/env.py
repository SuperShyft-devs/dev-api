from __future__ import annotations

import os
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config


def _get_database_url() -> str:
    """Return the database URL for migrations."""

    return os.getenv("DATABASE_URL") or ""


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# We do not use autogenerate.
# Migrations are written by hand.
target_metadata = None


def run_migrations_offline() -> None:
    """Run migrations in offline mode."""

    url = _get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL is required for migrations")

    context.configure(
        url=url,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=False,
        compare_server_default=False,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in online mode."""

    url = _get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL is required for migrations")

    configuration: dict[str, Any] = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = url

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        def _configure(sync_connection):
            context.configure(
                connection=sync_connection,
                compare_type=False,
                compare_server_default=False,
            )

        await connection.run_sync(_configure)

        async with connection.begin():
            await connection.run_sync(lambda _: context.run_migrations())

    await connectable.dispose()


def run_migrations() -> None:
    """Entrypoint used by Alembic."""

    if context.is_offline_mode():
        run_migrations_offline()
        return

    import asyncio

    asyncio.run(run_migrations_online())


run_migrations()
