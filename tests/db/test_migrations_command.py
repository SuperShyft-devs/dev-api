from __future__ import annotations

import os

from db.migrations.__main__ import main


def test_migrations_command_requires_database_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    exit_code = main()

    assert exit_code == 2


def test_migrations_command_uses_database_url(monkeypatch):
    # We do not run the migrations here.
    # We only verify the command accepts a URL.
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/db")

    # Patch Alembic so we do not touch a real database.
    import alembic.command

    calls = {}

    def fake_upgrade(cfg, target):
        calls["url"] = cfg.get_main_option("sqlalchemy.url")
        calls["target"] = target

    monkeypatch.setattr(alembic.command, "upgrade", fake_upgrade)

    exit_code = main()

    assert exit_code == 0
    assert calls["url"] == os.environ["DATABASE_URL"]
    assert calls["target"] == "head"
