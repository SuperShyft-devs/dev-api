from __future__ import annotations

from dotenv import load_dotenv

from db.seed_sample.command import main


if __name__ == "__main__":
    load_dotenv(override=False)
    raise SystemExit(main())
