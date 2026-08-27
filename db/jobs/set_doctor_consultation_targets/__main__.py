from __future__ import annotations

from dotenv import load_dotenv

from db.jobs.set_doctor_consultation_targets.command import main


if __name__ == "__main__":
    load_dotenv(override=False)
    raise SystemExit(main())
