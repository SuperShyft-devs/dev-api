"""Entry point: python -m db.jobs.normalize_questionnaire_choice_answers"""

from __future__ import annotations

from dotenv import load_dotenv

from db.jobs.normalize_questionnaire_choice_answers.command import main


if __name__ == "__main__":
    load_dotenv(override=False)
    raise SystemExit(main())
