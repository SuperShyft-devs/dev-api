"""Entry point:  python -m db.jobs.retry_failed_notifications"""

from db.jobs.retry_failed_notifications.command import main

if __name__ == "__main__":
    raise SystemExit(main())
