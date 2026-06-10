"""Entry point:  python -m db.jobs.expire_stale_notifications"""

from db.jobs.expire_stale_notifications.command import main

if __name__ == "__main__":
    raise SystemExit(main())
