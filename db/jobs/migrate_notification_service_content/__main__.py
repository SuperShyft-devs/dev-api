"""Entry point: python -m db.jobs.migrate_notification_service_content"""

from db.jobs.migrate_notification_service_content.command import main

if __name__ == "__main__":
    raise SystemExit(main())
