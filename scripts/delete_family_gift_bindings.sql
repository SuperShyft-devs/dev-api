-- Remove global family gift notification bindings (staging / one-off).
BEGIN;
DELETE FROM notification_events
WHERE event_code IN ('family.gift_sent', 'family.gift_received');
DELETE FROM engagement_notifications
WHERE event_code IN ('family.gift_sent', 'family.gift_received');
COMMIT;
