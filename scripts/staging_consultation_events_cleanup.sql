BEGIN;

UPDATE notification_events
SET event_code = 'consultations.booked', event_name = 'Consultation booked'
WHERE event_code = 'assistants.consultation_booked';

UPDATE notification_events
SET event_code = 'reminders.consultation_ready_to_book', event_name = 'Ready to book consultation'
WHERE event_code = 'consultations.ready_to_book';

UPDATE notification_events
SET event_code = 'reminders.consultation_reminder', event_name = 'Consultation reminder'
WHERE event_code = 'consultations.reminder';

UPDATE engagement_notifications en
SET event_code = fixed.new_code
FROM (VALUES
  ('assistants.consultation_booked', 'consultations.booked'),
  ('consultation_booking_alert', 'consultations.booked'),
  ('consultation.booked', 'consultations.booked'),
  ('consultations.ready_to_book', 'reminders.consultation_ready_to_book'),
  ('consultation_ready', 'reminders.consultation_ready_to_book'),
  ('consultation.ready', 'reminders.consultation_ready_to_book'),
  ('consultations.reminder', 'reminders.consultation_reminder'),
  ('consultation_remainder', 'reminders.consultation_reminder'),
  ('consultation.reminder', 'reminders.consultation_reminder')
) AS fixed(legacy, new_code)
WHERE en.event_code = fixed.legacy;

COMMIT;
