-- One-off staging cleanup: normalize legacy notification bindings to catalog codes.
-- Idempotent. Run: psql "$DB_URL" -v ON_ERROR_STOP=1 -f staging_notification_legacy_cleanup.sql
BEGIN;

-- Non-catalog event slugs (n8n / pre-catalog)
UPDATE notification_events SET event_code = 'family.gift_sent', event_name = 'Family gift sent (booker)' WHERE event_code = 'payment.gift.sent.booker';
UPDATE notification_events SET event_code = 'family.gift_received', event_name = 'Family gift received (member)' WHERE event_code = 'payment.gift.sent.member';
UPDATE notification_events SET event_code = 'bookings.confirmed', event_name = 'Booking confirmed' WHERE event_code = 'collection.confirmed';
UPDATE notification_events SET event_code = 'bookings.phlebo_assigned', event_name = 'Phlebo assigned' WHERE event_code = 'collection.phlebo.assigned';
UPDATE notification_events SET event_code = 'bookings.phlebo_reassigned', event_name = 'Phlebo reassigned' WHERE event_code = 'collection.phlebo.reassigned';
UPDATE notification_events SET event_code = 'bookings.phlebo_enroute', event_name = 'Phlebo en route' WHERE event_code = 'collection.phlebo.enroute';
UPDATE notification_events SET event_code = 'bookings.phlebo_delayed', event_name = 'Phlebo delayed' WHERE event_code = 'collection.phlebo.delayed';
UPDATE notification_events SET event_code = 'system.cpu_threshold', event_name = 'CPU threshold exceeded' WHERE event_code = 'system.cpu.alert';
UPDATE notification_events SET event_code = 'consultations.booked', event_name = 'Consultation booked' WHERE event_code IN ('consultation.booked', 'assistants.consultation_booked');
UPDATE notification_events SET event_code = 'reminders.consultation_reminder', event_name = 'Consultation reminder' WHERE event_code IN ('consultation.reminder', 'consultations.reminder');
UPDATE notification_events SET event_code = 'reminders.consultation_ready_to_book', event_name = 'Ready to book consultation' WHERE event_code IN ('consultation.ready', 'consultations.ready_to_book');
UPDATE notification_events SET event_code = 'reminders.pretest', event_name = 'Pretest reminder' WHERE event_code = 'collection.reminder.pretest';
UPDATE notification_events SET event_code = 'reminders.booking_guide', event_name = 'Booking guide' WHERE event_code = 'collection.reminder.booking_guide';
UPDATE notification_events SET event_code = 'reminders.questionnaire_before', event_name = 'Questionnaire reminder (before)' WHERE event_code = 'assessment.reminder.before';
UPDATE notification_events SET event_code = 'reminders.questionnaire_after', event_name = 'Questionnaire reminder (after)' WHERE event_code = 'assessment.reminder.after';
UPDATE notification_events SET event_code = 'reports.blood_ready', event_name = 'Blood report ready' WHERE event_code = 'report.blood.ready';
UPDATE notification_events SET event_code = 'reports.bioai_ready', event_name = 'BioAI report ready' WHERE event_code = 'report.bioai.ready';
UPDATE notification_events SET event_code = 'reports.face_scan_ready', event_name = 'Face scan report ready' WHERE event_code = 'report.vifc.ready';

UPDATE notification_events
SET
  event_code = 'users.onboarded',
  event_name = 'User onboarded to engagement'
WHERE event_code IN ('onboarding.completed', 'onboarding');

UPDATE notification_events
SET
  event_code = 'support.ticket_created',
  event_name = 'Support ticket created'
WHERE event_code = 'support.ticket.created';

UPDATE notification_events SET recipient_type = 'members' WHERE recipient_type = 'family_member';

UPDATE notification_events SET recipient_type = 'assistants' WHERE recipient_type = 'oa.all';
UPDATE notification_events SET recipient_type = 'assistants.phlebo' WHERE recipient_type = 'oa.phlebo';
UPDATE notification_events SET recipient_type = 'assistants.expert' WHERE recipient_type = 'oa.expert';
UPDATE notification_events SET recipient_type = 'assistants.staff' WHERE recipient_type = 'oa.staff';
UPDATE notification_events SET recipient_type = 'user' WHERE recipient_type IN ('participant', 'booker');
UPDATE notification_events SET recipient_type = 'departments' WHERE recipient_type IN ('employees', 'employee');
UPDATE notification_events SET recipient_type = 'departments.ops' WHERE recipient_type IN ('admin', 'support');
UPDATE notification_events SET recipient_type = 'custom' WHERE recipient_type IN ('specific_contacts', 'specific_numbers', 'specific');

-- Default onboarding assistant alerts use all assistants, not staff-only
UPDATE notification_events
SET recipient_type = 'assistants'
WHERE event_code = 'users.onboarded'
  AND recipient_type = 'assistants.staff'
  AND notification_services::text LIKE '%booking-alert%';

UPDATE engagement_notifications en
SET event_code = fixed.new_code
FROM (VALUES
  ('onboarding.completed', 'users.onboarded'),
  ('onboarding', 'users.onboarded'),
  ('payment.gift.sent.booker', 'family.gift_sent'),
  ('payment.gift.sent.member', 'family.gift_received'),
  ('collection.confirmed', 'bookings.confirmed'),
  ('collection.phlebo.assigned', 'bookings.phlebo_assigned'),
  ('collection.phlebo.reassigned', 'bookings.phlebo_reassigned'),
  ('collection.phlebo.enroute', 'bookings.phlebo_enroute'),
  ('collection.phlebo.delayed', 'bookings.phlebo_delayed'),
  ('system.cpu.alert', 'system.cpu_threshold'),
  ('consultation.booked', 'consultations.booked'),
  ('consultation.reminder', 'reminders.consultation_reminder'),
  ('consultation.ready', 'reminders.consultation_ready_to_book'),
  ('collection.reminder.pretest', 'reminders.pretest'),
  ('collection.reminder.booking_guide', 'reminders.booking_guide'),
  ('assessment.reminder.before', 'reminders.questionnaire_before'),
  ('assessment.reminder.after', 'reminders.questionnaire_after'),
  ('report.blood.ready', 'reports.blood_ready'),
  ('report.bioai.ready', 'reports.bioai_ready'),
  ('report.vifc.ready', 'reports.face_scan_ready'),
  ('pretest_guidelines', 'reminders.pretest'),
  ('questionnaire_reminder_before', 'reminders.questionnaire_before'),
  ('questionnaire_reminder_after', 'reminders.questionnaire_after'),
  ('blood_report_ready', 'reports.blood_ready'),
  ('bioai_report_ready', 'reports.bioai_ready'),
  ('report_ready', 'reports.face_scan_ready'),
  ('consultation_ready', 'reminders.consultation_ready_to_book'),
  ('consultation_remainder', 'reminders.consultation_reminder'),
  ('consultation_booking_alert', 'consultations.booked'),
  ('assistants.consultation_booked', 'consultations.booked'),
  ('consultations.ready_to_book', 'reminders.consultation_ready_to_book'),
  ('consultations.reminder', 'reminders.consultation_reminder'),
  ('booking_guide', 'reminders.booking_guide')
) AS fixed(legacy, new_code)
WHERE en.event_code = fixed.legacy;

COMMIT;
