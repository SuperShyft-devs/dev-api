# Notification service migration report

Generated: 2026-11-10T07:54:20.703692+00:00

| service_key | channel | template_name | webhook_path | workflow_file | status | legacy_event_hint | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| whatapi-otp | whatsapp | otp | /send-otp-whatsapp-v3 | whatapi-otp.json | catalog_match | auth.otp_sent |  |
| email-otp | email |  | /send-otp-email-v3 | email-otp.json | catalog_match | auth.otp_sent |  |
| consultation-booking-alert-whatsapp | whatsapp |  | /consultation-booking-alert-whatsapp-v1 | consultation-booking-alert-whatsapp.json | catalog_match | users.onboarded | classified_as_orchestrator |
| booking-confirmation-whatsapp | whatsapp | camp17 | /booking-confirmation-whatsapp-v1 | booking-confirmation-whatsapp.json | catalog_match | bookings.confirmed |  |
| booking-confirmation-email | email |  | /booking-confirmation-email-v1 | booking-confirmation-email.json | catalog_match | bookings.confirmed |  |
| phlebo-assigned-whatsapp | whatsapp | camp17 | /phlebo-assigned-whatsapp-v1 | phlebo-assigned-whatsapp.json | catalog_match | bookings.phlebo_assigned |  |
| phlebo-assigned-email | email |  | /phlebo-assigned-email-v1 | phlebo-assigned-email.json | catalog_match | bookings.phlebo_assigned |  |
| phlebo-reassigned-whatsapp | whatsapp | camp17 | /phlebo-reassigned-whatsapp-v1 | phlebo-reassigned-whatsapp.json | catalog_match | bookings.phlebo_reassigned |  |
| phlebo-reassigned-email | email |  | /phlebo-reassigned-email-v1 | phlebo-reassigned-email.json | catalog_match | bookings.phlebo_reassigned |  |
| phlebo-enroute-whatsapp | whatsapp | camp17 | /phlebo-enroute-whatsapp-v1 | phlebo-enroute-whatsapp.json | catalog_match | bookings.phlebo_enroute |  |
| phlebo-enroute-email | email |  | /phlebo-enroute-email-v1 | phlebo-enroute-email.json | catalog_match | bookings.phlebo_enroute |  |
| phlebo-delay-whatsapp | whatsapp | camp17 | /phlebo-delay-whatsapp-v1 | phlebo-delay-whatsapp.json | catalog_match | bookings.phlebo_delayed |  |
| phlebo-delay-email | email |  | /phlebo-delay-email-v1 | phlebo-delay-email.json | catalog_match | bookings.phlebo_delayed |  |
| family-onboard-sender-whatsapp | whatsapp | camp13 | /family-onboard-sender-whatsapp-v1 | family-onboard-sender-whatsapp.json | catalog_match | family.gift_sent |  |
| family-onboard-sender-email | email |  | /family-onboard-sender-email-v1 | family-onboard-sender-email.json | catalog_match | family.gift_sent |  |
| family-onboard-receiver-whatsapp | whatsapp | camp15 | /family-onboard-receiver-whatsapp-v1 | family-onboard-receiver-whatsapp.json | catalog_match | family.gift_received |  |
| family-onboard-receiver-email | email |  | /family-onboard-receiver-email-v1 | family-onboard-receiver-email.json | catalog_match | family.gift_received |  |
| cpu_above_threshold | email |  | /cpu_above_threshold | cpu-overload-email.json | catalog_match | system.cpu_threshold |  |

## unmapped_workflows

- assessment-reminder-whatsapp-metsights-v2.json (/assessment-reminder-whatsapp-metsights-v3)
- b2b-booking-alert-whatsapp.json (/b2b-booking-alert-whatsapp-v1)
- book-doctor-consultation-email.json (/book-doctor-consultation-email-v1)
- book-doctor-consultation-whatsapp.json (/book-doctor-consultation-whatsapp-v1)
- book-nutrition-consultation-email-1+1.json (/book-nutrition-consultation-email-1+1-v1)
- book-nutrition-consultation-whatsapp-1+1.json (/book-nutrition-consultation-whatsapp-1+1-v1)
- book-nvidia-consultation-email.json (/book-nvidia-consultation-email-v1)
- book-nvidia-consultation-whatsapp.json (/book-nvidia-consultation-whatsapp-v1)
- booking-alert-email+whatsapp+participant-welcome-metsights.json (/booking-alert-email+whatsapp+participant-welcome-metsights-v2)
- booking-alert-whatsapp-1+1.json (/booking-alert-email+whatsapp+welcome-booking-1+1-v1)
- booking-alert-whatsapp.json (/booking-alert-email+whatsapp+participant-welcome-supershyft-v2)
- consultation-confirmed-email.json (/consultation-confirmed-email-v1)
- consultation-confirmed-whatsapp.json (/consultation-confirmed-whatsapp-v1)
- consultation-remainder-email.json (/consultation-remainder-email-v3)
- consultation-remainder-whatsapp.json (/consultation-remainder-whatsapp-v1)
- doctor-consultation-booking-notice-email.json (/doctor-consultation-booking-notice-email-v1)
- notify-admin-contact-query.json (/notify-admin-contact-query)
- nutrition-consultation-confirmed-email-1+1.json (/nutrition-consultation-confirmed-email-1+1-v1)
- nutrition-consultation-confirmed-whatsapp-1+1.json (/nutrition-consultation-confirmed-whatsapp-1+1-v1)
- nutrition-consultation-reminder-email-1+1.json (/nutrition-consultation-reminder-email-1+1-v1)
- nutrition-consultation-reminder-whatsapp-1+1.json (/nutrition-consultation-reminder-whatsapp-1+1-v1)
- nutrition-consultation-thank-you-email.json (/nutrition-consultation-thank-you-email-v1)
- nutrition-consultation-thank-you-whatsapp.json (/nutrition-consultation-thank-you-whatsapp-v1)
- nvidia-collection-booking-alert-whatsapp.json (/nvidia-collection-booking-alert-whatsapp-v1)
- nvidia-collection-confirmed-email.json (/nvidia-collection-confirmed-email-v1)
- nvidia-collection-confirmed-whatsapp.json (/nvidia-collection-confirmed-whatsapp-v1)
- nvidia-consultation-booking-alert-whatsapp.json (/nvidia-consultation-booking-alert-whatsapp-v1)
- nvidia-consultation-confirmed-email.json (/nvidia-consultation-confirmed-email-v1)
- nvidia-consultation-confirmed-whatsapp.json (/nvidia-consultation-confirmed-whatsapp-v1)
- nvidia-consultation-remainder-email.json (/nvidia-consultation-remainder-email-v1)
- nvidia-consultation-remainder-whatsapp.json (/nvidia-consultation-remainder-whatsapp-v1)
- nvidia-consultation-thank-you-email.json (/nvidia-consultation-thank-you-email-v1)
- nvidia-consultation-thank-you-whatsapp.json (/nvidia-consultation-thank-you-whatsapp-v1)
- pretest-email-1+1.json (/pretest-email-1+1-v1)
- pretest-email.json (/pretest-email-v3)
- pretest-whatsapp-1+1.json (/pretest-whatsapp-1+1-v1)
- pretest-whatsapp-session.json (/pretest-whatsapp-session-v1)
- pretest-whatsapp.json (/pretest-whatsapp-v3)
- send-bioai-report-email-v2.json (/send-bioai-report-email-v4)
- send-bioai-whatsapp.json (/send-bioai-whatsapp-v4)
- send-blood-report-email-1+1.json (/send-blood-report-email-1+1-v1)
- send-blood-report-email-v2.json (/send-blood-report-email-v4)
- send-blood-report-whatsapp-1+1.json (/send-blood-report-whatsapp-1+1-v1)
- send-blood-report-whatsapp.json (/send-blood-report-whatsapp-v4)
- send-expert-consultation-guide-whatsapp.json (/send-expert-consultation-guide-whatsapp-v1)
- send-phlebo-booking-guide-whatsapp.json (/send-phlebo-booking-guide-whatsapp-v1)
- send-reports-email.json (/send-reports-email-v4)
- thank-you-feedback-email-1+1.json (/thank-you-feedback-email-1+1-v1)
- thank-you-feedback-whatsapp-1+1.json (/thank-you-feedback-whatsapp-1+1-v1)
- welcome-booking-email-1+1.json (/welcome-booking-email-1+1-v1)
- welcome-booking-whatsapp-1+1.json (/welcome-booking-whatsapp-1+1-v1)
- welcome-email.json (/welcome-email-v3)
- welcome-whatsapp-metsights.json (/welcome-whatsapp-metsights-v3)
- welcome-whatsapp.json (/welcome-whatsapp-v3)
