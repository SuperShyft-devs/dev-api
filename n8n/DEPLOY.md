# n8n Workflow Deployment

Updated workflow templates live in `dev-api/n8n/workflows/`. After changing any JSON file, import it into the live n8n instance at `https://n8n.supershyft.com`.

## Report workflows updated for callback error handling

These templates were patched so Metsights fetch failures, WhatsApp send failures, and invalid phone numbers always POST to `/notifications/callback`:

| File | Webhook path |
|------|--------------|
| `send-blood-report-email-v2.json` | `send-blood-report-email-v2` |
| `send-blood-report-whatsapp.json` | `send-blood-report-whatsapp-v2` |
| `send-bioai-report-email-v2.json` | `send-bioai-report-email-v2` |
| `send-bioai-whatsapp.json` | `send-bioai-whatsapp-v2` |
| `send-reports-email.json` | `send-reports-email-v2` |

## Deploy steps

1. Open n8n → **Workflows** → select the matching workflow (or create from import).
2. **⋮** menu → **Import from File** → choose the updated JSON from this folder.
3. Confirm webhook paths match the `webhook_path` values in the `notification_services` table.
4. **Activate** the workflow.
5. Send a test dispatch from admin or `POST /notifications/dispatch` and verify the notification reaches `sent` or `failed` (not stuck `pending`).

## After deploy

Run the stale-notification cleanup and retry blood reports:

```bash
python -m db.jobs.expire_stale_notifications --yes
python -m db.jobs.load_blood_reports --yes
```
