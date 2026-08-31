# Healing missing Metsights record ids

After deploying the transaction-release fix to live (`/var/www/backend/api`):

1. Open the affected engagement in admin (e.g. NVIDIA Bangalore Men 2026).
2. Open **Assessments**.
3. On the package row (e.g. Metsights Pro), click the **connect/sync** control.
4. Confirm the toast: `connected` / `skipped` / `failed`.
5. **Synced** should rise (e.g. 988/1065 → closer to 1065 for users with a Metsights profile).
6. Repeat for other camp engagements (1024, 1027, etc.) if needed.
7. Spot-check a known failure: console **Submit category** for vitals should succeed once the record id is linked.

**Skipped `no_metsights_profile_id`:** user has no Metsights profile yet — create/link profile first, then run connect again.

**Do not** run bulk connect before deploying the fix; the old code could hold one DB transaction open across many Metsights HTTP calls and timeout under load.
