# bcycle-automations

## MTEK Sales → BigQuery sync + Meta Offline Conversions

Workflow file: `.github/workflows/mtek-sales-to-bigquery.yml`
Script: `scripts/mtek-sales-to-bigquery.mjs`

### What it does
Runs daily (`17 7 * * *` UTC). Pulls new completed sales from MTEK's "Orders -
UTC" report, inserts them into `SalesZF.SalesMTEK`, then — in the same run —
sends any new rows with `product_type` in `Credits`, `Memberships`, or any of
the four gift card types (`Line Status = Completed`) to Meta as `Purchase`
events, so ad attribution can account for app purchases Meta's pixel can't
otherwise see.

### Meta side
- Dataset: **"b.cycle Offline Conversions"**, ID `1979154219698960` (owned by
  Cardigan/Oli's agency, shared to the b.cycle ad account — NOT owned by
  b.cycle's own business portfolio).
- Sent via Graph API `POST /{dataset_id}/events`, `action_source:
  "physical_store"`, `value` = `Line Subtotal` (**exclusive of tax**, per Oli,
  2026-08-06).
- Customer email/phone/first/last name come from a **live MTEK
  `GET /api/users/{id}` lookup**, not BigQuery — an earlier attempt joined a
  BigQuery `Customers` table, but that table's `id` column turned out to be a
  completely unrelated ID space (19-digit values vs. MTEK's 5-6 digit
  customer IDs) — zero rows matched. MTEK's `/api/users/{id}` already
  includes the country code on `phone_number`.
- All PII (email, phone, first/last name) is normalized (lowercase/trim) and
  SHA-256 hashed before sending, per Meta's Conversions API spec.

### Required secrets
- `MTEK_API_TOKEN`, `GCP_BQ_SERVICE_ACCOUNT_KEY`, `SLACK_BIGQUERY_UPDATES_WEBHOOK_URL` (existing)
- `META_OFFLINE_CONVERSIONS_TOKEN` — System User token for the `bcycle-automation`
  Meta app (App ID `1029082523374014`), permission `ads_management`, generated
  against a system user with Full Control access assigned to the b.cycle ad
  account (`916126211757889`). Token does not expire.

### Known gotchas (found the hard way)
- **You cannot view submitted event data back through Meta, ever.** Once
  hashed data is sent, there is no UI/API to read individual records back —
  this is a permanent platform limitation, not a lag. The only way to
  visually confirm a payload landed correctly is to resend it with a fresh
  `test_event_code` (Events Manager → dataset → Test events tab) — test
  events get a live field-level viewer that production events never get.
- **Meta's own stats lag badly and unreliably.** `event_stats` /
  `match_rate_approx` (via Graph API) and the Overview tab's event count can
  sit empty or stale for over a day after events are genuinely accepted and
  processed — don't treat an empty/stale count as proof of failure.
- **A "received" API response does not guarantee the event was actually
  usable.** Early on, POSTs returned `events_received: N` with zero errors,
  but nothing ever showed up — root cause was the system user only having
  ad-account-level access to the dataset (shared cross-business by Cardigan),
  not dataset-level "manage" permission. Meta's own "Set up Conversions API"
  wizard surfaced the real error explicitly
  ("you don't have permission to manage it"); the raw API call didn't.
  Fixed by having b.cycle create and own its own dataset instead of relying
  on a cross-business-shared one — solves the permission problem entirely
  since same-business system users get full access automatically.
- **GitHub Actions scheduled crons are unreliable, especially at popular
  times.** The daily run silently skipped Aug 8 and 9, 2026 entirely (no run
  at all — not delayed, just never fired) while a different workflow in this
  same repo scheduled hourly fired 4,967+ times without issue. Root cause is
  GitHub-side contention at popular cron times (originally `0 7 * * *` — the
  top of the hour is heavily oversubscribed across GitHub); moved to
  `17 7 * * *` to reduce collisions. The script's own catch-up window (walks
  forward from the last synced date, up to 7 days per run) means a skipped
  day doesn't lose data — it just gets swept up whenever the cron next fires
  — but don't assume "no Slack message today" means nothing happened; check
  Actions run history if that ever seems off.

## b.cycle PAYROLL Classes GitHub Action

Workflow file: `.github/workflows/bcycle-payroll-classes-action.yml`

### Triggers
- `workflow_dispatch` with required input `record_id`
- `repository_dispatch` with event type `airtable-bcycle-payroll-classes` and payload field `record_id`

### Required secrets
- `AIRTABLE_TOKEN`
- `MTEK_API_TOKEN`

### Config values (editable directly in workflow/script)
- Airtable base/table IDs
- MarianaTek base URL and API paths

### Script
- `scripts/bcycle-payroll-classes-workflow.mjs`
