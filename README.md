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

## b.cycle MTEK Monthly Data Audit

Workflow file: `.github/workflows/bcycle-mtek-monthly-data-audit.yml`
Script: `scripts/bcycle-mtek-monthly-data-audit.mjs`
Shared config: `scripts/lib/bcycle-mtek-tables.mjs`, `scripts/lib/bcycle-mtek-sql-dates.mjs`

### What it does
Runs on the 2nd of each month (`41 11 2 * *` UTC), auditing the full previous
calendar month across all three b.cycle MTEK tables (`SalesMTEK`,
`FirstTimersMTEK`, `ReservationsMTEK`) for three things:

1. **Missing calendar dates** — every date in the month should have at least
   one row. Any gap is auto-backfilled from the same MTEK reports the daily/
   weekly sync scripts use, deduped against what's already in BigQuery. A
   date the MTEK report itself returns zero rows for is reported as
   "confirmed empty in MTEK," not an alarm — that's a normal, expected state
   for First Timers especially.
2. **Bad date-string formatting** — `SalesMTEK.order_date_utc`/
   `transaction_date` and `FirstTimersMTEK.string_field_4`/`string_field_5`
   are historically all-STRING date columns that were once found in three
   inconsistent formats (see gotcha below). Any row in the audited month not
   in strict `MM/DD/YYYY` gets normalized in place. `ReservationsMTEK` is
   skipped here — its date columns are real typed `DATE`, not strings.
3. **Duplicate rows** — exact full-row duplicates within the audited month
   (byte-identical except possibly the date-string variant that caused them)
   get removed via a scoped staging-table + transaction, never a whole-table
   rewrite. Rows that share a table's normal dedup key but differ in other
   columns are reported only, never auto-deleted — that's a real upstream
   problem that needs a human.

One consolidated Slack message covering all three tables is posted to
`#bigquery-updates` at the end of every real (non-dry-run) run, whether the
month was clean or something needed fixing.

In a clean month — the expected steady state — this job issues **only
`SELECT`s**: no snapshot, no staging table, no writes at all.

### Required secrets
Same as the sales sync: `MTEK_API_TOKEN`, `GCP_BQ_SERVICE_ACCOUNT_KEY`,
`SLACK_BIGQUERY_UPDATES_WEBHOOK_URL`. No new secrets.

### Manual runs / testing
`workflow_dispatch` accepts `dry_run` (default checked), `audit_month`
(`YYYY-MM`, blank = previous month — rejects a month that isn't fully in the
past, so it can't be pointed at an in-progress month), and `tables`
(comma-separated subset of `sales,firstTimers,reservations`, blank = all
three). Locally: `DRY_RUN=true AUDIT_MONTH=2026-07 node
scripts/bcycle-mtek-monthly-data-audit.mjs` with
`SLACK_BIGQUERY_UPDATES_WEBHOOK_URL` unset prints the exact Slack message
text to the console without posting anywhere.

### Known gotchas
- **The bug this job guards against, for the record.** We found historical
  rows in the two STRING date columns above in three formats: `MM/DD/YYYY`
  (good), `M/D/YYYY` (unpadded), and `MM/ D/YYYY` (a literal space instead of
  a zero for single-digit days). The space-padded variant silently breaks a
  naive `SAFE.PARSE_DATE('%m/%d/%Y', col)` — the rows don't error, they just
  vanish from any date-filtered query, which once looked exactly like a
  9-day "missing sales" gap that never actually happened. Every date
  comparison in this audit script goes through the tolerant regex-based
  parser in `lib/bcycle-mtek-sql-dates.mjs` instead — **never** add a raw
  `SAFE.PARSE_DATE('%m/%d/%Y', col)` against these two tables' date columns
  anywhere in this repo without going through that helper, or the exact same
  false alarm (or worse, a re-inserted duplicate on the write path) happens
  again.
- **No permanent backup table per run, by design.** Unlike a one-off manual
  fix, this job doesn't snapshot a table before mutating it every month —
  that's 12 orphaned `_backup_...` tables a year nobody prunes. Recovery
  instead relies on BigQuery's 7-day time-travel window, which the scoped
  DML approach here preserves (a whole-table `CREATE OR REPLACE` would reset
  it — deliberately avoided for exactly this reason). To restore a table to
  its state before a given run:
  ```sql
  CREATE OR REPLACE TABLE `root-cargo-453703-k7.SalesZF.<Table>_restore` AS
  SELECT * FROM `root-cargo-453703-k7.SalesZF.<Table>`
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  ```
  adjusting the interval to before the run in question, only usable within 7
  days of that run.
- **A missing monthly Slack message looks identical to "nothing to report."**
  Same caveat as the daily sales sync's GitHub Actions cron gotcha below — a
  monthly cron only gets 12 chances a year to be silently skipped. If a
  month goes by with no audit message in `#bigquery-updates`, check the
  Actions run history for this workflow before assuming everything was
  clean.

## SPINCO MTEK Monthly Data Audit

Workflow file: `.github/workflows/spinco-mtek-monthly-data-audit.yml`
Script: `scripts/spinco-mtek-monthly-data-audit.mjs`
Shared config: `scripts/lib/spinco-mtek-tables.mjs`, `scripts/lib/spinco-mtek-sql-dates.mjs`

### What it does
Runs on the 2nd of each month (`13 12 2 * *` UTC — a different minute/hour
than the b.cycle audit below, to dodge GH Actions cron contention and stay
clear of SPINCO's own weekly Monday 07:00 UTC syncs), auditing the full
previous calendar month across all three SPINCO MTEK tables (`Sales`,
`` `First Timers` ``, `reservations_raw_2025`, dataset `SPINCO`) for missing
calendar dates, bad date-string formatting, and duplicate rows — same shape
as the b.cycle audit above, but **the underlying data problem is different
in kind, not just company name**:

- **`Sales`** and **`First Timers`** already have real typed `DATE` columns —
  no string-format risk at all. Only the missing-dates and duplicate-row
  checks apply to them.
- **`reservations_raw_2025`** (~4.18M rows) is fully STRING-typed, including
  its four date columns (`class_start_date`, `creation_date`, `updated_date`,
  `cancelled_date`). Confirmed live against production: these are a genuine
  mix of **two different date systems** — ISO `YYYY-MM-DD` (the historical
  majority) and US `M/D/YYYY` (padded and unpadded) — correlating with *when*
  the data was imported, not the class date itself. The two most recent
  months are effectively all zero-padded `MM/DD/YYYY`, matching this table's
  own sync script (`toMDYYYY()`), so `MM/DD/YYYY` is the canonical target
  format. The sync script's `getInitialSinceDate` query already defensively
  parses both formats — this audit's tolerant parser
  (`lib/spinco-mtek-sql-dates.mjs`'s `tolerantDateAny`/`normalizeAnyToMdy`)
  mirrors that exact shape so the two never disagree.

**Important scoping note:** format-fixing, like every other check in this
job, is scoped to the single previous calendar month being audited. This is
**not** a one-time cleanup of `reservations_raw_2025`'s multi-million-row
historical backlog spanning 2024–2026 in mixed formats — that backlog is
deliberately out of scope for this recurring job. A single run only ever
touches the one month it's auditing; the ISO-format history further back
will simply never be visited by this job (by design, not an oversight).

One consolidated Slack message covering all three tables is posted to
`#bigquery-updates` (the same channel the b.cycle audit and both companies'
other BigQuery sync jobs already post to — the message title says "SPINCO"
explicitly to disambiguate) at the end of every real (non-dry-run) run.

In a clean month — the expected steady state — this job issues **only
`SELECT`s**: no snapshot, no staging table, no writes at all.

### Required secrets
`MTEK_SPINCO_API_TOKEN`, `GCP_BQ_SERVICE_ACCOUNT_KEY`,
`SLACK_BIGQUERY_UPDATES_WEBHOOK_URL` (all existing, shared with the other
SPINCO/b.cycle jobs). No new secrets.

### Manual runs / testing
Same `workflow_dispatch` inputs as the b.cycle audit: `dry_run` (default
checked), `audit_month` (`YYYY-MM`, blank = previous month — rejects a month
that isn't fully in the past), `tables` (comma-separated subset of
`sales,firstTimers,reservations`, blank = all three).

### Known gotchas
- **Don't add a naive `SAFE.PARSE_DATE('%m/%d/%Y', col)` or
  `SAFE.PARSE_DATE('%Y-%m-%d', col)` against `reservations_raw_2025`'s date
  columns anywhere in this repo without going through
  `lib/spinco-mtek-sql-dates.mjs`'s `tolerantDateAny`.** A single-format
  parse silently drops whichever format it doesn't recognize — on this
  table, that's roughly half the historical rows, not an edge case.
- **`Sales`' dedup key has ~132 pre-existing collisions in the full baseline
  data** (documented in `spinco-sales-to-bigquery.mjs`'s header comment) even
  under the full composite key. If the audit's report-only key-collision
  check ever surfaces some of these for a given month, that's expected
  pre-existing noise, not a new bug — don't chase it as if it were new.
- Same time-travel recovery approach and "missing monthly Slack message
  looks identical to nothing to report" caveat as the b.cycle audit above —
  see its gotchas section for the restore snippet and the reasoning.

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
