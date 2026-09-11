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

## HR Payroll Time Punches

Workflow file: `.github/workflows/hr-payroll-time-punches.yml`
Script: `scripts/hr-payroll-time-punches-workflow.mjs`

Staff (not instructor) payroll. Same shape as b.cycle PAYROLL Classes above,
but it runs against the **HR** base (`appiwfeujJzUZPPBx`) — *not* HR -
Instructors — and pulls time clock punches instead of classes.

### Triggers
- `workflow_dispatch` with required input `record_id`
- `repository_dispatch` with event type `airtable-hr-payroll-time-punches` and
  payload field `record_id`

`record_id` is a **Budget week - Studio** record (`tblbyFY6TlRi4BxOe`), which
supplies both the date window (via its linked Budget week) and the studio.

### Required secrets
- `AIRTABLE_TOKEN`
- `MTEK_API_TOKEN`

### Airtable tables (HR base)
- `Budget week` (`tblt2pfs356rDVDIa`) — Start Date / End Date, one row per week
- `Budget week - Studio` (`tblbyFY6TlRi4BxOe`) — the run record, one per studio
  per week, with the four status fields and Notes
- `Time Punches` (`tblVxt2W7NanQmJFR`) — one row per punch, linked to Employees
  and Rates

### MTEK gotchas
- The endpoint is `/api/time_clock_shifts` (JSON:API, same pagination shape as
  `class_sessions`).
- **Only `min_start_datetime`/`max_start_datetime` actually filter.** Passing
  `min_date`/`max_date`/`start_date`/`end_date` is silently ignored and returns
  the *entire* unfiltered history — verified live. Those bounds are also applied
  loosely at the edges, so the script pads the query window by a day either side
  and enforces the real range against `America/Toronto` local dates itself.
- A punch carries employee and shift type as bare relationship IDs only. Names
  come from `include=employee.user,shift_type`: the employee's name lives on the
  nested `user` (`full_name`), not on `employees`.
- `shift_types[].name` (e.g. `MC/SC`, `EE`) matches the `Shift Type` options on
  the Rates table exactly, which is what makes the rate match work: Rates rows
  are named `"<Employee name> <Shift Type>"`.

### How a run gets triggered
`Budget week - Studio` has a `Fetch time punches` formula field holding a
clickable URL (`<Make webhook>?recordId=` + `RECORD_ID()`). Clicking it is a
plain browser GET, and GitHub's dispatch API needs an authenticated POST, so
the Make scenario **HR Fetch time punches** (id `6166754`, webhook `2777677`)
bridges the two: it reads `recordId` off the query string and sends a
`repository_dispatch` of type `airtable-hr-payroll-time-punches`, then responds
in the browser tab.

It authenticates with the **`GITHUB BEARER` keychain key** (Make key `86508`)
via `http:MakeRequest`, so no token is stored in the blueprint. That is the same
key and module the existing `e3un - Get new profiles yesterday` scenario uses —
copy that one if this ever needs rebuilding. Note Make **Keys** are a separate
store from Make **Connections**; the GitHub entries under Connections point at
`gitmcp.io` and Copilot and cannot authorize a dispatch.

### Employee matching
Employees are matched from the **"Active Employees - ALL" view**
(`viws8tSbvXfujLnwG`), not the whole table, via the `view` query parameter.

This is a correctness fix, not just a narrowing. The table holds **836** rows
including former staff, and **74 names are duplicated** across it. The lookup is
first-match-wins, so matching against everything resolved **14 people to a stale
Inactive/Offboarding record** rather than their current one.

Two things this does not solve: four names are duplicated *within* the active view and are still resolved first-match-wins; and anyone outside the
view no longer matches at all, which surfaces as `Employee Status: PROBLEM`.

### Derived fields on Time Punches
- `MTEK ID` — MarianaTek's shift id, the dedupe key (see below)
- `Total Hours` — a **formula** over the `Time In`/`Time Out` text, so a punch
  corrected by hand recalculates. The script deliberately does not write it.
  `MOD(... + 1440, 1440)` keeps a shift crossing midnight positive.
- `First name` / `Last name` / `Desjardins ID` — lookups via the Employee link
- `Hourly rate` — lookup of `Rate` via the Rate link
- `Wages` — `Total Hours * Hourly rate`

Deriving hours from `HH:MM` rather than MTEK's raw `duration` was checked
against the whole Rockland 2026-08-23..29 week: all 46 punches matched to the
cent (152.00 hours). MTEK appears to round a shift to the quarter hour once it
closes, so the two agree in practice; only an in-progress shift carries
sub-minute seconds that the text would drop.

The script recomputes the same `HH:MM` arithmetic in memory for its run notes,
so the reported totals always agree with the column.

### Re-running a fetch
Each punch stores MTEK's shift id in `MTEK ID`. A run reads back the ids already
linked to the Budget week - Studio record and skips them, so clicking **Fetch
time punches** twice creates nothing and reports the skip count in Notes.

A re-fetch also reconciles what is already on the row against MTEK, and never
overwrites a value someone may have corrected by hand:

- **Clock-out filled.** A punch imported while still open has a blank Time Out.
  Once MTEK has the clock-out, a re-fetch writes it in — but only into a blank
  field, so a hand correction is never touched. Without this, dedupe would skip
  the punch forever and its hours would stay at 0.
- **Differs from MTEK.** Any other disagreement (date, time in, time out) is
  listed in Notes and left alone. It may be an MTEK edit made after the fetch,
  or a deliberate correction in Airtable — the script can't tell which.
- **No longer in MTEK.** A punch on the row whose MTEK ID MTEK no longer returns
  is listed in Notes and left alone.

### An empty fetch fails
If MTEK returns no punches for the studio and week, the run throws and lands on
`PROBLEM` rather than reporting `COMPLETE` on nothing. Everyone works every day,
so an empty studio-week is always a mistake — usually a SPINCO studio (this
script only queries b.cycle's MTEK, where a SPINCO location returns 0) or the
wrong dates.

### The fetch proves it is complete
The fetch doesn't assume it got everything — it checks, and fails before writing
anything if it can't prove it:

- Every MTEK page must carry its data and paging info. A malformed page stops the
  run instead of being taken as the last page.
- MTEK's reported total must be identical on every page. A change means records
  shifted between pages while it was fetching.
- The number of **unique** punches received must equal MTEK's reported total.
  Counting unique IDs also catches a skipped record hidden behind a duplicated
  one, where the raw total would look right.
- A punch that appears on two pages is kept once.
- A punch whose start time can't be read fails the run rather than being skipped.

So either every punch MTEK reports for that studio and window arrives, or the run
ends on `PROBLEM` with the reason in Notes. Re-running is the recovery.

Tested by feeding the real fetch function nine synthetic page sequences (it
accepted the three complete ones and threw on all six faulty ones) and against
real multi-page MTEK data with no false alarm. `HR_PUNCHES_SKIP_RUN=1` skips the
sync so `fetchPaginatedMtek` can be imported and exercised on its own.

### Pay period assignment
Every new punch is linked to the **Payroll period** covering its date. All of a
run's punches are resolved against the Payroll period table *before anything is
written*: a date no period covers, or one covered by two overlapping periods,
fails the run and imports nothing. Fix the Payroll period table (or run HR Create
Payroll period) and re-fetch.

### Notes is an append-only log
Each run prepends a timestamped entry and keeps everything below it, so a
Budget week - Studio row reads as a history rather than only the last result:

```
[2026-09-10 09:50 EDT] # of Time punches in MTEK: 7 | # of New punches: 1 | # of Duplicates skipped: 6 | # of Clock-outs filled: 1 | # of Punches with no clock-out: 1 | # of Differs from MTEK: 1 | # of No longer in MTEK: 1 | ... | Week total hours: 26.50 | ...
Differs from MTEK (Airtable kept, not overwritten):
- <employee> 2026-08-24: Airtable 2026-08-24 07:00-10:30 / MTEK 2026-08-24 06:30-10:30
No longer in MTEK:
- <employee> 2026-08-24 15:00: not in MTEK any more

[2026-09-10 09:48 EDT] # of Time punches in MTEK: 7 | # of New punches: 7 | ...
```

Week totals cover every punch on the row, not only those a run created — filling
a clock-out changes the week's hours without creating anything.

The stamp carries the real zone abbreviation, so it reads `EDT` in summer and
`EST` in winter rather than being hardcoded. The field is re-read immediately
before writing, so the append is against whatever is actually there. A single
entry is capped at 5,000 characters and the whole field at 100,000, oldest
entries falling off the bottom — so a huge error message cannot wipe the history.

### Run status sequence
`Overall Status` is set to `Started` as the run's very first write, before the
try block — the same shape as `OVERALL Status` in the instructors payroll
script. It only reaches `COMPLETE` when nothing needs a human: an unmatched
employee or rate, or any punch still without a clock-out, lands it on
`PROBLEM`, as does any thrown error.

`Time punch Status` and `Time in/out Status` both go `Started` at the top of a
run and are set together once the punches are in: `Time punch Status` to
COMPLETE, and `Time in/out Status` to COMPLETE — or **PROBLEM** while any punch
on the row still has no clock-out. `Employee Status` goes
`Started` at that point, then COMPLETE — or **PROBLEM** if any employee went
unmatched. `Rate type Status` then goes `Started`, and COMPLETE or PROBLEM the
same way; an unmatched rate silently pays someone nothing, so it is treated as
a failure rather than a note. Employee/Rate statuses are cleared at the start
so a re-run cannot display the previous run's COMPLETE, and a mid-run crash
marks whichever phase was actually in flight.

### Known data issues
- `b.home` and `Vieux-port` in the HR Studios table share MTEK Location ID
  `48719`, so a fetch for either returns the same punches. Creating a Budget
  week - Studio row for both in the same week would double-count them.
- Many `EE` rows in the Rates table have a `Rate` of `$0.00`. Those punches
  match a rate record correctly but contribute nothing to Wages, so they look
  like unpaid hours rather than an unmatched-rate error.
- Dedupe is scoped to one Budget week - Studio row. A shift whose date is moved
  into a later week in MTEK is imported again there and paid twice; the old
  week's Date Range Check may flag it. A global check was considered and
  deliberately not added.

## HR Payroll Barter

Workflow file: `.github/workflows/hr-payroll-barter.yml`
Script: `scripts/hr-payroll-barter-workflow.mjs`

Imports staff barter (the `BARTER` promotion, promo code `TEAMBARTER`) from
MTEK into the **Barter** table of the **HR** base, one row per redemption, so
barter can be tied to the employee who used it and to the pay period.

### Triggers
- `workflow_dispatch` with required input `record_id`
- `repository_dispatch` with event type `airtable-hr-payroll-barter` and payload
  field `record_id`

`record_id` is a **Payroll period** record (`tbl9qw4kqw0BY0DyJ`); its Start and
End Date are the window. Payroll period has a `Fetch Barter` formula field
(`<Make webhook>?recordId=` + `RECORD_ID()`), bridged to GitHub by the Make
scenario **HR Fetch barter** (id `6234235`, webhook `2800037`) — a copy of HR
Fetch time punches using the same `GITHUB BEARER` keychain key (`86508`).

### Required secrets
- `AIRTABLE_TOKEN`
- `MTEK_API_TOKEN`

### Where the data comes from
MTEK's **Promotion Redemptions** table report (id `292`, slug
`promotion-redemptions`) — the same data as the `promotionsRedeemed.csv` export.
It goes through the shared async report fetcher (`scripts/lib/mtek-report.mjs`),
filtered with `min_order_date` / `max_order_date`; its `Date` column is studio
local time. There is no JSON:API resource for promotions
(`/api/promotions`, `/api/promotion_redemptions`, `/api/promo_codes` all 404), and
the report can't be filtered by promotion, so every redemption in the window
comes back and the script keeps rows whose `Promotion` is `BARTER`
(trimmed, case-insensitive). If a column the sync reads disappears from the
report, the run stops rather than importing blanks.

### Airtable schema (HR base)
- `Barter` (`tblYxeSSem1plIvIR`) — Order Number (primary, dedupe key), Payroll
  period, Employee, Customer ID, Customer Name, Customer Email, Discount Amount,
  Promotion, Promo Code, Order Products, Date
- On `Payroll period`: `Barter Status`, `Barter Employee Status`,
  `Barter Overall Status`, `Barter Notes`, `Fetch Barter`

### Dedupe and re-running
Order numbers are unique in MTEK (one BARTER redemption per order), so an Order
Number already anywhere in the Barter table is never imported again. A re-run
reports, without overwriting, any row whose Date or Discount Amount differs from
MTEK, and any row on the period that is no longer a BARTER redemption in MTEK.
Rows still missing an employee are re-matched on every run, so fixing an
employee's email in HR and clicking Fetch Barter again fills them in.

### Employee matching
Against the **Active Employees - ALL** view, like time punches: the customer's
email against `Email` and `Zingfit e-mail`, then their full name against `Name`.
A key shared by two active employees is ambiguous and left unmatched rather than
guessed. Customers with no match are listed by name and email in Barter Notes.

### Run status sequence
`Barter Overall Status` and `Barter Status` go `Started` (and `Barter Employee
Status` is cleared) before anything else. `Barter Status` becomes `COMPLETE -
Barter found` once rows are written, then `Barter Employee Status` runs.
Overall is COMPLETE only when every redemption on the period has an employee;
otherwise PROBLEM. A crash marks whichever phase was in flight PROBLEM, and
Barter Notes is append-only like the time punch Notes. An MTEK report with no
redemptions at all (any promotion) fails the run — usually wrong dates, or a
period that has only just started.

### Verification record
First run on 2026-08-23 -> 2026-09-05: 412 redemptions across all promotions, 92
BARTER, 92 created, 69 matched to an employee, 23 (13 customers) with no active
employee match — so Overall correctly reads PROBLEM. A second run created 0 and
skipped all 92.

The GitHub Actions log for this job is public (the repo is public), so the
script prints counts only. Customer names, emails and the period's discount
total are written to `Barter Notes` in Airtable and nowhere else.

## HR Create Budget week

Workflow file: `.github/workflows/hr-create-budget-week.yml`
Script: `scripts/hr-create-budget-week.mjs`

Runs Sundays at 08:00 UTC (4am America/Toronto). Each run:

1. **Creates the upcoming Budget week** — Start = the coming Sunday, End = Start + 6,
   so weeks run Sunday-Saturday and never overlap. No-op if it already exists.
2. **Assigns every Budget week to its Payroll period** — the week just created and
   any made by hand. The link is re-checked on every run, so a hand-edited link
   is put back on the period its dates belong to.
3. **Creates a Payroll period only when a week needs one.** Periods are 14 days,
   Sunday-Saturday, stepped from the latest period in the table (seeded by hand:
   `2026-08-23 -> 2026-09-05`). So a period appears one week before it starts,
   together with its first Budget week — never speculatively ahead. A week more than
   three periods past the latest one fails as a probable typo instead of filling the
   gap.
4. **Links any time punch with no pay period** to the existing period covering its
   date. It never creates a period for a punch.

Anything it can't place fails the run with a list of what and why, and GitHub sends
its usual failed-workflow notification. Weeks and punches dated before the first
period are ignored.

Payroll period `Name` is a formula in Airtable, so the job writes only the dates.

This job absorbed the short-lived standalone "HR Create Payroll period" job, so a
period and the weeks inside it are created at the same moment and can't drift apart.

The date arithmetic is done in code rather than by the AI step used by "Run Weekly
classes." in HR - Instructors — deliberately, since a wrong date silently produces a
wrong payroll week. Only HR works this way; the instructors base was left alone.

### Budget week fields
- `Payroll period` — set by the job.
- `Week of pay period` — formula: 1 if the week starts in the first 7 days of its
  pay period, 2 in the second 7. It reads `Pay period start`, a rollup of the linked
  period's Start Date, so it follows the link with no help from the job.
- `Studios completed` — unique names of that week's studios whose Budget week -
  Studio row has Overall Status COMPLETE: a rollup of the row's `Completed studio`
  formula. (Airtable's API can't create filtered rollups, so the formula does the
  filtering.)
- `WEEK 1` / `WEEK 2` — that same list, shown only in the column matching the week's
  place in its pay period.
