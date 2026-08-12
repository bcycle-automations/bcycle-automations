---
name: churn-risk-system
description: How b.cycle's churn-risk prediction system works end to end — BigQuery ML models, classification logic, the weekly scoring job, and the Airtable "Churn Risk" output. Read this BEFORE touching anything related to churn prediction, and UPDATE it after any change (new model, new threshold, new bug fix, new category built). This is the single source of truth for this project — do not re-derive it from scratch each session.
---

# b.cycle Churn Risk System

**Read this file in full before doing any work on churn prediction. Update it (don't just leave it stale) the moment anything described here changes — new model version, new bug found, new category shipped, threshold retuned, etc. Future sessions rely on this file instead of re-deriving everything from conversation history.**

## 1. What this is, in one paragraph

A weekly job scores every current b.cycle membership and credit-pack customer for churn risk using two BigQuery ML logistic regression models, writes results to a persistent BigQuery table, and syncs only *new or escalated* risk cases into an Airtable table where managers work them (Status: New → Contacted → Resolved/Lost). A third model estimates whether an already-flagged membership customer's attendance gap is *permanent* vs. a *temporary seasonal break*.

## 2. Where everything lives

**Repo**: `bcycle-automations/bcycle-automations` (GitHub, public)
- `scripts/churn-risk-weekly-scoring.mjs` — the production job
- `.github/workflows/churn-risk-weekly-scoring.yml` — schedule: **Monday 05:00 America/Toronto (cron `0 9 * * 1`, fixed UTC, not DST-adjusted)** + `workflow_dispatch` with `dry_run` (default true) and `seed_only` (default false) inputs
- `scripts/churn-risk-attendance-credit-slice.mjs` / `.yml` — an early throwaway prototype, superseded, harmless to ignore or delete

**BigQuery** (`root-cargo-453703-k7`, dataset `SalesZF` unless noted):
- `churn_model_membership` — production model, membership customers (LOGISTIC_REG)
- `churn_model_credit` — production model, credit-pack customers (LOGISTIC_REG)
- `churn_return_model` — production model, "will an already-silent membership customer actually come back" (LOGISTIC_REG)
- `churn_risk_scores` — persistent output table (one row per email+population), tracks `tier` and `previous_tier` so the job can diff week over week
- Everything suffixed `_temp` (e.g. `churn_model_training_v5_temp`, `purchases_v2_temp`, `category_at_checkpoint_v2_temp`) is exploratory/training scaffolding from model development — safe to leave, safe to delete, NOT depended on by the production job (which has all its SQL self-contained inline)

**Airtable**: base `appofCRTxHoIe6dXI` ("Customer Tracking Tool" — same base as Freeze/Cancellation forms), table `tblmykQyEGNBZ1Y84` ("Churn Risk"). Fields: Email, Name, Population (Membership/Credit Pack), Risk Score, Risk Tier (Critical/Watch), Key Signal, Last Visit Date, MTEK Customer ID, Status (New/Contacted/Resolved/Lost), Date Flagged, Tier Change, Internal Notes, **Likely Permanent %** (membership only).

**GitHub secrets used**: `GCP_BQ_SERVICE_ACCOUNT_KEY` (via `google-github-actions/auth@v2`), `AIRTABLE_TOKEN`, `SLACK_BIGQUERY_UPDATES_WEBHOOK_URL` (currently unused — see below).

**Env vars / flags**:
- `DRY_RUN` — default true for manual runs, always false on schedule. No BigQuery/Airtable writes when true.
- `SEED_ONLY` — populates `churn_risk_scores` via BigQuery but skips the Airtable API entirely. Use this after any model/logic change instead of letting the normal diff logic try to push a huge "everything changed" backlog through the Airtable API one record at a time (this happened once, ~1,900 records, had to be cancelled — see §6). After a `SEED_ONLY` run, export the Critical/Watch rows from `churn_risk_scores` to CSV and paste into Airtable manually.
- `ENABLE_SLACK` — **off by default, intentionally, per explicit user instruction.** Do not turn this on without asking. When enabled, posts to `#bigquery-updates` only when something actually changed (not on the first/seed run).

## 3. How customers are classified (Membership vs. Credit Pack)

**This is the trickiest part and has been rebuilt once already after a real bug — read carefully before changing it.**

Only `product_type IN ('Memberships', 'Credits')` (post-migration/SalesMTEK) or `TYPE = 'series'` (pre-migration/Sales_view) items ever factor in. Retail, food, penalty fees, gift cards, etc. never touch classification.

- **Era 1** (pre-Dec 3, 2025, `Sales_view`): item classified as `membership` if the item name matches `unlimit|illimit|member|bike.*(commit|month|autorenew)`, else `credit`. This regex was validated against the real distribution of item names.
- **Era 2** (post-migration, `SalesMTEK`): `product_type = 'Memberships'` → membership, `product_type = 'Credits'` → credit. Clean field, no regex needed.
- **Current category** (live scoring) = does the customer's most recent MEMBERSHIP purchase's parsed coverage window (purchase date + parsed commitment length + 7-day grace) still include today? If yes → `membership`, **regardless of a more recent credit top-up purchase**. If no, and they have any credit purchase → `credit`. If no membership coverage and no credit purchase but a lapsed membership exists → `membership` (lapsed).
  - **The bug this fixes**: originally, category was "whichever category was most recently purchased." A member who bought a $3 water-adjacent credit item the day after renewing would flip to "credit" classification even though their membership was still active. ~275 people were miscategorized this way; ~417 people the other direction (lapsed members incorrectly kept as "membership forever" under the old lifetime-history logic).
- **Commitment length parsing** (regex, confirmed against real data): `"X year(s)/an/année"` → X×365d, `"X month(s)/mois"` → X×30d, `"X week(s)/semaine"` → X×7d. Default 30d if unparseable. Real confirmed examples: "4 weeks" → 28d, "3 months" → 90d, "1 year" → 365d, "6 month commitment" (bike rental) → 180d.
- **Plan-implied weekly visit rate** (for attendance normalization, see §4): `unlimit|illimit` → 7.0 (uncapped), `"X classes/cours ... per week/par semaine"` → X, `"X classes/cours ... per month/par mois"` → X/4.33. Default 7.0 (unlimited) if unparseable, since that's the dominant plan type.
- **Live scoring population scope**: `last_visit_date` within the last 180 days (both populations — otherwise ancient permanently-lapsed accounts flood the pool). Credit additionally requires `last_purchase_date` within the last 90 days (otherwise ~90% of ever-purchased credit customers score Critical by default, since most one-time credit buyers never repurchase — this isn't actionable, it's just how that customer type behaves).

## 4. The three models

### `churn_model_membership` (production)
- **Features**: `gap_days` (days between customer's last two visits), `visits_trailing60`, `visits_prior60` (both windows relative to TODAY for live scoring), `freq_ratio` (trailing/prior, -1 sentinel if undefined), `visit_count` (lifetime), `is_single_visit`, `attendance_ratio` (visits_trailing60 ÷ what their specific plan implies over 60 days — NOT compared to the whole population's average; this fixes a real flaw where a 1x/week-plan member looked artificially "low" against unlimited-plan members), `month_sin`/`month_cos` (cyclical encoding of current month — genuine seasonal signal, confirmed via feature weights, not a leak — see §6 for how the first seasonal attempt WAS a leak and was caught/fixed).
- **Label** (training only): confirmed-silent 60–425 days as of the training snapshot, using a **multi-snapshot** methodology (7 reference dates spread across the past 14 months, not just "today") specifically to avoid an active-examples-all-cluster-in-one-month confound that broke the first seasonal attempt.
- **Threshold**: Critical ≥0.55, Watch ≥0.35.
- **Honest validated performance**: ~80% recall / ~82% precision for "is this person newly silent 60+ days" — but see §5, this label does NOT reliably mean "permanently gone."

### `churn_model_credit` (production)
- **Features**: `gap_days`, `visit_count`, `last_pack_size`, `days_since_credit_purchase`, `credits_remaining_est` (pack size − visits used since purchase), `credits_expired` (>28 days since purchase — every credit pack expires 28 days after purchase, confirmed from real data regardless of pack size), `expired_with_unused_credits` (**single strongest feature** — real signal, not attendance-borrowed noise), `days_until_expiry_est`, `is_downgrade` (was this person ever a member? Counterintuitively **protective** — ex-members-on-credits churn at 63.7% vs. 87.1% for lifetime-credit-only customers, so being a downgrade LOWERS risk).
- **Threshold**: Critical ≥0.53, Watch ≥0.35.
- **Honest performance**: ROC AUC 0.832, ~80% recall / ~94% precision at threshold — but base rate for this population is inherently high (most one-time credit buyers never return), so treat precision numbers with the same caution as membership.

### `churn_return_model` (production, membership only, secondary score)
- **What it answers**: given someone is ALREADY flagged silent, how likely is this permanent vs. a temporary break? This is a fundamentally different, harder question than "is this person silent" — see §5 for why the first two models structurally cannot answer it no matter how many features you add.
- **Label**: real historical outcomes. For every historical gap-start event (a visit followed by 60+ days of silence) in a customer's history, if a LATER visit exists in the data, label = 0 (returned, we know for certain regardless of how long ago). If it's their most-recent-ever visit and 425+ days have elapsed as of today, label = 1 (confirmed non-return, confident enough time has passed). Anything else (final visit, but not enough time elapsed yet to be sure) is **excluded as censored** — this is important, don't relax it to get more training data, it reintroduces the exact bias this model was built to fix.
- **Features**: `gap_days` (spacing pattern BEFORE the silence began), `visits_trailing60`/`visits_prior60` (relative to the pre-gap visit, not today), `freq_ratio`, `is_single_visit`, `month_sin`/`month_cos` of the pre-gap visit date (i.e., when did the silence begin).
- **Honest performance**: ROC AUC 0.707. At ~80% recall, ~31% precision (vs. 22% base rate — real but modest ~1.4x lift). `month_sin`/`month_cos` are the two largest feature weights — genuine validated seasonal signal.
- **Known limitation in the live wiring**: in the current `churn-risk-weekly-scoring.mjs`, the `visits_trailing60`/`visits_prior60` inputs to this model are stubbed to 0 rather than properly re-derived as-of the last-visit date (see comment in the script at the `return_predicted` CTE). This was a scope-management shortcut when wiring it in — the model still gets `gap_days` and month signal correctly, but is not using its full validated feature set live. **This should be fixed properly** (recompute visits_trailing60/prior60 relative to `last_visit_date`, matching how `churn_return_training_temp` was built) before leaning on "Likely Permanent %" too heavily operationally.

## 5. The central finding that shaped this whole system — READ THIS

Early versions of the membership model claimed ~80% recall / 82% precision for predicting "churn," which sounds like a real cancellation predictor. **It wasn't.** Rigorous testing (pick a reference date 12 months ago, apply the exact same labeling rule as if it were "today," then check the real data since then) found:

- **53.4%** of people labeled "churned" 12 months ago, using a loose population, had actually returned since.
- Redone with the population correctly bounded to match the real training methodology (60–425 days silent, not unbounded history): **70.6% false-positive rate.** Recently-silent people are the ones MOST likely to come back, not least — extending the confirmation window (tested 60/90/120/150/180/270 days) barely moved this number, because it doesn't address the real issue.
- **Root cause**: "silent 60+ days" and "permanently gone" are different questions. The original models were only ever trained to distinguish "active" from "silent" — trivially easy — never to distinguish, within the silent population, who's gone for good vs. on a break. No amount of feature engineering on the OLD label fixes this; it required a genuinely different model (`churn_return_model`, §4) trained on real resolved historical outcomes.
- **A first attempt to add season as a feature produced an artifact, not a real signal** — caught before shipping: "active" (label=0) training examples were always sampled from "last 21 days" relative to whenever the script was run, so they all clustered in one calendar month, while "churned" examples spanned many months. The model learned "if checkpoint month = current month → active," which is a leak, not seasonality (giveaway: absurd feature weights, 8+ in magnitude). Fixed by sampling both classes from **multiple historical reference dates**, not just "today" — see the multi-snapshot methodology in §4.

**Practical framing to use going forward**: the membership/credit models flag "notable attendance gap, worth outreach" with real, validated 80%/~82-94% numbers for that specific claim. The `churn_return_model` gives a genuine, if modest (AUC 0.707), read on permanence, factoring in season. Don't conflate the two, and don't undersell the return model's honesty by rounding it up to sound like the others.

## 6. Other things worth knowing

- **Report 336 (`customers-details`, MTEK Reports API)** already computes RFM scores, Retention/Activity Segment, Membership Status, and Total Remaining Credits per customer — but none of it is synced anywhere (the existing `mtek-customers-to-airtable.js` only pulls a subset of fields into the CRM `Customers` table). Not used by the churn-risk system currently; could be a future data source but would need its own sync job. `Activity Segment` specifically is NOT usable as a churn signal (it's a lifetime engagement tier, ~46% of ALL customers ever are "At Risk" by that definition — recency isn't part of it).
- **Satisfaction Trend** (Ratings/Feedback data) was tried and added ~nothing (AUC moved 0.885→0.890, negligible) — the `Clients` rollup table only has all-time aggregates, not a real time-windowed trend, and only covered 62% of the training population. Not wired into any production model. Would need the full 51,609-row Feedback table with real per-rating dates to do properly, not attempted due to Airtable API pagination cost.
- **Price Sensitivity** was tested against real 14-month behavioral data (not survey data, per explicit instruction not to train on the small 2-month-old Cancellations form) and found the OPPOSITE of the assumed direction: discount/promo buyers churn LESS (25.6%) than full-price payers (43.5%), likely because promos here are mostly win-back/acquisition tools pulling in already-motivated people. **Not implemented as a risk-increasing feature** — the real data doesn't support the original hypothesis.
- **Life-Event** (freeze reason = moving/travel) — the "would you consider returning" field on the Cancellations form is nearly universally "yes" (93%, and 81% even for moving/travel-specific cancellations), so it doesn't discriminate. Best used as a blanket follow-up policy, not a targeting signal. Not implemented.
- **Contract-End Window** — blocked. Needs per-membership commitment/remaining-renewal data from the MTEK Admin API; only the plan catalog (`/api/memberships`) was reachable, not individual customer contract instances. Would need a different Admin API resource or Admin API access not yet available.
- **Downgrade Trend** — folded into the credit model as the `is_downgrade` feature (§4). Not a separate category/model.
- **API-volume incident**: the very first live seed run tried to write ~1,900 records to Airtable one-by-one (search + write per record, no batching) and had to be cancelled mid-run. This is why `SEED_ONLY` mode + manual CSV export exists — **always use `SEED_ONLY` + CSV export after any model/logic change, never let the normal path try to push a large backlog through the Airtable API.**
- **A real BigQuery view bug exists independently of this project**: `SalesZF.Sales_with_customers` joins `Sales_view.\`CUSTOMER ID\`` (STRING) to `Customers.id` (INT64) without a cast, and errors when actually queried. The production script works around this with its own explicit `CAST(...AS STRING)` join — don't use `Sales_with_customers` directly for anything.
- **b.cycle only.** SPINCO has its own, separate, still-functioning churn model (`SPINCO.churn_model_logreg_v3_temp` or similar — built earlier, AUC 0.832, not touched by the b.cycle rebuild described in §2-5) built during the same session, on a much cleaner dataset (single continuous schema, no era split, clean `Product Type` field). Not currently operationalized into a weekly job or Airtable output — only b.cycle was operationalized, per explicit user direction ("no no i jjst want to do with bcycle").

## 7. Current status (as of last update)

- ✅ Weekly scoring job live on schedule (Monday 5am ET), `DRY_RUN`/`SEED_ONLY` tested working
- ✅ Membership model: duration-aware classification + plan-normalized attendance + season, rebuilt and validated (AUC 0.912 in-sample; see §5 for the honest caveat on what the label really means)
- ✅ Credit model: real balance/expiry signal, `is_downgrade`, validated (AUC 0.832)
- ✅ `churn_return_model` built, validated (AUC 0.707), wired into the live script as "Likely Permanent %" — **but with the known feature-stubbing shortcut noted in §4, should be revisited**
- ✅ BigQuery seeded (`SEED_ONLY` run completed), CSV exported and delivered to user for manual Airtable paste (2,472 rows: 466 membership Critical / 192 Watch, 1,648 credit Critical / 166 Watch)
- ⏳ User needs to confirm the CSV was actually pasted into Airtable — not yet confirmed as of last update
- ⏳ Slack notifications OFF (`ENABLE_SLACK` unset) — waiting on explicit user go-ahead before enabling
- ❌ Not started: fixing the `visits_trailing60`/`visits_prior60` stub in the return-model wiring (§4)
- ❌ Not started: Contract-End Window (blocked on API access), full Satisfaction Trend rebuild, SPINCO operationalization

## 8. Working conventions this user has established (don't relearn these)

- Push diagnostic/prototype work to a feature branch first; merge to `main` only when needed (GitHub `workflow_dispatch` requires the workflow file to exist on the default branch — this is a real GitHub limitation hit twice, not a preference).
- Quote YAML workflow `name:` fields if they start with a bracket or special character — bare `[Diagnostic] ...` breaks YAML parsing (hit this once).
- Never train/calibrate anything on the Cancellations Airtable form data — it's only ~2 months old, too short a window. Use real BigQuery behavioral history instead.
- User wants rigor over reassuring numbers — has caught real bugs (classification flip-flop, tier-normalization gap, seasonal leak, censoring bias) by pushing back on results that looked too clean. Don't round up honest-but-modest numbers to sound better.
- Prefers CSV export + manual paste over bulk Airtable API writes for anything more than a handful of records.
- Wants plain-language explanations with real worked arithmetic when asked "how does this actually work" — see conversation history for the exact style (ML.EXPLAIN_PREDICT walkthroughs with real customers, real numbers).
