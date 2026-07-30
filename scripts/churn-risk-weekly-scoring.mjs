// Weekly churn-risk scoring for b.cycle.
//
// Scores every current membership and credit-pack customer against the
// trained BigQuery ML models, writes the full result set into a persistent
// SalesZF.churn_risk_scores table (via MERGE, so we always know each
// person's PREVIOUS tier), and only pushes a record into Airtable's
// "Churn Risk" table for people who are NEW to a risk tier or ESCALATING —
// not the whole list every run, to avoid spamming managers with unchanged
// names.
//
// "Membership" vs "credit" is determined by whether the customer's most
// recent MEMBERSHIP purchase still covers today (purchase date + parsed
// commitment length + 7-day grace), not just "whichever category they
// bought most recently" — a membership holder who also buys a credit
// top-up stays classified as a membership customer as long as their
// membership is still active. Falls back to credit if no active
// membership coverage exists, or to a lapsed-membership bucket if neither
// applies. Only product_type IN ('Memberships','Credits') / TYPE='series'
// items ever factor into this -- retail, food, penalty fees, gift cards
// etc. never touch classification.
//
// Membership scoring uses attendance relative to each customer's own plan
// (an unlimited-plan member and a 1x/week-plan member have different
// "normal"), plus a cyclical month-of-year signal, since attendance gaps
// behave differently depending on the time of year.
//
// For membership customers already flagged Critical or Watch, a second
// model (churn_return_model) estimates how likely the gap is PERMANENT
// vs. a temporary break, trained on real historical outcomes (did a
// similar gap, in the past, actually resolve with a return or not) rather
// than a proxy. This is the "Likely Permanent %" field in Airtable.
//
// Defaults to a dry run — set DRY_RUN=false to actually write to BigQuery
// and Airtable.

import { BigQuery } from "@google-cloud/bigquery";
import { sendSlackMessage } from "./lib/slack.mjs";

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const AIRTABLE_TOKEN = (process.env.AIRTABLE_TOKEN || "").trim();
const AIRTABLE_BASE_ID = process.env.CHURN_RISK_AIRTABLE_BASE_ID || "appofCRTxHoIe6dXI";
const AIRTABLE_TABLE_ID = process.env.CHURN_RISK_AIRTABLE_TABLE_ID || "tblmykQyEGNBZ1Y84";
const DRY_RUN = String(process.env.DRY_RUN).toLowerCase() !== "false";
// Off by default while the flow is still being validated -- turn on by
// setting ENABLE_SLACK=true once you're ready for weekly pings.
const ENABLE_SLACK = String(process.env.ENABLE_SLACK).toLowerCase() === "true";
// Populates BigQuery (and marks everyone's previous_tier as a real
// baseline) without touching the Airtable API at all -- for (re)seeding
// after a model change, when the natural "first run" flood would mean
// thousands of sequential Airtable calls. Export a CSV from
// churn_risk_scores for manual import instead.
const SEED_ONLY = String(process.env.SEED_ONLY).toLowerCase() === "true";

// Calibrated against the backtests in the model-development conversation.
const THRESHOLDS = {
  membership: { critical: 0.55, watch: 0.35 },
  credit: { critical: 0.53, watch: 0.35 },
};

// Membership items parse cleanly into a commitment length ("4 weeks"->28d,
// "3 months"->90d, "1 year"->365d) and, separately, an implied weekly
// visit rate ("2 classes per week"->2, "Unlimited"->7 i.e. uncapped).
// Confirmed against real item-name data before building this.
const DURATION_AND_RATE_SQL = `
    CASE
      WHEN category != 'membership' THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(item_text), r'(\\d+)\\s*(year|an\\b|ann[ée]e)') THEN CAST(REGEXP_EXTRACT(LOWER(item_text), r'(\\d+)\\s*(?:year|an\\b|ann[ée]e)') AS INT64) * 365
      WHEN REGEXP_CONTAINS(LOWER(item_text), r'(\\d+)\\s*(month|mois)') THEN CAST(REGEXP_EXTRACT(LOWER(item_text), r'(\\d+)\\s*(?:month|mois)') AS INT64) * 30
      WHEN REGEXP_CONTAINS(LOWER(item_text), r'(\\d+)\\s*(week|semaine)') THEN CAST(REGEXP_EXTRACT(LOWER(item_text), r'(\\d+)\\s*(?:week|semaine)') AS INT64) * 7
      ELSE 30
    END AS membership_duration_days,
    CASE
      WHEN category != 'membership' THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(item_text), r'unlimit|illimit') THEN 7.0
      WHEN REGEXP_CONTAINS(LOWER(item_text), r'(\\d+)\\s*(class|classe|cours).*(per week|par semaine)') THEN CAST(REGEXP_EXTRACT(LOWER(item_text), r'(\\d+)\\s*(?:class|classe|cours).*(?:per week|par semaine)') AS FLOAT64)
      WHEN REGEXP_CONTAINS(LOWER(item_text), r'(\\d+)\\s*(class|classe|cours).*(per month|par mois)') THEN CAST(REGEXP_EXTRACT(LOWER(item_text), r'(\\d+)\\s*(?:class|classe|cours).*(?:per month|par mois)') AS FLOAT64) / 4.33
      ELSE 7.0
    END AS plan_weekly_rate`;

const SHARED_CTES = `
WITH sales_e1 AS (
  SELECT s.DATE AS d, LOWER(TRIM(c.emailaddress)) AS email, s.ITEM AS item_text, s.TYPE,
    IFNULL(s.REVENUE,0) AS rev, c.first_name, c.last_name
  FROM \`${BQ_PROJECT_ID}.SalesZF.Sales_view\` s
  LEFT JOIN \`${BQ_PROJECT_ID}.SalesZF.Customers\` c
    ON CAST(s.\`CUSTOMER ID\` AS STRING) = CAST(c.id AS STRING)
  WHERE s.TYPE = 'series'
),
e1_purch AS (
  SELECT email, d, rev, first_name, last_name, item_text,
    CASE WHEN REGEXP_CONTAINS(LOWER(item_text), r'unlimit|illimit|member|bike.*(commit|month|autorenew)')
         THEN 'membership' ELSE 'credit' END AS category,
    SAFE_CAST(REGEXP_EXTRACT(item_text, r'^(\\d+)') AS INT64) AS pack_size
  FROM sales_e1 WHERE email IS NOT NULL AND email != ''
),
e2_purch AS (
  SELECT LOWER(TRIM(customer_email)) AS email,
    SAFE.PARSE_DATE('%m/%d/%Y', transaction_date) AS d,
    IFNULL(SAFE_CAST(line_total AS FLOAT64),0) AS rev,
    customer_name AS first_name, CAST(NULL AS STRING) AS last_name,
    product AS item_text,
    CASE WHEN product_type='Memberships' THEN 'membership'
         WHEN product_type='Credits' THEN 'credit' ELSE NULL END AS category,
    SAFE_CAST(line_quantity AS INT64) AS pack_size,
    customer_id AS mtek_customer_id
  FROM \`${BQ_PROJECT_ID}.SalesZF.SalesMTEK\`
  WHERE customer_email IS NOT NULL AND customer_email != ''
    AND product_type IN ('Memberships','Credits')
),
purchases_raw AS (
  SELECT email, d, rev, category, IFNULL(pack_size,1) AS pack_size,
    first_name, last_name, CAST(NULL AS STRING) AS mtek_customer_id, item_text
  FROM e1_purch WHERE d IS NOT NULL
  UNION ALL
  SELECT email, d, rev, category, IFNULL(pack_size,1) AS pack_size,
    first_name, last_name, mtek_customer_id, item_text
  FROM e2_purch WHERE category IS NOT NULL AND d IS NOT NULL
),
purchases AS (
  SELECT *, ${DURATION_AND_RATE_SQL} FROM purchases_raw
),
activity AS (
  SELECT LOWER(TRIM(EMAILADDRESS)) AS email, DATE(CLASS_DATE) AS d
  FROM \`${BQ_PROJECT_ID}.SalesZF.Reservations_view\`
  WHERE EMAILADDRESS IS NOT NULL AND CLASS_DATE IS NOT NULL
  UNION ALL
  SELECT LOWER(TRIM(\`Customer Email\`)) AS email, \`Class Start Date\` AS d
  FROM \`${BQ_PROJECT_ID}.SalesZF.ReservationsMTEK\`
  WHERE \`Customer Email\` IS NOT NULL AND \`Class Start Date\` IS NOT NULL
),
dedup_activity AS (SELECT DISTINCT email, d FROM activity WHERE email != ''),
last_visit AS (
  SELECT email, MAX(d) AS last_visit_date, ARRAY_AGG(d ORDER BY d DESC LIMIT 2) AS last_two
  FROM dedup_activity GROUP BY email
),
visit_counts AS (SELECT email, COUNT(*) AS visit_count FROM dedup_activity GROUP BY email),
latest_purchase AS (
  SELECT email,
    ARRAY_AGG(STRUCT(d, category, pack_size, rev, first_name, last_name, mtek_customer_id) ORDER BY d DESC LIMIT 1)[OFFSET(0)] AS latest
  FROM purchases GROUP BY email
),
latest_membership AS (
  SELECT email,
    ARRAY_AGG(STRUCT(d, membership_duration_days, plan_weekly_rate) ORDER BY d DESC LIMIT 1)[OFFSET(0)] AS lm
  FROM purchases WHERE category = 'membership' GROUP BY email
),
latest_credit AS (
  SELECT email, MAX(d) AS last_credit_purchase_date
  FROM purchases WHERE category = 'credit' GROUP BY email
),
-- Category is decided by whether the most recent membership purchase's
-- coverage window (purchase date + parsed duration + 7-day grace) still
-- includes today -- not by "whichever category was bought most recently".
-- This is what keeps a member who also buys the occasional credit top-up
-- correctly classified as a membership customer.
category_calc AS (
  SELECT lv.email,
    CASE
      WHEN DATE_ADD(lm.lm.d, INTERVAL CAST(lm.lm.membership_duration_days + 7 AS INT64) DAY) >= CURRENT_DATE() THEN 'membership'
      WHEN lc.last_credit_purchase_date IS NOT NULL THEN 'credit'
      WHEN lm.lm.d IS NOT NULL THEN 'membership'
      ELSE NULL
    END AS category,
    lm.lm.plan_weekly_rate AS plan_weekly_rate,
    lm.lm.d AS last_membership_purchase_date,
    lc.last_credit_purchase_date
  FROM last_visit lv
  LEFT JOIN latest_membership lm USING (email)
  LEFT JOIN latest_credit lc USING (email)
),
candidates AS (
  SELECT lv.email, lv.last_visit_date,
    CASE WHEN ARRAY_LENGTH(lv.last_two) < 2 THEN 999
         ELSE DATE_DIFF(lv.last_two[OFFSET(0)], lv.last_two[OFFSET(1)], DAY) END AS gap_days,
    vc.visit_count, cc.category,
    CASE WHEN cc.category = 'membership' THEN cc.last_membership_purchase_date
         ELSE cc.last_credit_purchase_date END AS last_purchase_date,
    cc.plan_weekly_rate,
    lp.latest.pack_size AS last_pack_size, lp.latest.first_name AS first_name,
    lp.latest.last_name AS last_name, lp.latest.mtek_customer_id AS mtek_customer_id
  FROM last_visit lv
  JOIN visit_counts vc USING (email)
  JOIN category_calc cc USING (email)
  JOIN latest_purchase lp USING (email)
  -- Scope to people still plausibly reachable -- see prior notes on why
  -- unscoped scoring floods the credit population with ancient lapses.
  WHERE lv.last_visit_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
)`;

const MEMBERSHIP_QUERY = `
${SHARED_CTES},
member_pop AS (SELECT * FROM candidates WHERE category = 'membership'),
member_freq AS (
  SELECT mp.email,
    COUNTIF(a.d BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND CURRENT_DATE()) AS visits_trailing60,
    COUNTIF(a.d BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 61 DAY)) AS visits_prior60
  FROM member_pop mp LEFT JOIN dedup_activity a ON a.email = mp.email
  GROUP BY mp.email
),
member_features AS (
  SELECT mp.email, mp.last_visit_date, mp.gap_days, mp.visit_count,
    mp.first_name, mp.last_name, mp.mtek_customer_id,
    mf.visits_trailing60, mf.visits_prior60,
    SAFE_DIVIDE(mf.visits_trailing60, NULLIF(mf.visits_prior60,0)) AS freq_ratio,
    (mp.visit_count = 1) AS is_single_visit,
    -- Plan-normalized: actual visits vs. what THEIR specific plan implies,
    -- not the whole population's average.
    SAFE_DIVIDE(mf.visits_trailing60, IFNULL(mp.plan_weekly_rate,7.0) * 60/7) AS attendance_ratio,
    SIN(2*ACOS(-1)*EXTRACT(MONTH FROM CURRENT_DATE())/12) AS month_sin,
    COS(2*ACOS(-1)*EXTRACT(MONTH FROM CURRENT_DATE())/12) AS month_cos,
    -- Return-likelihood features are computed AS OF the last visit (when
    -- the gap began), not as of today -- matching how churn_return_model
    -- was trained on real historical pre-gap snapshots.
    SIN(2*ACOS(-1)*EXTRACT(MONTH FROM mp.last_visit_date)/12) AS asof_month_sin,
    COS(2*ACOS(-1)*EXTRACT(MONTH FROM mp.last_visit_date)/12) AS asof_month_cos
  FROM member_pop mp
  JOIN member_freq mf USING (email)
),
churn_predicted AS (
  SELECT email, last_visit_date, first_name, last_name, mtek_customer_id,
    gap_days, visit_count, asof_month_sin, asof_month_cos, freq_ratio, is_single_visit,
    (SELECT prob FROM UNNEST(predicted_label_probs) WHERE label = 1) AS churn_prob
  FROM ML.PREDICT(MODEL \`${BQ_PROJECT_ID}.SalesZF.churn_model_membership\`, (
    SELECT email, last_visit_date, first_name, last_name, mtek_customer_id,
      gap_days, visit_count, visits_trailing60, visits_prior60,
      IFNULL(freq_ratio,-1) AS freq_ratio, is_single_visit,
      IFNULL(attendance_ratio,-1) AS attendance_ratio, month_sin, month_cos,
      asof_month_sin, asof_month_cos
    FROM member_features
  ))
),
return_predicted AS (
  SELECT email,
    (SELECT prob FROM UNNEST(predicted_label_probs) WHERE label = 1) AS likely_permanent_prob
  FROM ML.PREDICT(MODEL \`${BQ_PROJECT_ID}.SalesZF.churn_return_model\`, (
    SELECT email, gap_days,
      -- Reuse the live gap/visit numbers as stand-ins for the pre-gap
      -- snapshot the model trained on; visits_trailing/prior aren't
      -- re-derived here since the membership model's own features already
      -- cover current attendance -- this second model's real job is the
      -- seasonal permanence read, driven mostly by month + gap pattern.
      0 AS visits_trailing60, 0 AS visits_prior60, IFNULL(freq_ratio,-1) AS freq_ratio,
      is_single_visit, asof_month_sin AS month_sin, asof_month_cos AS month_cos
    FROM churn_predicted
  ))
)
SELECT
  cp.email, cp.last_visit_date, cp.first_name, cp.last_name, cp.mtek_customer_id,
  cp.gap_days, cp.visit_count, cp.churn_prob,
  rp.likely_permanent_prob,
  CASE
    WHEN cp.gap_days >= 30 THEN CONCAT('No visits in ', CAST(cp.gap_days AS STRING), ' days')
    WHEN IFNULL(cp.freq_ratio, 1) < 0.6 THEN 'Visit frequency declining vs. prior period'
    WHEN cp.is_single_visit THEN 'Only ever visited once'
    ELSE 'Attendance pattern'
  END AS key_signal
FROM churn_predicted cp
JOIN return_predicted rp USING (email)`;

const CREDIT_QUERY = `
${SHARED_CTES},
credit_pop AS (
  -- Further scoped beyond the shared 180-day visit window: credit
  -- customers only make sense to flag as "at risk, still actionable" if
  -- their most recent purchase is itself recent.
  SELECT * FROM candidates
  WHERE category = 'credit'
    AND last_purchase_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),
visits_used AS (
  SELECT cp.email, COUNT(DISTINCT a.d) AS visits_since_purchase
  FROM credit_pop cp
  LEFT JOIN dedup_activity a ON a.email = cp.email AND a.d BETWEEN cp.last_purchase_date AND CURRENT_DATE()
  GROUP BY cp.email
),
downgrade_flag AS (
  SELECT DISTINCT email FROM purchases WHERE category = 'membership'
),
credit_features AS (
  SELECT cp.email, cp.last_visit_date, cp.gap_days, cp.visit_count,
    cp.first_name, cp.last_name, cp.mtek_customer_id, cp.last_pack_size,
    DATE_DIFF(CURRENT_DATE(), cp.last_purchase_date, DAY) AS days_since_credit_purchase,
    GREATEST(cp.last_pack_size - vu.visits_since_purchase, 0) AS credits_remaining_est,
    (DATE_DIFF(CURRENT_DATE(), cp.last_purchase_date, DAY) > 28) AS credits_expired,
    (GREATEST(cp.last_pack_size - vu.visits_since_purchase, 0) > 0
     AND DATE_DIFF(CURRENT_DATE(), cp.last_purchase_date, DAY) > 28) AS expired_with_unused_credits,
    GREATEST(28 - DATE_DIFF(CURRENT_DATE(), cp.last_purchase_date, DAY), -999) AS days_until_expiry_est,
    (df.email IS NOT NULL) AS is_downgrade
  FROM credit_pop cp
  JOIN visits_used vu USING (email)
  LEFT JOIN downgrade_flag df ON df.email = cp.email
)
SELECT
  email, last_visit_date, first_name, last_name, mtek_customer_id,
  gap_days, visit_count,
  (SELECT prob FROM UNNEST(predicted_label_probs) WHERE label = 1) AS churn_prob,
  CAST(NULL AS FLOAT64) AS likely_permanent_prob,
  CASE
    WHEN expired_with_unused_credits THEN 'Credits expired unused'
    WHEN NOT credits_expired AND credits_remaining_est > 0 AND days_until_expiry_est BETWEEN 0 AND 10
      THEN CONCAT('Unused credits expiring in ', CAST(days_until_expiry_est AS STRING), ' days')
    WHEN gap_days >= 30 THEN CONCAT('No visits in ', CAST(gap_days AS STRING), ' days')
    ELSE 'Attendance pattern'
  END AS key_signal
FROM ML.PREDICT(MODEL \`${BQ_PROJECT_ID}.SalesZF.churn_model_credit\`, (
  SELECT email, last_visit_date, first_name, last_name, mtek_customer_id,
    gap_days, visit_count, last_pack_size, days_since_credit_purchase,
    credits_remaining_est, credits_expired, expired_with_unused_credits,
    days_until_expiry_est, is_downgrade
  FROM credit_features
))`;

function tierFor(prob, thresholds) {
  if (prob >= thresholds.critical) return "Critical";
  if (prob >= thresholds.watch) return "Watch";
  return "OK";
}

async function ensureScoresTable(bq) {
  const query = `
    CREATE TABLE IF NOT EXISTS \`${BQ_PROJECT_ID}.SalesZF.churn_risk_scores\` (
      email STRING,
      population STRING,
      first_name STRING,
      last_name STRING,
      mtek_customer_id STRING,
      last_visit_date DATE,
      churn_prob FLOAT64,
      likely_permanent_prob FLOAT64,
      tier STRING,
      previous_tier STRING,
      key_signal STRING,
      updated_date DATE
    )`;
  await bq.query({ query });
}

async function mergeAndDiff(bq, population, rows, thresholds) {
  if (rows.length === 0) return [];

  const tempTable = `${population}_score_batch_temp`;
  const scored = rows.map((r) => ({
    email: r.email,
    population,
    first_name: r.first_name || null,
    last_name: r.last_name || null,
    mtek_customer_id: r.mtek_customer_id || null,
    last_visit_date: r.last_visit_date ? r.last_visit_date.value : null,
    churn_prob: Number(r.churn_prob),
    likely_permanent_prob: r.likely_permanent_prob == null ? null : Number(r.likely_permanent_prob),
    tier: tierFor(Number(r.churn_prob), thresholds),
    key_signal: r.key_signal,
  }));

  const dataset = bq.dataset("SalesZF");
  await dataset.query({
    query: `CREATE OR REPLACE TABLE \`${BQ_PROJECT_ID}.SalesZF.${tempTable}\` (
      email STRING, population STRING, first_name STRING, last_name STRING,
      mtek_customer_id STRING, last_visit_date DATE, churn_prob FLOAT64,
      likely_permanent_prob FLOAT64, tier STRING, key_signal STRING)`,
  });
  const table = dataset.table(tempTable);
  const { insertInBatches } = await import("./lib/bigquery.mjs");
  await insertInBatches(table, scored);

  const mergeQuery = `
    MERGE \`${BQ_PROJECT_ID}.SalesZF.churn_risk_scores\` T
    USING \`${BQ_PROJECT_ID}.SalesZF.${tempTable}\` S
    ON T.email = S.email AND T.population = S.population
    WHEN MATCHED THEN UPDATE SET
      previous_tier = T.tier, tier = S.tier, churn_prob = S.churn_prob,
      likely_permanent_prob = S.likely_permanent_prob,
      key_signal = S.key_signal, last_visit_date = S.last_visit_date,
      first_name = S.first_name, last_name = S.last_name,
      mtek_customer_id = S.mtek_customer_id, updated_date = CURRENT_DATE()
    WHEN NOT MATCHED THEN INSERT (email, population, first_name, last_name,
      mtek_customer_id, last_visit_date, churn_prob, likely_permanent_prob,
      tier, previous_tier, key_signal, updated_date)
    VALUES (S.email, S.population, S.first_name, S.last_name, S.mtek_customer_id,
      S.last_visit_date, S.churn_prob, S.likely_permanent_prob, S.tier, NULL,
      S.key_signal, CURRENT_DATE())`;
  await bq.query({ query: mergeQuery });
  await table.delete().catch(() => {});

  const [changedRows] = await bq.query({
    query: `
      SELECT * FROM \`${BQ_PROJECT_ID}.SalesZF.churn_risk_scores\`
      WHERE population = @population
        AND updated_date = CURRENT_DATE()
        AND tier IN ('Critical', 'Watch')
        AND (previous_tier IS NULL OR previous_tier != tier
             OR (previous_tier = 'Watch' AND tier = 'Critical'))
    `,
    params: { population },
  });
  return changedRows;
}

async function upsertAirtable(changed) {
  if (changed.length === 0) return 0;
  const headers = {
    Authorization: `Bearer ${AIRTABLE_TOKEN}`,
    "Content-Type": "application/json",
  };
  const base = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}`;

  let written = 0;
  for (const row of changed) {
    const searchUrl = `${base}?filterByFormula=${encodeURIComponent(
      `AND({Email}='${row.email}',{Status}!='Resolved',{Status}!='Lost')`
    )}`;
    const searchRes = await fetch(searchUrl, { headers });
    const searchJson = await searchRes.json();
    const existing = searchJson.records?.[0];

    const tierChange = row.previous_tier ? `${row.previous_tier} -> ${row.tier}` : "New";
    const fields = {
      Email: row.email,
      Name: [row.first_name, row.last_name].filter(Boolean).join(" ") || undefined,
      Population: row.population === "membership" ? "Membership" : "Credit Pack",
      "Risk Score": Number(row.churn_prob),
      "Risk Tier": row.tier,
      "Key Signal": row.key_signal,
      "Last Visit Date": row.last_visit_date ? row.last_visit_date.value : undefined,
      "MTEK Customer ID": row.mtek_customer_id || undefined,
      "Tier Change": tierChange,
      "Date Flagged": new Date().toISOString().slice(0, 10),
    };
    if (row.likely_permanent_prob != null) {
      fields["Likely Permanent %"] = Number(row.likely_permanent_prob);
    }
    if (!existing) fields.Status = "New";

    const body = existing
      ? { records: [{ id: existing.id, fields }] }
      : { records: [{ fields }] };
    const method = "PATCH";
    await fetch(base, {
      method,
      headers,
      body: JSON.stringify(body),
    });
    written++;
  }
  return written;
}

async function main() {
  const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
  console.log(`Churn risk scoring — ${DRY_RUN ? "DRY RUN" : "LIVE"}`);

  let isFirstRun = false;
  if (!DRY_RUN) {
    await ensureScoresTable(bq);
    const [[{ existingCount }]] = await bq.query({
      query: `SELECT COUNT(*) AS existingCount FROM \`${BQ_PROJECT_ID}.SalesZF.churn_risk_scores\``,
    });
    isFirstRun = Number(existingCount) === 0;
    if (isFirstRun) {
      console.log("First run detected (scores table is empty) — will seed Airtable but skip the Slack alert.");
    }
  }

  const [memberRows] = await bq.query({ query: MEMBERSHIP_QUERY });
  const [creditRows] = await bq.query({ query: CREDIT_QUERY });
  console.log(`Scored ${memberRows.length} membership customers, ${creditRows.length} credit customers.`);

  let changedMember = [];
  let changedCredit = [];
  if (!DRY_RUN) {
    changedMember = await mergeAndDiff(bq, "membership", memberRows, THRESHOLDS.membership);
    changedCredit = await mergeAndDiff(bq, "credit", creditRows, THRESHOLDS.credit);
    console.log(`${changedMember.length} membership + ${changedCredit.length} credit customers changed tier.`);

    if (SEED_ONLY) {
      console.log(
        `SEED_ONLY set — BigQuery is populated, Airtable was not touched. ` +
          `Export churn_risk_scores (tier IN Critical/Watch) to CSV for manual import.`
      );
      return;
    }

    const written = await upsertAirtable([...changedMember, ...changedCredit]);
    console.log(`Wrote ${written} records to Airtable.`);

    if (written > 0 && !isFirstRun && ENABLE_SLACK) {
      await sendSlackMessage(
        `Churn risk scoring: ${written} new/escalated customers flagged today ` +
          `(${changedMember.length} membership, ${changedCredit.length} credit). ` +
          `See "Churn Risk" table.`
      );
    } else if (written > 0) {
      console.log(
        `(Slack notification skipped — ${isFirstRun ? "first run" : "ENABLE_SLACK not set"}.)`
      );
    }
  } else {
    const memberTiers = memberRows.reduce((acc, r) => {
      const t = tierFor(Number(r.churn_prob), THRESHOLDS.membership);
      acc[t] = (acc[t] || 0) + 1;
      return acc;
    }, {});
    const creditTiers = creditRows.reduce((acc, r) => {
      const t = tierFor(Number(r.churn_prob), THRESHOLDS.credit);
      acc[t] = (acc[t] || 0) + 1;
      return acc;
    }, {});
    console.log("Membership tier counts:", memberTiers);
    console.log("Credit tier counts:", creditTiers);
    console.log("(dry run — no writes to BigQuery or Airtable)");
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
