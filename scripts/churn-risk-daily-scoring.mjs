// Daily churn-risk scoring for b.cycle.
//
// Scores every current membership and credit-pack customer against the two
// trained BigQuery ML models (SalesZF.churn_model_membership,
// SalesZF.churn_model_credit), writes the full result set into a persistent
// SalesZF.churn_risk_scores table (via MERGE, so we always know each
// person's PREVIOUS tier), and only pushes a record into Airtable's
// "Churn Risk" table for people who are NEW to a risk tier or ESCALATING —
// not the whole list every day, to avoid spamming managers with unchanged
// names.
//
// "Membership" vs "credit" population is determined by each customer's most
// recent purchase as of today, not lifetime history — someone who was a
// member years ago and now only buys credit packs is scored as a credit
// customer, not a membership one. See conversation history / docs for why.
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
// setting ENABLE_SLACK=true once you're ready for daily pings.
const ENABLE_SLACK = String(process.env.ENABLE_SLACK).toLowerCase() === "true";

// Calibrated against the backtest in the model-development conversation —
// membership crosses ~80% recall / 82% precision around 0.55; credit around
// 0.53. "Watch" is a softer, earlier-warning threshold.
const THRESHOLDS = {
  membership: { critical: 0.55, watch: 0.35 },
  credit: { critical: 0.53, watch: 0.35 },
};

const SHARED_CTES = `
WITH sales_e1 AS (
  SELECT s.DATE AS d, LOWER(TRIM(c.emailaddress)) AS email, s.ITEM, s.TYPE,
    IFNULL(s.REVENUE,0) AS rev, c.first_name, c.last_name
  FROM \`${BQ_PROJECT_ID}.SalesZF.Sales_view\` s
  LEFT JOIN \`${BQ_PROJECT_ID}.SalesZF.Customers\` c
    ON CAST(s.\`CUSTOMER ID\` AS STRING) = CAST(c.id AS STRING)
  WHERE s.TYPE = 'series'
),
e1_purch AS (
  SELECT email, d, rev, first_name, last_name,
    CASE WHEN REGEXP_CONTAINS(LOWER(ITEM), r'unlimit|illimit|member|bike.*(commit|month|autorenew)')
         THEN 'membership' ELSE 'credit' END AS category,
    SAFE_CAST(REGEXP_EXTRACT(ITEM, r'^(\\d+)') AS INT64) AS pack_size
  FROM sales_e1 WHERE email IS NOT NULL AND email != ''
),
e2_purch AS (
  SELECT LOWER(TRIM(customer_email)) AS email,
    SAFE.PARSE_DATE('%m/%d/%Y', transaction_date) AS d,
    IFNULL(SAFE_CAST(line_total AS FLOAT64),0) AS rev,
    customer_name AS first_name, CAST(NULL AS STRING) AS last_name,
    CASE WHEN product_type='Memberships' THEN 'membership'
         WHEN product_type='Credits' THEN 'credit' ELSE NULL END AS category,
    SAFE_CAST(line_quantity AS INT64) AS pack_size,
    customer_id AS mtek_customer_id
  FROM \`${BQ_PROJECT_ID}.SalesZF.SalesMTEK\`
  WHERE customer_email IS NOT NULL AND customer_email != ''
    AND product_type IN ('Memberships','Credits')
),
purchases AS (
  SELECT email, d, rev, category, IFNULL(pack_size,1) AS pack_size,
    first_name, last_name, CAST(NULL AS STRING) AS mtek_customer_id
  FROM e1_purch WHERE d IS NOT NULL
  UNION ALL
  SELECT email, d, rev, category, IFNULL(pack_size,1) AS pack_size,
    first_name, last_name, mtek_customer_id
  FROM e2_purch WHERE category IS NOT NULL AND d IS NOT NULL
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
candidates AS (
  SELECT lv.email, lv.last_visit_date,
    CASE WHEN ARRAY_LENGTH(lv.last_two) < 2 THEN 999
         ELSE DATE_DIFF(lv.last_two[OFFSET(0)], lv.last_two[OFFSET(1)], DAY) END AS gap_days,
    vc.visit_count, lp.latest.category AS category, lp.latest.d AS last_purchase_date,
    lp.latest.pack_size AS last_pack_size, lp.latest.first_name AS first_name,
    lp.latest.last_name AS last_name, lp.latest.mtek_customer_id AS mtek_customer_id
  FROM last_visit lv
  JOIN visit_counts vc USING (email)
  JOIN latest_purchase lp USING (email)
  -- Scope to people still plausibly reachable: recent enough that "at risk"
  -- is a meaningful warning, not someone who fully lapsed long ago (already
  -- churned, not actionable) or someone brand new who just visited (not at
  -- risk yet). This window matters most for credit customers, where most of
  -- the ever-purchased population is already permanently gone.
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
member_spend AS (
  SELECT mp.email,
    SUM(IF(s.d BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND CURRENT_DATE(), s.rev, 0)) AS spend_trailing60,
    SUM(IF(s.d BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 61 DAY), s.rev, 0)) AS spend_prior60
  FROM member_pop mp LEFT JOIN purchases s ON s.email = mp.email
  GROUP BY mp.email
),
member_features AS (
  SELECT mp.email, mp.last_visit_date, mp.gap_days, mp.visit_count,
    mp.first_name, mp.last_name, mp.mtek_customer_id,
    mf.visits_trailing60, mf.visits_prior60, ms.spend_trailing60, ms.spend_prior60,
    SAFE_DIVIDE(mf.visits_trailing60, NULLIF(mf.visits_prior60,0)) AS freq_ratio,
    SAFE_DIVIDE(ms.spend_trailing60, NULLIF(ms.spend_prior60,0)) AS spend_ratio,
    (mp.visit_count = 1) AS is_single_visit
  FROM member_pop mp
  JOIN member_freq mf USING (email)
  JOIN member_spend ms USING (email)
)
SELECT
  email, last_visit_date, first_name, last_name, mtek_customer_id,
  gap_days, visit_count,
  (SELECT prob FROM UNNEST(predicted_label_probs) WHERE label = 1) AS churn_prob,
  CASE
    WHEN gap_days >= 30 THEN CONCAT('No visits in ', CAST(gap_days AS STRING), ' days')
    WHEN IFNULL(freq_ratio, 1) < 0.6 THEN 'Visit frequency declining vs. prior period'
    WHEN is_single_visit THEN 'Only ever visited once'
    ELSE 'Attendance pattern'
  END AS key_signal
FROM ML.PREDICT(MODEL \`${BQ_PROJECT_ID}.SalesZF.churn_model_membership\`, (
  SELECT email, last_visit_date, first_name, last_name, mtek_customer_id,
    gap_days, visit_count, visits_trailing60, visits_prior60,
    IFNULL(freq_ratio,-1) AS freq_ratio, spend_trailing60, spend_prior60,
    IFNULL(spend_ratio,-1) AS spend_ratio, is_single_visit
  FROM member_features
))`;

const CREDIT_QUERY = `
${SHARED_CTES},
credit_pop AS (
  -- Further scoped beyond the shared 180-day visit window: credit
  -- customers only make sense to flag as "at risk, still actionable" if
  -- their most recent purchase is itself recent. Someone whose last pack
  -- purchase was 150 days ago and never returned isn't "at risk" in an
  -- actionable sense -- they're already gone, and belongs in a win-back
  -- flow, not this one. Without this, most of the credit population scores
  -- Critical by default, since most credit-pack buyers never repurchase.
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
    tier: tierFor(Number(r.churn_prob), thresholds),
    key_signal: r.key_signal,
  }));

  const dataset = bq.dataset("SalesZF");
  await dataset.query({
    query: `CREATE OR REPLACE TABLE \`${BQ_PROJECT_ID}.SalesZF.${tempTable}\` (
      email STRING, population STRING, first_name STRING, last_name STRING,
      mtek_customer_id STRING, last_visit_date DATE, churn_prob FLOAT64,
      tier STRING, key_signal STRING)`,
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
      key_signal = S.key_signal, last_visit_date = S.last_visit_date,
      first_name = S.first_name, last_name = S.last_name,
      mtek_customer_id = S.mtek_customer_id, updated_date = CURRENT_DATE()
    WHEN NOT MATCHED THEN INSERT (email, population, first_name, last_name,
      mtek_customer_id, last_visit_date, churn_prob, tier, previous_tier,
      key_signal, updated_date)
    VALUES (S.email, S.population, S.first_name, S.last_name, S.mtek_customer_id,
      S.last_visit_date, S.churn_prob, S.tier, NULL, S.key_signal, CURRENT_DATE())`;
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
