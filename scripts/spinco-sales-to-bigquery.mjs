// scripts/spinco-sales-to-bigquery.mjs
// Daily sync: pulls SPINCO's "Orders - UTC" MTEK report (id 383, slug
// orders-utc) and appends new order lines into the existing SPINCO.Sales
// table. Unlike b.cycle's SalesMTEK, this table already has real typed
// columns (DATE/BOOLEAN/INTEGER/FLOAT), so values pass through with minimal
// transformation — dates truncated, Product Barcode parsed to a number.
//
// Each run starts from the latest Transaction Date already in the table and
// walks forward in <=7-day chunks until it catches up to yesterday. A Slack
// message is posted to #bigquery-updates after every real insert.
//
// Dedup key: Order Number + Product ID + Variant ID + Transaction Date +
// Line Status + Line Quantity — same composite key validated for b.cycle's
// Sales, since refunds share Order/Product/Variant with their original
// purchase line, sometimes even same-day (confirmed: order/product/variant
// alone is NOT unique here either — ~132 pre-existing collisions in the
// baseline data even under the full composite key, a pre-existing data
// quality quirk, not something this script introduces).
//
// Same run also pushes Meta offline conversions (mirrors b.cycle's
// mtek-sales-to-bigquery.mjs): of the rows newly inserted this chunk, any
// with Product Type in Credits/Memberships/Gift Cards and Line Status
// Completed get sent as Purchase events to the "SPINCO Offline Data" dataset.
// Customer email/phone/name comes from a live MTEK /api/users/{id} lookup.
// Must run daily — Meta rejects events older than 7 days.
//
// Defaults to a dry run — set DRY_RUN=false to actually write to BigQuery
// or send events to Meta.

import { BigQuery } from "@google-cloud/bigquery";
import { createHash } from "node:crypto";
import {
  fetchMtekReport,
  pastDaysWindow,
  clampSyncWindow,
  bqDateToString,
  addDaysUTC,
  formatDisplayDate,
} from "./lib/mtek-report.mjs";
import { sendSlackMessage } from "./lib/slack.mjs";
import { insertInBatches } from "./lib/bigquery.mjs";
import { fetchJsonWithRateLimit } from "./lib/mtek.mjs";
import { TABLES } from "./lib/spinco-mtek-tables.mjs";

const SALES = TABLES.sales;

const MAX_WINDOW_DAYS = 2;
const SYNC_LABEL = "SPINCO - Sales";

const MTEK_BASE_URL = (process.env.MTEK_SPINCO_BASE_URL || "https://spinco.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_SPINCO_API_TOKEN || "").trim();
const REPORT_ID = SALES.reportId;
const REPORT_SLUG = SALES.reportSlug;
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");
const SYNC_DAYS = Number(process.env.SYNC_DAYS || "7");

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SPINCO";
const BQ_TABLE = SALES.bqTable;

const DRY_RUN = process.env.DRY_RUN !== "false";

const META_OFFLINE_CONVERSIONS_TOKEN = (process.env.META_OFFLINE_CONVERSIONS_TOKEN_SPINCO || "").trim();
const META_OFFLINE_DATASET_ID = process.env.META_OFFLINE_DATASET_ID_SPINCO || "1119305444099374";
const META_TEST_EVENT_CODE = (process.env.META_TEST_EVENT_CODE || "").trim();
// Test mode (test code set on a real run): send a small sample of events to
// Meta's Test events tab ONLY — no BigQuery inserts (otherwise the next live
// run would see those rows as already synced and never send them to Meta),
// no Slack, single chunk.
const TEST_MODE = Boolean(META_TEST_EVENT_CODE) && !DRY_RUN;
const TEST_MODE_MAX_EVENTS = 20;
const META_GRAPH_VERSION = "v19.0";
const META_EVENT_BATCH_SIZE = 500;

const META_TARGET_PRODUCT_TYPES = new Set([
  "Credits",
  "Memberships",
  "Email Credit Gift Cards",
  "Email Gift Cards",
  "Physical Credit Gift Cards",
  "Physical Gift Cards",
]);

// Headroom under Meta's hard 7-day event-time window (subcode 2804003).
const BACKFILL_MIN_DATE = (process.env.META_BACKFILL_MIN_DATE || "").trim();
const BACKFILL_MAX_DATE = (process.env.META_BACKFILL_MAX_DATE || "").trim();
const BACKFILL = Boolean(BACKFILL_MIN_DATE && BACKFILL_MAX_DATE);
// Backfill sends are one-off and retryable, so use a tighter-to-the-limit
// cutoff (Meta's hard limit is 7 days) to recover as many events as possible.
const META_MAX_EVENT_AGE_DAYS = BACKFILL ? 6.8 : 6.5;
const META_MAX_EVENT_AGE_SECONDS = META_MAX_EVENT_AGE_DAYS * 24 * 60 * 60;

// Report headers, column mapping, and dedup key now live in
// ./lib/spinco-mtek-tables.mjs (shared with the monthly data audit script).
const EXPECTED_HEADERS = SALES.expectedHeaders;
const mapRow = SALES.mapRow;
const dedupKey = SALES.dedupKey;

function sha256(value) {
  return createHash("sha256").update(String(value).trim().toLowerCase()).digest("hex");
}

// Raw "Order Date (UTC)" + "Order Time (UTC)" are already UTC.
function orderTimestampSeconds(rawRow, headerIndex) {
  const orderDateUtc = rawRow[headerIndex["Order Date (UTC)"]];
  if (!orderDateUtc) return null;
  const orderTimeUtc = rawRow[headerIndex["Order Time (UTC)"]] || "00:00:00";
  const ms = Date.parse(`${String(orderDateUtc).slice(0, 10)}T${orderTimeUtc}Z`);
  return Number.isNaN(ms) ? null : Math.floor(ms / 1000);
}

// event_time is derived from Transaction Date (YYYY-MM-DD here, typed DATE
// column), not Order Date — recurring membership charges carry the ORIGINAL
// order date, often months old. Same-UTC-day orders use the precise order
// timestamp instead of a flattened noon-UTC one.
function eventTimeFromRow(rawRow, row, headerIndex) {
  const orderSeconds = orderTimestampSeconds(rawRow, headerIndex);
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(row["Transaction Date"] || ""));

  if (m) {
    const [year, month, day] = [Number(m[1]), Number(m[2]), Number(m[3])];
    if (orderSeconds !== null) {
      const d = new Date(orderSeconds * 1000);
      if (d.getUTCFullYear() === year && d.getUTCMonth() === month - 1 && d.getUTCDate() === day) {
        return orderSeconds;
      }
    }
    return Math.floor(Date.UTC(year, month - 1, day, 12, 0, 0) / 1000);
  }
  if (orderSeconds !== null) return orderSeconds;
  throw new Error(`Could not parse event time from Transaction Date "${row["Transaction Date"]}" or Order Date/Time`);
}

function hasMetaContactInfo(userData) {
  return Boolean(userData.em || userData.ph);
}

function isEventTooOldForMeta(eventTimeSeconds, nowSeconds = Math.floor(Date.now() / 1000)) {
  return nowSeconds - eventTimeSeconds > META_MAX_EVENT_AGE_SECONDS;
}

const mtekUserCache = new Map();

// Live MTEK lookup: GET /api/users/{id} -> { data: { attributes: { email,
// first_name, last_name, phone_number } } }; phone already includes country code.
async function fetchMtekUser(customerId) {
  if (mtekUserCache.has(customerId)) return mtekUserCache.get(customerId);
  const url = new URL(`/api/users/${customerId}`, MTEK_BASE_URL);
  const json = await fetchJsonWithRateLimit(url, {
    headers: { Authorization: `Bearer ${MTEK_API_TOKEN}`, Accept: "application/vnd.api+json" },
  });
  const attrs = json?.data?.attributes || null;
  mtekUserCache.set(customerId, attrs);
  return attrs;
}

async function buildMetaEvent(rawRow, row, headerIndex) {
  const get = (name) => rawRow[headerIndex[name]];
  const customerId = get("Customer ID");

  let user = null;
  try {
    user = await fetchMtekUser(customerId);
  } catch (err) {
    console.warn(`  MTEK user lookup failed for customer ${customerId}: ${err.message}`);
  }

  const email = (user?.email || get("Customer Email") || "").trim();
  const firstName = (user?.first_name || "").trim();
  const lastName = (user?.last_name || "").trim();
  const phone = (user?.phone_number || "").replace(/\D/g, "");

  const userData = {};
  if (email) userData.em = [sha256(email)];
  if (phone) userData.ph = [sha256(phone)];
  if (firstName) userData.fn = [sha256(firstName)];
  if (lastName) userData.ln = [sha256(lastName)];
  if (customerId) userData.external_id = [sha256(customerId)];

  return {
    event_name: "Purchase",
    event_time: eventTimeFromRow(rawRow, row, headerIndex),
    action_source: "physical_store",
    user_data: userData,
    custom_data: {
      // Tax-exclusive, same as b.cycle — Line Subtotal, not Line Total.
      value: Number(row["Line Subtotal"]),
      currency: (row["Currency"] || "").trim(),
    },
  };
}

async function sendMetaEvents(events) {
  const url = `https://graph.facebook.com/${META_GRAPH_VERSION}/${META_OFFLINE_DATASET_ID}/events`;
  let totalReceived = 0;

  for (let i = 0; i < events.length; i += META_EVENT_BATCH_SIZE) {
    const batch = events.slice(i, i + META_EVENT_BATCH_SIZE);
    const body = { data: batch, access_token: META_OFFLINE_CONVERSIONS_TOKEN };
    if (META_TEST_EVENT_CODE) body.test_event_code = META_TEST_EVENT_CODE;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`Meta API error for batch starting at ${i}: ${JSON.stringify(json)}`);
    }
    totalReceived += json.events_received || 0;
  }
  return totalReceived;
}

async function getInitialSinceDate(bq) {
  const [[lastDateRow]] = await bq.query({
    query: `SELECT MAX(\`Transaction Date\`) AS last_date
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\``,
  });
  return bqDateToString(lastDateRow?.last_date) || pastDaysWindow(SYNC_DAYS).minDate;
}

async function getExistingKeys(bq, minDate, maxDate) {
  const bufferedMinDate = addDaysUTC(minDate, -3);
  const [existingRows] = await bq.query({
    query: `SELECT DISTINCT \`Order Number\` AS order_number, \`Product ID\` AS product_id,
                   \`Variant ID\` AS variant_id, \`Transaction Date\` AS transaction_date,
                   \`Line Status\` AS line_status, \`Line Quantity\` AS line_quantity
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
            WHERE \`Transaction Date\` BETWEEN @minDate AND @maxDate`,
    params: { minDate: bufferedMinDate, maxDate },
  });
  return new Set(
    existingRows.map(
      (r) =>
        `${r.order_number}|${r.product_id}|${r.variant_id}|${bqDateToString(r.transaction_date)}|${r.line_status}|${r.line_quantity}`
    )
  );
}

// Meta-ONLY backfill: sends qualifying rows for an explicit date window that
// are already in BigQuery but were never sent to Meta. No BigQuery insert, no
// dedup against BigQuery — the caller must have verified the window has not
// already been sent (a resend would double-count purchases, since no event_id
// is set). Meta's 7-day event-time limit still applies.
async function metaBackfill() {
  const headerIndex = {};
  EXPECTED_HEADERS.forEach((h, i) => (headerIndex[h] = i));
  console.log(`META BACKFILL ${BACKFILL_MIN_DATE}..${BACKFILL_MAX_DATE}${DRY_RUN ? " (DRY RUN)" : ""}`);

  let sent = 0, zero = 0, noContact = 0, tooOld = 0, qualifyingTotal = 0;
  let start = BACKFILL_MIN_DATE;
  while (start <= BACKFILL_MAX_DATE) {
    const end = [addDaysUTC(start, MAX_WINDOW_DAYS - 1), BACKFILL_MAX_DATE].sort()[0];
    console.log(`\n--- Backfill chunk ${start}..${end} ---`);
    const report = await fetchMtekReport({
      baseUrl: MTEK_BASE_URL,
      token: MTEK_API_TOKEN,
      reportId: REPORT_ID,
      slug: REPORT_SLUG,
      pageSize: PAGE_SIZE,
      dateParams: SALES.dateParams(start, end),
    });
    if (JSON.stringify(report.headers) !== JSON.stringify(EXPECTED_HEADERS)) {
      throw new Error(`Report headers changed shape — refusing to guess. Got: ${JSON.stringify(report.headers)}`);
    }
    const qualifying = report.rows
      .map((raw) => ({ raw, row: mapRow(raw) }))
      .filter(({ row }) => META_TARGET_PRODUCT_TYPES.has(row["Product Type"]) && row["Line Status"] === "Completed");
    qualifyingTotal += qualifying.length;
    console.log(`Fetched ${report.rows.length} rows, ${qualifying.length} qualify by product type/status.`);

    const events = [];
    const nowSeconds = Math.floor(Date.now() / 1000);
    for (const { raw, row } of qualifying) {
      const event = await buildMetaEvent(raw, row, headerIndex);
      if (!(event.custom_data.value > 0)) { zero++; continue; }
      if (!hasMetaContactInfo(event.user_data)) { noContact++; continue; }
      if (isEventTooOldForMeta(event.event_time, nowSeconds)) { tooOld++; continue; }
      events.push(event);
    }
    console.log(`${events.length} event(s) ready for chunk ${start}..${end}.`);
    if (events.length > 0) {
      if (DRY_RUN) {
        console.log("DRY RUN — would send. Sample:", JSON.stringify(events[0], null, 2));
      } else {
        const received = await sendMetaEvents(events);
        console.log(`Sent ${events.length} event(s) to Meta, acknowledged ${received}.`);
        sent += events.length;
      }
    }
    start = addDaysUTC(end, 1);
  }
  console.log(
    `\nBackfill done. ${qualifyingTotal} qualifying, ${sent} sent${DRY_RUN ? " (dry run: none)" : ""}, ` +
      `${zero} skipped ($0), ${noContact} skipped (no contact), ${tooOld} skipped (too old).`
  );
}

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_SPINCO_API_TOKEN");
  if (!DRY_RUN && !META_OFFLINE_CONVERSIONS_TOKEN) {
    throw new Error("Missing META_OFFLINE_CONVERSIONS_TOKEN_SPINCO");
  }

  if (BACKFILL) {
    await metaBackfill();
    return;
  }

  const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
  const table = bq.dataset(BQ_DATASET).table(BQ_TABLE);

  const headerIndex = {};
  EXPECTED_HEADERS.forEach((h, i) => (headerIndex[h] = i));

  const manualOverride = process.env.SYNC_MIN_DATE && process.env.SYNC_MAX_DATE;
  let sinceDate = manualOverride ? process.env.SYNC_MIN_DATE : await getInitialSinceDate(bq);

  const seenThisRun = new Set();

  let totalInserted = 0;
  let totalMetaSent = 0;
  let totalMetaSkippedNoContact = 0;
  let totalMetaSkippedTooOld = 0;
  let totalMetaSkippedZeroValue = 0;
  let chunkCount = 0;
  const metaFailedChunks = [];

  while (true) {
    const window = manualOverride
      ? { minDate: sinceDate, maxDate: process.env.SYNC_MAX_DATE }
      : clampSyncWindow(sinceDate, MAX_WINDOW_DAYS);

    if (!window) {
      console.log(`Caught up (last Transaction Date: ${sinceDate}). Nothing more to sync.`);
      break;
    }
    const { minDate, maxDate } = window;
    chunkCount++;
    console.log(`\n--- Chunk ${chunkCount}: ${minDate}..${maxDate} ---`);

    const report = await fetchMtekReport({
      baseUrl: MTEK_BASE_URL,
      token: MTEK_API_TOKEN,
      reportId: REPORT_ID,
      slug: REPORT_SLUG,
      pageSize: PAGE_SIZE,
      dateParams: SALES.dateParams(minDate, maxDate),
    });

    const headersMatch = JSON.stringify(report.headers) === JSON.stringify(EXPECTED_HEADERS);
    if (!headersMatch) {
      throw new Error(
        `Report headers changed shape — refusing to guess. Got: ${JSON.stringify(report.headers)}`
      );
    }
    console.log(`Fetched ${report.rows.length} rows from MTEK`);

    const existingKeys = await getExistingKeys(bq, minDate, maxDate);
    const indexed = report.rows.map((raw) => ({ raw, row: mapRow(raw) }));
    const newIndexed = indexed.filter(({ row }) => {
      const key = dedupKey(row);
      return !existingKeys.has(key) && !seenThisRun.has(key);
    });
    const newRows = newIndexed.map(({ row }) => row);
    const skipped = indexed.length - newIndexed.length;
    console.log(`${newRows.length} new rows, ${skipped} skipped as already present`);

    if (DRY_RUN) {
      console.log("DRY RUN — nothing written. Sample:", JSON.stringify(newRows.slice(0, 2), null, 2));
    } else if (TEST_MODE) {
      console.log("TEST MODE — skipping BigQuery insert and Slack.");
    } else if (newRows.length > 0) {
      await insertInBatches(table, newRows);
      console.log(`Inserted ${newRows.length} rows into ${BQ_TABLE}`);
      await sendSlackMessage(
        `SPINCO - Sales added from ${formatDisplayDate(minDate)} to ${formatDisplayDate(maxDate)}: ${newRows.length} rows`
      );
    } else {
      console.log("Nothing new to insert for this chunk.");
    }

    // Meta offline conversions: only ever drawn from rows just confirmed new
    // (never resends rows already in BigQuery from a prior run).
    let metaQualifying = newIndexed.filter(
      ({ row }) => META_TARGET_PRODUCT_TYPES.has(row["Product Type"]) && row["Line Status"] === "Completed"
    );
    // Test mode: avoid hundreds of MTEK user lookups for a 20-event sample.
    if (TEST_MODE) metaQualifying = metaQualifying.slice(0, TEST_MODE_MAX_EVENTS * 3);
    console.log(
      `${metaQualifying.length} of ${newRows.length} new row(s) qualify for Meta (target product types, Completed).`
    );

    // Filter out anything Meta would reject the WHOLE BATCH for (subcodes
    // 2804050 and 2804003) rather than let one bad row sink the chunk.
    const chunkEvents = [];
    const nowSeconds = Math.floor(Date.now() / 1000);
    for (const { raw, row } of metaQualifying) {
      const event = await buildMetaEvent(raw, row, headerIndex);
      const orderNumber = row["Order Number"];
      const customerId = row["Customer ID"];

      // ~20% of qualifying rows are genuine $0 lines (comps, promos, staff
      // passes) — not real purchases, and they'd dilute ad optimization.
      if (!(event.custom_data.value > 0)) {
        totalMetaSkippedZeroValue++;
        continue;
      }
      if (!hasMetaContactInfo(event.user_data)) {
        totalMetaSkippedNoContact++;
        console.log(`  Skipping Meta event (no em/ph): order ${orderNumber}, customer ${customerId}`);
        continue;
      }
      if (isEventTooOldForMeta(event.event_time, nowSeconds)) {
        totalMetaSkippedTooOld++;
        console.log(
          `  Skipping Meta event (older than ${META_MAX_EVENT_AGE_DAYS} days): order ${orderNumber}, customer ${customerId}`
        );
        continue;
      }
      chunkEvents.push(event);
    }

    if (TEST_MODE) chunkEvents.splice(TEST_MODE_MAX_EVENTS);

    if (chunkEvents.length > 0) {
      if (DRY_RUN) {
        console.log(
          `DRY RUN — would send ${chunkEvents.length} event(s) to Meta. Sample:`,
          JSON.stringify(chunkEvents[0], null, 2)
        );
      } else {
        // A Meta failure must never take down the BigQuery sync for later chunks.
        try {
          const totalReceived = await sendMetaEvents(chunkEvents);
          console.log(`Sent ${chunkEvents.length} event(s) to Meta, acknowledged ${totalReceived}.`);
          totalMetaSent += chunkEvents.length;
        } catch (err) {
          console.error(`Meta send failed for chunk ${chunkCount} (${minDate}..${maxDate}): ${err.message}`);
          metaFailedChunks.push({ chunk: chunkCount, minDate, maxDate, count: chunkEvents.length, error: err.message });
        }
      }
    }

    for (const r of newRows) seenThisRun.add(dedupKey(r));
    totalInserted += newRows.length;

    if (manualOverride || TEST_MODE) break;
    sinceDate = addDaysUTC(maxDate, 1);
  }

  if (metaFailedChunks.length > 0) {
    const summaryLines = metaFailedChunks
      .map((f) => `  chunk ${f.chunk} (${f.minDate}..${f.maxDate}): ${f.count} event(s) — ${f.error}`)
      .join("\n");
    console.error(`Meta Offline Conversions failed for ${metaFailedChunks.length} chunk(s):\n${summaryLines}`);
    if (!DRY_RUN) {
      try {
        await sendSlackMessage(
          `⚠️ ${SYNC_LABEL} sync: Meta Offline Conversions failed for ${metaFailedChunks.length} chunk(s) ` +
            `(BigQuery inserts were not affected):\n${summaryLines}`
        );
      } catch (slackErr) {
        console.error("Additionally failed to post Meta failure summary to Slack:", slackErr.message);
      }
    }
    process.exitCode = 1;
  }

  console.log(
    `\nDone. ${chunkCount} chunk(s), ${totalInserted} total row(s) ${DRY_RUN ? "would be " : ""}inserted, ` +
      `${totalMetaSent} event(s) ${DRY_RUN ? "would be " : ""}sent to Meta, ` +
      `${totalMetaSkippedZeroValue} skipped ($0 value), ${totalMetaSkippedNoContact} skipped (no contact info), ${totalMetaSkippedTooOld} skipped (too old).`
  );
}

main().catch(async (err) => {
  console.error("Sync failed:", err.message);
  if (!DRY_RUN) {
    try {
      await sendSlackMessage(`⚠️ ${SYNC_LABEL} sync failed: ${err.message}`);
    } catch (slackErr) {
      console.error("Additionally failed to post failure notice to Slack:", slackErr.message);
    }
  }
  process.exit(1);
});
