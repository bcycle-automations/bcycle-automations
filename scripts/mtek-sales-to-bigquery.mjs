// scripts/mtek-sales-to-bigquery.mjs
// Daily sync: pulls the "Orders - UTC" MTEK report (id 353, slug
// orders-utc) and appends new order lines into the existing SalesMTEK
// table. Report's 35 headers map 1:1 (by position) onto SalesMTEK's 35
// columns — no corrupted/missing columns like FirstTimersMTEK.
//
// Each run starts from the latest transaction_date already in the table
// and walks forward in <=7-day chunks — fetching, deduping, and inserting
// one chunk at a time — until it catches up to yesterday (today's data is
// still incomplete when the job runs). A Slack message is posted to
// #bigquery-updates after every real insert.
//
// Dedup key: Order Number + Product ID + Variant ID + Transaction Date (no
// standalone line-item ID in the report; a repeat purchase of the same item
// in one order increments Line Quantity rather than adding a new line, but
// a refund creates a second line with the same order/product/variant and a
// negated quantity — Transaction Date is what actually distinguishes it
// from the original purchase).
//
// Same run also pushes Meta offline conversions: of the rows newly
// inserted this chunk, any with product_type in Credits/Memberships/Gift
// Cards and line_status Completed get sent as Purchase events to the
// "b.cycle Offline Conversions" dataset. Customer email/phone/name comes
// from a live MTEK /api/users/{id} lookup, not BigQuery — MTEK is the
// authoritative, current-format source (phone already includes country
// code there; the old Customers BigQuery table turned out to key off a
// completely different ID space and is stale).
//
// Defaults to a dry run — set DRY_RUN=false to actually write to BigQuery
// or send events to Meta.

import { createHash } from "node:crypto";
import { BigQuery } from "@google-cloud/bigquery";
import {
  fetchMtekReport,
  pastDaysWindow,
  clampSyncWindow,
  bqDateToString,
  addDaysUTC,
  formatDisplayDate,
} from "./lib/mtek-report.mjs";
import { toMDYYYY, toTime12h, boolToString, nullToEmpty } from "./lib/format.mjs";
import { sendSlackMessage } from "./lib/slack.mjs";
import { insertInBatches } from "./lib/bigquery.mjs";
import { fetchJsonWithRateLimit } from "./lib/mtek.mjs";

const MAX_WINDOW_DAYS = 7;
const SYNC_LABEL = "b.cycle - Sales";

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const REPORT_ID = process.env.MTEK_SALES_REPORT_ID || "353";
const REPORT_SLUG = process.env.MTEK_SALES_REPORT_SLUG || "orders-utc";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");
const SYNC_DAYS = Number(process.env.SYNC_DAYS || "7");

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SalesZF";
const BQ_TABLE = process.env.BQ_TABLE || "SalesMTEK";

const DRY_RUN = process.env.DRY_RUN !== "false";

const META_OFFLINE_CONVERSIONS_TOKEN = (process.env.META_OFFLINE_CONVERSIONS_TOKEN || "").trim();
const META_OFFLINE_DATASET_ID = process.env.META_OFFLINE_DATASET_ID || "1979154219698960";
const META_GRAPH_VERSION = "v19.0";
const META_EVENT_BATCH_SIZE = 500;

// Oli's spec: "class packs, memberships, and gift cards" — Credits covers
// class packs/passes in MTEK's naming.
const META_TARGET_PRODUCT_TYPES = new Set([
  "Credits",
  "Memberships",
  "Email Credit Gift Cards",
  "Email Gift Cards",
  "Physical Credit Gift Cards",
  "Physical Gift Cards",
]);

const EXPECTED_HEADERS = [
  "Order Number",
  "Order Status",
  "Order Date (UTC)",
  "Order Time (UTC)",
  "Order Device",
  "Processed by System?",
  "Customer ID",
  "Customer Email",
  "Customer Name",
  "Company Name",
  "Broker ID",
  "Broker Email",
  "Broker Name",
  "Product",
  "Product ID",
  "Variant ID",
  "Product Barcode",
  "Product SKU",
  "Product Color",
  "Product Size",
  "Product Type",
  "Vendor",
  "Sub Category",
  "Line Status",
  "Transaction Date",
  "Currency",
  "Line Quantity",
  "Line Subtotal",
  "Original Price",
  "Line Tax",
  "Line Total",
  "Order Payment Method(s)",
  "Purchase Location",
  "Fulfillment Location",
  "Fulfillment Region",
];

// Positional map: report column index -> [bqColumnName, transformFn]
const identity = nullToEmpty;
const COLUMN_MAP = [
  ["order_number", identity],
  ["order_status", identity],
  ["order_date_utc", toMDYYYY],
  ["order_time_utc", toTime12h],
  ["order_device", identity],
  ["processed_by_system", boolToString],
  ["customer_id", identity],
  ["customer_email", identity],
  ["customer_name", identity],
  ["company_name", identity],
  ["broker_id", identity],
  ["broker_email", identity],
  ["broker_name", identity],
  ["product", identity],
  ["product_id", identity],
  ["variant_id", identity],
  ["product_barcode", identity],
  ["product_sku", identity],
  ["product_color", identity],
  ["product_size", identity],
  ["product_type", identity],
  ["vendor", identity],
  ["sub_category", identity],
  ["line_status", identity],
  ["transaction_date", toMDYYYY],
  ["currency", identity],
  ["line_quantity", identity],
  ["line_subtotal", identity],
  ["original_price", identity],
  ["line_tax", identity],
  ["line_total", identity],
  ["order_payment_methods", identity],
  ["purchase_location", identity],
  ["fulfillment_location", identity],
  ["fulfillment_region", identity],
];

function mapRow(row) {
  const out = {};
  COLUMN_MAP.forEach(([bqCol, transform], i) => {
    out[bqCol] = transform(row[i]);
  });
  return out;
}

// transaction_date, line_status, and line_quantity are all part of the key
// because refunds create a second line with the same order_number/
// product_id/variant_id — sometimes on a later transaction_date, sometimes
// the exact same day as the original purchase — distinguished only by
// line_status ("Completed" vs "Refunded") and the sign of line_quantity.
// Without all of these, a refund can look like "already present" (matching
// its own original purchase line) and get silently dropped.
function dedupKey(row) {
  return `${row.order_number}|${row.product_id}|${row.variant_id}|${row.transaction_date}|${row.line_status}|${row.line_quantity}`;
}

function sha256(value) {
  return createHash("sha256").update(String(value).trim().toLowerCase()).digest("hex");
}

// Raw report row's "Order Date (UTC)" + "Order Time (UTC)" are already UTC
// — combine and parse directly, no timezone math needed.
function eventTimeFromRawRow(rawRow, headerIndex) {
  const orderDateUtc = rawRow[headerIndex["Order Date (UTC)"]];
  const orderTimeUtc = rawRow[headerIndex["Order Time (UTC)"]] || "00:00:00";
  const ms = Date.parse(`${orderDateUtc}T${orderTimeUtc}Z`);
  if (Number.isNaN(ms)) throw new Error(`Could not parse event time from "${orderDateUtc} ${orderTimeUtc}"`);
  return Math.floor(ms / 1000);
}

const mtekUserCache = new Map();

// Live MTEK lookup — authoritative and current, unlike the stale BigQuery
// Customers table (which turned out to key off a different ID space
// entirely). Confirmed shape: GET /api/users/{id} -> { data: { attributes:
// { email, first_name, last_name, phone_number } } }, phone already
// includes country code.
async function fetchMtekUser(customerId) {
  if (mtekUserCache.has(customerId)) return mtekUserCache.get(customerId);

  const url = new URL(`/api/users/${customerId}`, MTEK_BASE_URL);
  const json = await fetchJsonWithRateLimit(url, {
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: "application/vnd.api+json",
    },
  });
  const attrs = json?.data?.attributes || null;
  mtekUserCache.set(customerId, attrs);
  return attrs;
}

async function buildMetaEvent(rawRow, headerIndex) {
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

  return {
    event_name: "Purchase",
    event_time: eventTimeFromRawRow(rawRow, headerIndex),
    action_source: "physical_store",
    user_data: userData,
    custom_data: {
      // Exclusive of tax, per Oli — Line Subtotal, not Line Total.
      value: Number(get("Line Subtotal")),
      currency: (get("Currency") || "").trim(),
    },
  };
}

async function sendMetaEvents(events) {
  const url = `https://graph.facebook.com/${META_GRAPH_VERSION}/${META_OFFLINE_DATASET_ID}/events`;
  let totalReceived = 0;

  for (let i = 0; i < events.length; i += META_EVENT_BATCH_SIZE) {
    const batch = events.slice(i, i + META_EVENT_BATCH_SIZE);
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: batch, access_token: META_OFFLINE_CONVERSIONS_TOKEN }),
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
    query: `SELECT MAX(SAFE.PARSE_DATE('%m/%d/%Y', transaction_date)) AS last_date
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
            WHERE order_status != 'Order Status'`,
  });
  return bqDateToString(lastDateRow?.last_date) || pastDaysWindow(SYNC_DAYS).minDate;
}

async function getExistingKeys(bq, minDate, maxDate) {
  // Safety-net dedup window: a few days wider than the fetch window in case
  // of a rerun landing on the same dates.
  const bufferedMinDate = addDaysUTC(minDate, -3);
  const [existingRows] = await bq.query({
    query: `SELECT DISTINCT order_number, product_id, variant_id, transaction_date, line_status, line_quantity
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
            WHERE SAFE.PARSE_DATE('%m/%d/%Y', transaction_date)
                  BETWEEN @minDate AND @maxDate`,
    params: { minDate: bufferedMinDate, maxDate },
  });
  return new Set(
    existingRows.map(
      (r) => `${r.order_number}|${r.product_id}|${r.variant_id}|${r.transaction_date}|${r.line_status}|${r.line_quantity}`
    )
  );
}

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_API_TOKEN");
  if (!META_OFFLINE_CONVERSIONS_TOKEN) throw new Error("Missing META_OFFLINE_CONVERSIONS_TOKEN");

  const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
  const table = bq.dataset(BQ_DATASET).table(BQ_TABLE);

  const headerIndex = {};
  EXPECTED_HEADERS.forEach((h, i) => (headerIndex[h] = i));

  const manualOverride = process.env.SYNC_MIN_DATE && process.env.SYNC_MAX_DATE;
  let sinceDate = manualOverride ? process.env.SYNC_MIN_DATE : await getInitialSinceDate(bq);

  // Extra in-run dedup guard so back-to-back chunks in the same execution
  // never double-insert (real inserts land in BQ immediately, but this
  // keeps dry-run previews accurate across chunks too).
  const seenThisRun = new Set();

  let totalInserted = 0;
  let totalMetaSent = 0;
  let chunkCount = 0;

  while (true) {
    const window = manualOverride
      ? { minDate: sinceDate, maxDate: process.env.SYNC_MAX_DATE }
      : clampSyncWindow(sinceDate, MAX_WINDOW_DAYS);

    if (!window) {
      console.log(`Caught up (last transaction_date: ${sinceDate}). Nothing more to sync.`);
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
      dateParams: {
        min_transaction_date: minDate,
        max_transaction_date: maxDate,
      },
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
    } else if (newRows.length > 0) {
      await insertInBatches(table, newRows);
      console.log(`Inserted ${newRows.length} rows into ${BQ_TABLE}`);
      await sendSlackMessage(
        `b.cycle - Sales added from ${formatDisplayDate(minDate)} to ${formatDisplayDate(maxDate)}: ${newRows.length} rows`
      );
    } else {
      console.log("Nothing new to insert for this chunk.");
    }

    // Meta offline conversions: subset of this chunk's new rows that Oli's
    // ads care about, only ever drawn from rows we just confirmed are new
    // (never resends rows already in BigQuery from a prior run).
    const metaQualifying = newIndexed.filter(
      ({ row }) => META_TARGET_PRODUCT_TYPES.has(row.product_type) && row.line_status === "Completed"
    );
    console.log(
      `${metaQualifying.length} of ${newRows.length} new row(s) qualify for Meta (target product types, Completed).`
    );

    if (metaQualifying.length > 0) {
      if (DRY_RUN) {
        const sampleEvent = await buildMetaEvent(metaQualifying[0].raw, headerIndex);
        console.log("DRY RUN — nothing sent to Meta. Sample event:", JSON.stringify(sampleEvent, null, 2));
      } else {
        const events = [];
        for (const { raw } of metaQualifying) {
          events.push(await buildMetaEvent(raw, headerIndex));
        }
        const totalReceived = await sendMetaEvents(events);
        console.log(`Sent ${events.length} event(s) to Meta, acknowledged ${totalReceived}.`);
        totalMetaSent += events.length;
      }
    }

    for (const r of newRows) seenThisRun.add(dedupKey(r));
    totalInserted += newRows.length;

    if (manualOverride) break;
    sinceDate = addDaysUTC(maxDate, 1);
  }

  console.log(
    `\nDone. ${chunkCount} chunk(s), ${totalInserted} total row(s) ${DRY_RUN ? "would be " : ""}inserted, ` +
      `${totalMetaSent} event(s) ${DRY_RUN ? "would be " : ""}sent to Meta.`
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
