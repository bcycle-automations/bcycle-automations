// scripts/spinco-sales-to-bigquery.mjs
// Weekly sync: pulls SPINCO's "Orders - UTC" MTEK report (id 383, slug
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
// Defaults to a dry run — set DRY_RUN=false to actually write to BigQuery.

import { BigQuery } from "@google-cloud/bigquery";
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

const MAX_WINDOW_DAYS = 2;

const MTEK_BASE_URL = (process.env.MTEK_SPINCO_BASE_URL || "https://spinco.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_SPINCO_API_TOKEN || "").trim();
const REPORT_ID = process.env.MTEK_SPINCO_SALES_REPORT_ID || "383";
const REPORT_SLUG = process.env.MTEK_SPINCO_SALES_REPORT_SLUG || "orders-utc";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");
const SYNC_DAYS = Number(process.env.SYNC_DAYS || "7");

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SPINCO";
const BQ_TABLE = process.env.BQ_TABLE || "Sales";

const DRY_RUN = process.env.DRY_RUN !== "false";

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

const BQ_COLUMNS = [
  "Order Number",
  "Order Status",
  "Order Date _UTC_",
  "Order Time _UTC_",
  "Order Device",
  "Processed by System_",
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
  "Order Payment Method_s_",
  "Purchase Location",
  "Fulfillment Location",
  "Fulfillment Region",
];

const DATE_ONLY_COLUMNS = new Set(["Order Date _UTC_", "Transaction Date"]);
const FLOAT_COLUMNS = new Set(["Product Barcode", "Line Subtotal", "Original Price", "Line Tax", "Line Total"]);
const INT_COLUMNS = new Set(["Customer ID", "Broker ID", "Product ID", "Variant ID", "Line Quantity"]);

function passthrough(v) {
  return v === undefined ? null : v;
}

function toDateOnly(v) {
  if (v === null || v === undefined) return null;
  return String(v).slice(0, 10);
}

function toFloat(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

function toInt(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = parseInt(v, 10);
  return Number.isNaN(n) ? null : n;
}

function mapRow(row) {
  const out = {};
  BQ_COLUMNS.forEach((bqCol, i) => {
    const raw = row[i];
    if (DATE_ONLY_COLUMNS.has(bqCol)) {
      out[bqCol] = toDateOnly(raw);
    } else if (FLOAT_COLUMNS.has(bqCol)) {
      out[bqCol] = toFloat(raw);
    } else if (INT_COLUMNS.has(bqCol)) {
      out[bqCol] = toInt(raw);
    } else {
      out[bqCol] = passthrough(raw);
    }
  });
  return out;
}

function dedupKey(row) {
  return `${row["Order Number"]}|${row["Product ID"]}|${row["Variant ID"]}|${row["Transaction Date"]}|${row["Line Status"]}|${row["Line Quantity"]}`;
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

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_SPINCO_API_TOKEN");

  const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
  const table = bq.dataset(BQ_DATASET).table(BQ_TABLE);

  const manualOverride = process.env.SYNC_MIN_DATE && process.env.SYNC_MAX_DATE;
  let sinceDate = manualOverride ? process.env.SYNC_MIN_DATE : await getInitialSinceDate(bq);

  const seenThisRun = new Set();

  let totalInserted = 0;
  let chunkCount = 0;

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
    const mapped = report.rows.map(mapRow);
    const newRows = mapped.filter((r) => {
      const key = dedupKey(r);
      return !existingKeys.has(key) && !seenThisRun.has(key);
    });
    const skipped = mapped.length - newRows.length;
    console.log(`${newRows.length} new rows, ${skipped} skipped as already present`);

    if (DRY_RUN) {
      console.log("DRY RUN — nothing written. Sample:", JSON.stringify(newRows.slice(0, 2), null, 2));
    } else if (newRows.length > 0) {
      await insertInBatches(table, newRows);
      console.log(`Inserted ${newRows.length} rows into ${BQ_TABLE}`);
      await sendSlackMessage(
        `SPINCO - Sales added from ${formatDisplayDate(minDate)} to ${formatDisplayDate(maxDate)}: ${newRows.length} rows`
      );
    } else {
      console.log("Nothing new to insert for this chunk.");
    }

    for (const r of newRows) seenThisRun.add(dedupKey(r));
    totalInserted += newRows.length;

    if (manualOverride) break;
    sinceDate = addDaysUTC(maxDate, 1);
  }

  console.log(`\nDone. ${chunkCount} chunk(s), ${totalInserted} total row(s) ${DRY_RUN ? "would be " : ""}inserted.`);
}

main().catch((err) => {
  console.error("Sync failed:", err.message);
  process.exit(1);
});
