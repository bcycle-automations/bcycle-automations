// scripts/spinco-first-timers-to-bigquery.mjs
// Weekly sync: pulls SPINCO's "First Timers" MTEK report (id 317, slug
// first-timers) and appends new rows into the existing SPINCO.`First Timers`
// table. Unlike b.cycle's FirstTimersMTEK, this table already has real
// typed columns (DATE/BOOLEAN/INTEGER), so values pass through with minimal
// transformation — just date truncation and time formatting.
//
// Each run starts from the latest First Class Date already in the table and
// walks forward in <=7-day chunks until it catches up to yesterday. A Slack
// message is posted to #bigquery-updates after every real insert.
//
// Dedup: Customer ID (a customer should only ever appear once).
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
import { toTime12h } from "./lib/format.mjs";
import { sendSlackMessage } from "./lib/slack.mjs";
import { insertInBatches } from "./lib/bigquery.mjs";

const MAX_WINDOW_DAYS = 2;

const MTEK_BASE_URL = (process.env.MTEK_SPINCO_BASE_URL || "https://spinco.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_SPINCO_API_TOKEN || "").trim();
const REPORT_ID = process.env.MTEK_SPINCO_FIRST_TIMERS_REPORT_ID || "317";
const REPORT_SLUG = process.env.MTEK_SPINCO_FIRST_TIMERS_REPORT_SLUG || "first-timers";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");
const SYNC_DAYS = Number(process.env.SYNC_DAYS || "7");

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SPINCO";
const BQ_TABLE = process.env.BQ_TABLE || "First Timers";

const DRY_RUN = process.env.DRY_RUN !== "false";

const EXPECTED_HEADERS = [
  "Customer",
  "Customer ID",
  "Customer Email",
  "Customer Phone Number",
  "Join Date",
  "First Class Date",
  "Class Start Time",
  "Class Location",
  "Class Type",
  "Class Category",
  "Instructor Name",
  "Guest Reservation?",
  "Pay With Type",
];

function toDateOnly(v) {
  if (v === null || v === undefined) return null;
  return String(v).slice(0, 10);
}

function passthrough(v) {
  return v === undefined ? null : v;
}

function mapRow(row) {
  const [
    customer,
    customerId,
    email,
    phone,
    joinDate,
    firstClassDate,
    classStartTime,
    location,
    classType,
    classCategory,
    instructor,
    guestReservation,
    payWithType,
  ] = row;

  return {
    Customer: passthrough(customer),
    "Customer ID": passthrough(customerId),
    "Customer Email": passthrough(email),
    "Customer Phone Number": passthrough(phone),
    "Join Date": toDateOnly(joinDate),
    "First Class Date": toDateOnly(firstClassDate),
    "Class Start Time": toTime12h(classStartTime),
    "Class Location": passthrough(location),
    "Class Type": passthrough(classType),
    "Class Category": passthrough(classCategory),
    "Instructor Name": passthrough(instructor),
    "Guest Reservation_": passthrough(guestReservation),
    "Pay With Type": passthrough(payWithType),
  };
}

async function getInitialSinceDate(bq) {
  const [[lastDateRow]] = await bq.query({
    query: `SELECT MAX(\`First Class Date\`) AS last_date
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\``,
  });
  return bqDateToString(lastDateRow?.last_date) || pastDaysWindow(SYNC_DAYS).minDate;
}

async function getExistingIds(bq) {
  const [existingRows] = await bq.query({
    query: `SELECT DISTINCT \`Customer ID\` AS customer_id
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
            WHERE \`Customer ID\` IS NOT NULL`,
  });
  return new Set(existingRows.map((r) => String(r.customer_id)));
}

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_SPINCO_API_TOKEN");

  const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
  const table = bq.dataset(BQ_DATASET).table(BQ_TABLE);

  const manualOverride = process.env.SYNC_MIN_DATE && process.env.SYNC_MAX_DATE;
  let sinceDate = manualOverride ? process.env.SYNC_MIN_DATE : await getInitialSinceDate(bq);

  const existingIds = await getExistingIds(bq);
  console.log(`${existingIds.size} customer IDs already in ${BQ_TABLE}`);

  let totalInserted = 0;
  let chunkCount = 0;

  while (true) {
    const window = manualOverride
      ? { minDate: sinceDate, maxDate: process.env.SYNC_MAX_DATE }
      : clampSyncWindow(sinceDate, MAX_WINDOW_DAYS);

    if (!window) {
      console.log(`Caught up (last First Class Date: ${sinceDate}). Nothing more to sync.`);
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
        min_first_class_date_day: minDate,
        max_first_class_date_day: maxDate,
      },
    });

    const headersMatch = JSON.stringify(report.headers) === JSON.stringify(EXPECTED_HEADERS);
    if (!headersMatch) {
      throw new Error(
        `Report headers changed shape — refusing to guess. Got: ${JSON.stringify(report.headers)}`
      );
    }
    console.log(`Fetched ${report.rows.length} rows from MTEK`);

    const mapped = report.rows.map(mapRow);
    const newRows = mapped.filter((r) => !existingIds.has(String(r["Customer ID"])));
    const skipped = mapped.length - newRows.length;
    console.log(`${newRows.length} new rows, ${skipped} skipped as already present`);

    if (DRY_RUN) {
      console.log("DRY RUN — nothing written. Sample:", JSON.stringify(newRows.slice(0, 2), null, 2));
    } else if (newRows.length > 0) {
      await insertInBatches(table, newRows);
      console.log(`Inserted ${newRows.length} rows into ${BQ_TABLE}`);
      await sendSlackMessage(
        `SPINCO - First Timers added from ${formatDisplayDate(minDate)} to ${formatDisplayDate(maxDate)}: ${newRows.length} rows`
      );
    } else {
      console.log("Nothing new to insert for this chunk.");
    }

    for (const r of newRows) existingIds.add(String(r["Customer ID"]));
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
