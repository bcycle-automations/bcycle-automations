// scripts/mtek-reservations-to-bigquery.mjs
// Weekly sync: pulls the "Reservations" MTEK report (id 341, slug
// reservations) for the past N days and appends new rows into the existing
// ReservationsMTEK table. Report's 50 headers map 1:1 (by position) onto
// ReservationsMTEK's 50 columns — real typed columns (INTEGER/DATE/BOOLEAN),
// unlike the all-STRING SalesMTEK/FirstTimersMTEK tables, so values are
// passed through with minimal transformation (dates truncated to date-only,
// "Class Is Public?" normalized from string to boolean).
//
// Dedup key: Reservation ID (confirmed unique, INTEGER).
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

const MAX_WINDOW_DAYS = 7;

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const REPORT_ID = process.env.MTEK_RESERVATIONS_REPORT_ID || "341";
const REPORT_SLUG = process.env.MTEK_RESERVATIONS_REPORT_SLUG || "reservations";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");
const SYNC_DAYS = Number(process.env.SYNC_DAYS || "7");

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SalesZF";
const BQ_TABLE = process.env.BQ_TABLE || "ReservationsMTEK";

const DRY_RUN = process.env.DRY_RUN !== "false";

const EXPECTED_HEADERS = [
  "Reservation ID", "Reservation Type", "Status", "Creation Date", "Updated Date",
  "Cancelled Date", "Unconverted Guest", "Converted Guest", "Guest", "Guest Email",
  "Credit Payment Origin", "Credit", "Package", "Membership", "Contract",
  "Class ID", "Class Start Date", "Class Start Time", "Class Day of Week", "Class Custom Name",
  "Class Is Public?", "Class Tags", "Class Is Free?", "Class Type", "Class Category",
  "Class Duration (Minutes)", "Layout", "Layout Format", "Layout Capacity", "Class Capacity",
  "Classroom", "Location", "Region", "Instructor ID(s)", "Instructor Employee ID(s)",
  "Instructor Names", "Instructor Schedule Names", "Class Has Substitute?", "Customer ID", "Customer Email",
  "Customer Name", "Company Name", "Payer ID", "Payer Email", "Payer Name",
  "Broker ID", "Broker Email", "Broker Name", "Spot", "Spot Type",
];

// Exact BigQuery column names (positional match to EXPECTED_HEADERS above)
const BQ_COLUMNS = [
  "Reservation ID", "Reservation Type", "Status", "Creation Date", "Updated Date",
  "Cancelled Date", "Unconverted Guest", "Converted Guest", "Guest", "Guest Email",
  "Credit Payment Origin", "Credit", "Package", "Membership", "Contract",
  "Class ID", "Class Start Date", "Class Start Time", "Class Day of Week", "Class Custom Name",
  "Class Is Public_", "Class Tags", "Class Is Free_", "Class Type", "Class Category",
  "Class Duration _Minutes_", "Layout", "Layout Format", "Layout Capacity", "Class Capacity",
  "Classroom", "Location", "Region", "Instructor ID_s_", "Instructor Employee ID_s_",
  "Instructor Names", "Instructor Schedule Names", "Class Has Substitute_", "Customer ID", "Customer Email",
  "Customer Name", "Company Name", "Payer ID", "Payer Email", "Payer Name",
  "Broker ID", "Broker Email", "Broker Name", "Spot", "Spot Type",
];

const DATE_ONLY_COLUMNS = new Set(["Creation Date", "Updated Date", "Cancelled Date", "Class Start Date"]);
const STRING_BOOL_COLUMNS = new Set(["Class Is Public_"]); // report sends "True"/"False" string, not real bool

function passthrough(v) {
  return v === undefined ? null : v;
}

function toDateOnly(v) {
  if (v === null || v === undefined) return null;
  return String(v).slice(0, 10);
}

function toBool(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "boolean") return v;
  return String(v).toLowerCase() === "true";
}

function mapRow(row) {
  const out = {};
  BQ_COLUMNS.forEach((bqCol, i) => {
    const raw = row[i];
    if (DATE_ONLY_COLUMNS.has(bqCol)) {
      out[bqCol] = toDateOnly(raw);
    } else if (STRING_BOOL_COLUMNS.has(bqCol)) {
      out[bqCol] = toBool(raw);
    } else {
      out[bqCol] = passthrough(raw);
    }
  });
  return out;
}

async function getInitialSinceDate(bq) {
  const [[lastDateRow]] = await bq.query({
    query: `SELECT MAX(\`Class Start Date\`) AS last_date
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\``,
  });
  return bqDateToString(lastDateRow?.last_date) || pastDaysWindow(SYNC_DAYS).minDate;
}

async function getExistingIds(bq) {
  const [existingRows] = await bq.query({
    query: `SELECT DISTINCT \`Reservation ID\` AS id
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
            WHERE \`Reservation ID\` IS NOT NULL`,
  });
  return new Set(existingRows.map((r) => String(r.id)));
}

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_API_TOKEN");

  const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
  const table = bq.dataset(BQ_DATASET).table(BQ_TABLE);

  const manualOverride = process.env.SYNC_MIN_DATE && process.env.SYNC_MAX_DATE;
  let sinceDate = manualOverride ? process.env.SYNC_MIN_DATE : await getInitialSinceDate(bq);

  const existingIds = await getExistingIds(bq);
  console.log(`${existingIds.size} reservation IDs already in ${BQ_TABLE}`);

  let totalInserted = 0;
  let chunkCount = 0;

  while (true) {
    const window = manualOverride
      ? { minDate: sinceDate, maxDate: process.env.SYNC_MAX_DATE }
      : clampSyncWindow(sinceDate, MAX_WINDOW_DAYS);

    if (!window) {
      console.log(`Caught up (last Class Start Date: ${sinceDate}). Nothing more to sync.`);
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
        min_start_date_day: minDate,
        max_start_date_day: maxDate,
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
    const newRows = mapped.filter((r) => !existingIds.has(String(r["Reservation ID"])));
    const skipped = mapped.length - newRows.length;
    console.log(`${newRows.length} new rows, ${skipped} skipped as already present`);

    if (DRY_RUN) {
      console.log("DRY RUN — nothing written. Sample:", JSON.stringify(newRows.slice(0, 2), null, 2));
    } else if (newRows.length > 0) {
      await table.insert(newRows);
      console.log(`Inserted ${newRows.length} rows into ${BQ_TABLE}`);
      await sendSlackMessage(
        `b.cycle - Reservations added from ${formatDisplayDate(minDate)} to ${formatDisplayDate(maxDate)}: ${newRows.length} rows`
      );
    } else {
      console.log("Nothing new to insert for this chunk.");
    }

    for (const r of newRows) existingIds.add(String(r["Reservation ID"]));
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
