// scripts/spinco-reservations-to-bigquery.mjs
// Weekly sync: pulls SPINCO's "Reservations" MTEK report (id 341, slug
// reservations) and appends new rows into the existing
// SPINCO.reservations_raw_2025 table. Per explicit instruction, this table
// stays exactly as-is — every field is STRING-typed (even numbers/booleans/
// dates), lowercase snake_case column names, and existing queries depend on
// that shape. New rows are written to match: dates as zero-padded
// M/D/YYYY strings (matching the documented `SAFE.PARSE_DATE('%m/%d/%Y', ...)`
// convention already used against this table), booleans as lowercase
// "true"/"false" text, everything else stringified as-is.
//
// Each run starts from the latest class_start_date already in the table
// (parsed — the raw column is an unpadded, sometimes-mixed-format string,
// so a naive MAX() would be wrong) and walks forward in <=7-day chunks
// until it catches up to yesterday. A Slack message is posted to
// #bigquery-updates after every real insert.
//
// Dedup key: reservation_id (confirmed unique across the whole table).
//
// Defaults to a dry run — set DRY_RUN=false to actually write to BigQuery.

import { BigQuery } from "@google-cloud/bigquery";
import {
  fetchMtekReport,
  pastDaysWindow,
  clampSyncWindow,
  addDaysUTC,
  formatDisplayDate,
} from "./lib/mtek-report.mjs";
import { sendSlackMessage } from "./lib/slack.mjs";
import { insertInBatches } from "./lib/bigquery.mjs";
import { TABLES } from "./lib/spinco-mtek-tables.mjs";

const RESERVATIONS = TABLES.reservations;

const MAX_WINDOW_DAYS = 2;
const SYNC_LABEL = "SPINCO - Reservations";

const MTEK_BASE_URL = (process.env.MTEK_SPINCO_BASE_URL || "https://spinco.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_SPINCO_API_TOKEN || "").trim();
const REPORT_ID = RESERVATIONS.reportId;
const REPORT_SLUG = RESERVATIONS.reportSlug;
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");
const SYNC_DAYS = Number(process.env.SYNC_DAYS || "7");

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SPINCO";
const BQ_TABLE = RESERVATIONS.bqTable;

const DRY_RUN = process.env.DRY_RUN !== "false";

// Report headers and column mapping now live in ./lib/spinco-mtek-tables.mjs
// (shared with the monthly data audit script).
const EXPECTED_HEADERS = RESERVATIONS.expectedHeaders;
const mapRow = RESERVATIONS.mapRow;

// class_start_date in the existing table is a mix of unpadded M/D/YYYY and
// ISO YYYY-MM-DD — parse both to find the true chronological max.
async function getInitialSinceDate(bq) {
  const [[lastDateRow]] = await bq.query({
    query: `SELECT MAX(COALESCE(
              SAFE.PARSE_DATE('%m/%d/%Y', class_start_date),
              SAFE.PARSE_DATE('%Y-%m-%d', class_start_date)
            )) AS last_date
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\``,
  });
  const raw = lastDateRow?.last_date;
  const lastDate = raw == null ? null : typeof raw === "string" ? raw : raw.value;
  return lastDate || pastDaysWindow(SYNC_DAYS).minDate;
}

async function getExistingIds(bq) {
  const [existingRows] = await bq.query({
    query: `SELECT DISTINCT reservation_id AS id
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
            WHERE reservation_id IS NOT NULL`,
  });
  return new Set(existingRows.map((r) => String(r.id)));
}

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_SPINCO_API_TOKEN");

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
      console.log(`Caught up (last class_start_date: ${sinceDate}). Nothing more to sync.`);
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
      dateParams: RESERVATIONS.dateParams(minDate, maxDate),
    });

    const headersMatch = JSON.stringify(report.headers) === JSON.stringify(EXPECTED_HEADERS);
    if (!headersMatch) {
      throw new Error(
        `Report headers changed shape — refusing to guess. Got: ${JSON.stringify(report.headers)}`
      );
    }
    console.log(`Fetched ${report.rows.length} rows from MTEK`);

    const mapped = report.rows.map(mapRow);
    const newRows = mapped.filter((r) => !existingIds.has(String(r.reservation_id)));
    const skipped = mapped.length - newRows.length;
    console.log(`${newRows.length} new rows, ${skipped} skipped as already present`);

    if (DRY_RUN) {
      console.log("DRY RUN — nothing written. Sample:", JSON.stringify(newRows.slice(0, 2), null, 2));
    } else if (newRows.length > 0) {
      await insertInBatches(table, newRows);
      console.log(`Inserted ${newRows.length} rows into ${BQ_TABLE}`);
      await sendSlackMessage(
        `SPINCO - Reservations added from ${formatDisplayDate(minDate)} to ${formatDisplayDate(maxDate)}: ${newRows.length} rows`
      );
    } else {
      console.log("Nothing new to insert for this chunk.");
    }

    for (const r of newRows) existingIds.add(String(r.reservation_id));
    totalInserted += newRows.length;

    if (manualOverride) break;
    sinceDate = addDaysUTC(maxDate, 1);
  }

  console.log(`\nDone. ${chunkCount} chunk(s), ${totalInserted} total row(s) ${DRY_RUN ? "would be " : ""}inserted.`);
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
