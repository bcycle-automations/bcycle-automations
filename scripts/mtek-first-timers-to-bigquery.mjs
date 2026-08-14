// scripts/mtek-first-timers-to-bigquery.mjs
// Weekly sync: pulls the "First Timers" MTEK report (id 317, slug
// first-timers) and appends new rows into the existing FirstTimersMTEK
// table, matching its string_field_0..12 layout exactly (see docs/
// bcycle-bigquery schema notes for why it's shaped that way).
//
// Each run starts from the latest First Class Date already in the table
// and walks forward in <=7-day chunks — fetching, deduping, and inserting
// one chunk at a time — until it catches up to yesterday (today's data is
// still incomplete when the job runs). A Slack message is posted to
// #bigquery-updates after every real insert.
//
// Dedup: a customer should only ever appear once as a first-timer, so any
// Customer ID already present in the table (including ones inserted
// earlier in this same run) is skipped.
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
import { TABLES } from "./lib/bcycle-mtek-tables.mjs";

const FIRST_TIMERS = TABLES.firstTimers;

const MAX_WINDOW_DAYS = 7;
const SYNC_LABEL = "b.cycle - First Timers";

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const REPORT_ID = FIRST_TIMERS.reportId;
const REPORT_SLUG = FIRST_TIMERS.reportSlug;
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");
const SYNC_DAYS = Number(process.env.SYNC_DAYS || "7");

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SalesZF";
const BQ_TABLE = FIRST_TIMERS.bqTable;

const DRY_RUN = process.env.DRY_RUN !== "false";

// Report headers and column mapping now live in ./lib/bcycle-mtek-tables.mjs
// (shared with the monthly data audit script).
const EXPECTED_HEADERS = FIRST_TIMERS.expectedHeaders;
const mapRow = FIRST_TIMERS.mapRow;

async function getInitialSinceDate(bq) {
  const [[lastDateRow]] = await bq.query({
    query: `SELECT MAX(SAFE.PARSE_DATE('%m/%d/%Y', string_field_5)) AS last_date
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
            WHERE string_field_1 NOT IN ('Customer ID')
              AND string_field_0 IS NOT NULL
              AND string_field_0 NOT LIKE '#%'`,
  });
  return bqDateToString(lastDateRow?.last_date) || pastDaysWindow(SYNC_DAYS).minDate;
}

async function getExistingIds(bq) {
  const [existingRows] = await bq.query({
    query: `SELECT DISTINCT string_field_1 AS customer_id
            FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
            WHERE string_field_1 IS NOT NULL`,
  });
  return new Set(existingRows.map((r) => String(r.customer_id)));
}

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_API_TOKEN");

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
      dateParams: FIRST_TIMERS.dateParams(minDate, maxDate),
    });

    const headersMatch = JSON.stringify(report.headers) === JSON.stringify(EXPECTED_HEADERS);
    if (!headersMatch) {
      throw new Error(
        `Report headers changed shape — refusing to guess. Got: ${JSON.stringify(report.headers)}`
      );
    }
    console.log(`Fetched ${report.rows.length} rows from MTEK`);

    const mapped = report.rows.map(mapRow);
    const newRows = mapped.filter((r) => !existingIds.has(r.string_field_1));
    const skipped = mapped.length - newRows.length;
    console.log(`${newRows.length} new rows, ${skipped} skipped as already present`);

    if (DRY_RUN) {
      console.log("DRY RUN — nothing written. Sample:", JSON.stringify(newRows.slice(0, 2), null, 2));
    } else if (newRows.length > 0) {
      await insertInBatches(table, newRows);
      console.log(`Inserted ${newRows.length} rows into ${BQ_TABLE}`);
      await sendSlackMessage(
        `b.cycle - First Timers added from ${formatDisplayDate(minDate)} to ${formatDisplayDate(maxDate)}: ${newRows.length} rows`
      );
    } else {
      console.log("Nothing new to insert for this chunk.");
    }

    // Keep dedup accurate across chunks in the same run, real or simulated
    for (const r of newRows) existingIds.add(r.string_field_1);
    totalInserted += newRows.length;

    if (manualOverride) break; // explicit single-window test, don't auto-chunk past it
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
