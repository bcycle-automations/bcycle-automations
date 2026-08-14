// scripts/bcycle-mtek-monthly-data-audit.mjs
// Monthly self-healing audit for b.cycle's three MTEK BigQuery tables
// (SalesMTEK, FirstTimersMTEK, ReservationsMTEK): checks the previous full
// calendar month for (a) missing calendar dates, (b) bad date-string
// formatting, and (c) duplicate rows — auto-fixes what it safely can, and
// posts one consolidated Slack summary to #bigquery-updates either way.
//
// Why this exists: SalesMTEK.order_date_utc/transaction_date and
// FirstTimersMTEK.string_field_4/string_field_5 (all-STRING date columns)
// were found to contain historical rows in three inconsistent formats —
// "MM/DD/YYYY" (good), "M/D/YYYY" (unpadded), and "MM/ D/YYYY" (space
// instead of a zero) — which broke naive SAFE.PARSE_DATE('%m/%d/%Y', ...)
// queries and made real sales silently vanish from date-filtered reports.
// The same root cause produced 153 exact-duplicate FirstTimersMTEK rows for
// one week in July 2026. The daily/weekly sync scripts (mtek-*-to-bigquery)
// are NOT the source of new bad rows going forward (their toMDYYYY() helper
// already zero-pads correctly) — this job is a defensive safety net against
// the mess recurring or going unnoticed, not a fix to the ongoing pipeline.
//
// Every date comparison in this script goes through tolerantDate() from
// lib/bcycle-mtek-sql-dates.mjs (regex-based, tolerant of all three known
// historical formats) rather than a naive SAFE.PARSE_DATE('%m/%d/%Y', col) —
// using the naive form here would silently reintroduce the exact bug this
// job exists to catch.
//
// Ordering (not just style — a hard requirement): reads and DML (format fix,
// dedup) always run BEFORE the streaming backfill insert. BigQuery rejects
// UPDATE/DELETE over rows still in the streaming buffer (~30-90 min after
// insertAll), so backfilling first would intermittently fail the very months
// it had real work to do. Backfilled rows come out of each table's mapRow()
// already correctly formatted, so they never need the format/dedup passes.
//
// In a clean month (the expected steady state) this script issues only
// SELECTs — no snapshot, no staging table, no writes at all.
//
// Defaults to a dry run — set DRY_RUN=false to actually write to BigQuery.
// Dry runs are logged to console only and never post to Slack, matching the
// convention of the three sync scripts this job complements.

import { BigQuery } from "@google-cloud/bigquery";
import { fetchMtekReport } from "./lib/mtek-report.mjs";
import { sendSlackMessage } from "./lib/slack.mjs";
import { insertInBatches } from "./lib/bigquery.mjs";
import { TABLES } from "./lib/bcycle-mtek-tables.mjs";
import {
  strictMDY,
  looseMDY,
  normalizeMDY,
  tolerantDate,
  quoteIdent,
  previousMonthRange,
  parseAuditMonth,
  groupContiguous,
  chunkRange,
} from "./lib/bcycle-mtek-sql-dates.mjs";

const MAX_WINDOW_DAYS = 7;
const JOB_LABEL = "b.cycle MTEK monthly data audit";

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SalesZF";

const DRY_RUN = process.env.DRY_RUN !== "false";

const ALL_TABLE_KEYS = Object.keys(TABLES);
const AUDIT_TABLE_KEYS = (process.env.AUDIT_TABLES || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const TABLE_KEYS_TO_RUN = AUDIT_TABLE_KEYS.length > 0 ? AUDIT_TABLE_KEYS : ALL_TABLE_KEYS;

function fqTable(bqTable) {
  return `\`${BQ_PROJECT_ID}.${BQ_DATASET}.${bqTable}\``;
}

// The single date expression a table's audit queries should compare against
// a DATE value: the quoted column directly for a real DATE column
// (Reservations), or the tolerant regex-based parse for a date-string column
// (Sales, First Timers) — never a naive SAFE.PARSE_DATE('%m/%d/%Y', col).
function dateExprSql(cfg) {
  const col = quoteIdent(cfg.auditDateColumn);
  return cfg.auditDateKind === "date" ? col : tolerantDate(col);
}

function monthPredicateSql(cfg) {
  return `${dateExprSql(cfg)} BETWEEN @monthStart AND @monthEnd`;
}

// Converts a mapped row's audit-date field ("07/05/2026" for string-mdy
// tables, already "2026-07-05" for the real-DATE Reservations table) to
// "YYYY-MM-DD", or null if missing/unparseable.
function rowDateIso(cfg, mappedRow) {
  const raw = mappedRow[cfg.auditDateColumn];
  if (!raw) return null;
  if (cfg.auditDateKind === "date") return String(raw).slice(0, 10);
  const m = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(String(raw));
  if (!m) return null;
  const [, month, day, year] = m;
  return `${year}-${month}-${day}`;
}

// --- Read-phase queries ------------------------------------------------

async function queryMissingDates(bq, cfg, monthStart, monthEnd) {
  const query = `
    WITH days AS (
      SELECT d FROM UNNEST(GENERATE_DATE_ARRAY(@monthStart, @monthEnd)) AS d
    ),
    present AS (
      SELECT DISTINCT ${dateExprSql(cfg)} AS d
      FROM ${fqTable(cfg.bqTable)}
      WHERE ${cfg.junkFilterSql} AND ${monthPredicateSql(cfg)}
    )
    SELECT d FROM days
    WHERE d NOT IN (SELECT d FROM present WHERE d IS NOT NULL)
    ORDER BY d
  `;
  const [rows] = await bq.query({ query, params: { monthStart, monthEnd } });
  return rows.map((r) => (typeof r.d === "string" ? r.d : r.d.value));
}

async function queryUnparseableCount(bq, cfg) {
  if (cfg.auditDateKind !== "string-mdy") return 0;
  const query = `
    SELECT COUNT(*) AS n
    FROM ${fqTable(cfg.bqTable)}
    WHERE ${cfg.junkFilterSql} AND ${tolerantDate(quoteIdent(cfg.auditDateColumn))} IS NULL
  `;
  const [[row]] = await bq.query({ query });
  return Number(row.n);
}

async function queryBadFormatCount(bq, cfg, monthStart, monthEnd) {
  if (cfg.formatColumns.length === 0) return 0;
  const badClause = cfg.formatColumns
    .map((col) => `NOT IFNULL(${strictMDY(quoteIdent(col))}, FALSE)`)
    .join(" OR ");
  const query = `
    SELECT COUNT(*) AS n
    FROM ${fqTable(cfg.bqTable)}
    WHERE ${monthPredicateSql(cfg)} AND (${badClause})
  `;
  const [[row]] = await bq.query({ query, params: { monthStart, monthEnd } });
  return Number(row.n);
}

async function queryFullRowDupCount(bq, cfg, monthStart, monthEnd) {
  const query = `
    SELECT COUNT(*) AS total_rows, COUNT(DISTINCT TO_JSON_STRING(t)) AS distinct_rows
    FROM ${fqTable(cfg.bqTable)} AS t
    WHERE ${monthPredicateSql(cfg)}
  `;
  const [[row]] = await bq.query({ query, params: { monthStart, monthEnd } });
  const total = Number(row.total_rows);
  const distinct = Number(row.distinct_rows);
  return { total, distinct, dupCount: total - distinct };
}

// Rows sharing this table's normal dedup key but NOT identical in every
// other column — a genuine upstream problem (not the "same record inserted
// twice" case, which the full-row dedup already handles). Report-only.
async function queryKeyCollisionCount(bq, cfg, monthStart, monthEnd) {
  const keyCols = cfg.dedupKeyColumns.map(quoteIdent).join(", ");
  const query = `
    SELECT COUNT(*) AS n FROM (
      SELECT ${keyCols}, COUNT(*) AS n, COUNT(DISTINCT TO_JSON_STRING(t)) AS distinct_n
      FROM ${fqTable(cfg.bqTable)} AS t
      WHERE ${monthPredicateSql(cfg)}
      GROUP BY ${keyCols}
      HAVING n > 1 AND distinct_n > 1
    )
  `;
  const [[row]] = await bq.query({ query, params: { monthStart, monthEnd } });
  return Number(row.n);
}

// --- Mutate-phase DML ----------------------------------------------------

async function fixFormatting(bq, cfg, monthStart, monthEnd) {
  const setClauses = cfg.formatColumns
    .map((col) => {
      const q = quoteIdent(col);
      return `${q} = IF(${looseMDY(q)}, ${normalizeMDY(q)}, ${q})`;
    })
    .join(",\n    ");
  const badClause = cfg.formatColumns
    .map((col) => `NOT IFNULL(${strictMDY(quoteIdent(col))}, FALSE)`)
    .join(" OR ");
  const query = `
    UPDATE ${fqTable(cfg.bqTable)}
    SET ${setClauses}
    WHERE ${monthPredicateSql(cfg)} AND (${badClause})
  `;
  await bq.query({ query, params: { monthStart, monthEnd } });
}

// Staging-table + transaction dedup, scoped to the audited month only.
// Deliberately NOT a whole-table CREATE OR REPLACE (see lib note) — that
// would reset the table's time-travel history every month for no benefit.
// Only called when a dup count > 0 was already confirmed by the caller.
async function removeDuplicates(bq, cfg, monthStart, monthEnd, expectedDistinct) {
  const stagingTable = `${cfg.bqTable}_audit_dedup_tmp`;
  const fqStaging = fqTable(stagingTable);
  const predicate = monthPredicateSql(cfg);

  await bq.query({
    query: `CREATE OR REPLACE TABLE ${fqStaging} AS
            SELECT DISTINCT * FROM ${fqTable(cfg.bqTable)} WHERE ${predicate}`,
    params: { monthStart, monthEnd },
  });

  try {
    const script = `
      BEGIN TRANSACTION;
        DELETE FROM ${fqTable(cfg.bqTable)} WHERE ${predicate};
        INSERT INTO ${fqTable(cfg.bqTable)} SELECT * FROM ${fqStaging};
        ASSERT (SELECT COUNT(*) FROM ${fqTable(cfg.bqTable)} WHERE ${predicate}) = @expectedDistinct
          AS 'Monthly audit dedup row count mismatch — rolling back';
      COMMIT TRANSACTION;
    `;
    await bq.query({ query: script, params: { monthStart, monthEnd, expectedDistinct } });
  } finally {
    await bq.query({ query: `DROP TABLE IF EXISTS ${fqStaging}` });
  }
}

// --- Backfill phase --------------------------------------------------------

// Existing dedup keys for the missing-date span (tolerant date filter — a
// naive SAFE.PARSE_DATE('%m/%d/%Y', col) here would make a still-bad-format
// existing row look "absent" and get re-inserted as a duplicate, which is
// the same bug class this whole job exists to catch). By the time this runs
// (after the format-fix DML, in a real run), the month's dates are already
// normalized, so this only matters for correctness in DRY_RUN previews.
async function loadExistingKeys(bq, cfg, minDate, maxDate) {
  const bufferedMin = new Date(`${minDate}T00:00:00Z`);
  bufferedMin.setUTCDate(bufferedMin.getUTCDate() - 3);
  const bufferedMax = new Date(`${maxDate}T00:00:00Z`);
  bufferedMax.setUTCDate(bufferedMax.getUTCDate() + 3);
  const bufferedMinIso = bufferedMin.toISOString().slice(0, 10);
  const bufferedMaxIso = bufferedMax.toISOString().slice(0, 10);

  const cols = cfg.dedupKeyColumns.map(quoteIdent).join(", ");
  const query = `
    SELECT DISTINCT ${cols}
    FROM ${fqTable(cfg.bqTable)}
    WHERE ${cfg.junkFilterSql}
      AND ${dateExprSql(cfg)} BETWEEN @minDate AND @maxDate
  `;
  const [rows] = await bq.query({ query, params: { minDate: bufferedMinIso, maxDate: bufferedMaxIso } });
  return new Set(rows.map((r) => cfg.dedupKey(r)));
}

async function backfillMissingDates(bq, table, cfg, missingDates) {
  const result = { backfilled: 0, confirmedEmptyDates: [] };
  if (missingDates.length === 0) return result;

  const runs = groupContiguous(missingDates);
  const windows = runs.flatMap((r) => chunkRange(r, MAX_WINDOW_DAYS));
  const existingKeys = await loadExistingKeys(bq, cfg, missingDates[0], missingDates[missingDates.length - 1]);
  const seenThisRun = new Set();
  const mtekRowCountByDate = new Map();

  for (const w of windows) {
    const report = await fetchMtekReport({
      baseUrl: MTEK_BASE_URL,
      token: MTEK_API_TOKEN,
      reportId: cfg.reportId,
      slug: cfg.reportSlug,
      pageSize: PAGE_SIZE,
      dateParams: cfg.dateParams(w.minDate, w.maxDate),
    });

    const headersMatch = JSON.stringify(report.headers) === JSON.stringify(cfg.expectedHeaders);
    if (!headersMatch) {
      throw new Error(
        `${cfg.label} report headers changed shape — refusing to guess. Got: ${JSON.stringify(report.headers)}`
      );
    }

    const mapped = report.rows.map(cfg.mapRow);
    for (const row of mapped) {
      const iso = rowDateIso(cfg, row);
      if (iso) mtekRowCountByDate.set(iso, (mtekRowCountByDate.get(iso) || 0) + 1);
    }

    const newRows = mapped.filter((row) => {
      const key = cfg.dedupKey(row);
      return !existingKeys.has(key) && !seenThisRun.has(key);
    });

    if (!DRY_RUN && newRows.length > 0) {
      await insertInBatches(table, newRows);
    }
    for (const row of newRows) seenThisRun.add(cfg.dedupKey(row));
    result.backfilled += newRows.length;
  }

  result.confirmedEmptyDates = missingDates.filter((d) => !(mtekRowCountByDate.get(d) > 0));
  return result;
}

// --- Per-table orchestration -------------------------------------------

async function auditTable(bq, cfg, monthStart, monthEnd) {
  const table = bq.dataset(BQ_DATASET).table(cfg.bqTable);

  // Gap detection and the unparseable-rows diagnostic both go through
  // tolerantDate(), which parses any of the known historical formats
  // equally well — safe to read before the format fix.
  const missingDates = await queryMissingDates(bq, cfg, monthStart, monthEnd);
  const unparseableCount = await queryUnparseableCount(bq, cfg);
  const badFormatCount = await queryBadFormatCount(bq, cfg, monthStart, monthEnd);

  console.log(
    `[${cfg.label}] missing dates: ${missingDates.length}, unparseable: ${unparseableCount}, bad format: ${badFormatCount}`
  );

  // Format fix must run BEFORE the dup/key-collision reads below, not
  // after — two rows differing only by date-string variant (e.g.
  // "7/1/2026" vs "07/ 1/2026" for the same real event, exactly what
  // caused FirstTimersMTEK's July duplicates) are NOT byte-identical until
  // normalized, so counting duplicates first would silently miss exactly
  // the duplicates this format bug produces. Sales' dedup key also
  // includes transaction_date, so key-collision detection has the same
  // requirement.
  let formatFixed = 0;
  if (badFormatCount > 0) {
    if (!DRY_RUN) {
      await fixFormatting(bq, cfg, monthStart, monthEnd);
      console.log(`[${cfg.label}] fixed formatting on ${badFormatCount} row(s)`);
    } else {
      console.log(`[${cfg.label}] DRY RUN — would fix formatting on ${badFormatCount} row(s)`);
    }
    formatFixed = badFormatCount;
  }

  // In DRY_RUN, the fix above didn't actually run — dup/key-collision
  // counts below still reflect current (possibly still-bad-format) state,
  // so a dry-run preview can undercount format-driven duplicates. That's
  // an acceptable, clearly-labeled preview limitation; a live run always
  // sees the post-fix state before counting.
  const dup = await queryFullRowDupCount(bq, cfg, monthStart, monthEnd);
  const keyCollisions = await queryKeyCollisionCount(bq, cfg, monthStart, monthEnd);
  console.log(`[${cfg.label}] dup rows: ${dup.dupCount}, key collisions: ${keyCollisions}`);

  let dupsRemoved = 0;
  if (dup.dupCount > 0) {
    if (!DRY_RUN) {
      await removeDuplicates(bq, cfg, monthStart, monthEnd, dup.distinct);
      console.log(`[${cfg.label}] removed ${dup.dupCount} duplicate row(s)`);
    } else {
      console.log(`[${cfg.label}] DRY RUN — would remove ${dup.dupCount} duplicate row(s)`);
    }
    dupsRemoved = dup.dupCount;
  }

  // BACKFILL phase — last, per the streaming-buffer ordering constraint.
  const { backfilled, confirmedEmptyDates } = await backfillMissingDates(bq, table, cfg, missingDates);
  if (missingDates.length > 0) {
    console.log(
      `[${cfg.label}] ${DRY_RUN ? "would backfill" : "backfilled"} ${backfilled} row(s); ` +
        `${confirmedEmptyDates.length} date(s) confirmed empty in MTEK`
    );
  }

  return {
    key: cfg.key,
    label: cfg.label,
    hasFormatCheck: cfg.formatColumns.length > 0,
    missingDates,
    confirmedEmptyDates,
    backfilled,
    unparseableCount,
    badFormatCount,
    formatFixed,
    dupCount: dup.dupCount,
    dupsRemoved,
    keyCollisions,
    error: null,
  };
}

// --- Slack summary ---------------------------------------------------------

function buildTableLine(r, totalDays) {
  if (r.error) return `❌ ${r.label}: audit failed — ${r.error}`;

  const parts = [];
  const confirmedEmptyN = r.confirmedEmptyDates.length;
  const daysPresentAfter = totalDays - confirmedEmptyN;
  const backfilledDaysN = r.missingDates.length - confirmedEmptyN;

  if (r.backfilled > 0) {
    parts.push(`backfilled ${backfilledDaysN} missing day(s) → ${r.backfilled} row(s)`);
  } else {
    let presence = `${daysPresentAfter}/${totalDays} days present`;
    if (confirmedEmptyN > 0) presence += `, ${confirmedEmptyN} confirmed empty in MTEK`;
    parts.push(presence);
  }

  if (r.hasFormatCheck) {
    parts.push(r.formatFixed > 0 ? `${r.formatFixed} date format row(s) corrected` : "formats OK");
  }

  parts.push(r.dupsRemoved > 0 ? `${r.dupsRemoved} duplicate row(s) removed` : "no duplicates");

  if (r.unparseableCount > 0) {
    parts.push(`⚠️ ${r.unparseableCount} row(s) with unparseable dates (not auto-fixed)`);
  }

  return `• ${r.label}: ${parts.join(" · ")}`;
}

function buildSlackMessage({ monthLabel, results }) {
  const hasFatalError = results.some((r) => r.error);
  const hasFix = results.some((r) => r.backfilled > 0 || r.formatFixed > 0 || r.dupsRemoved > 0);
  const hasWarning = results.some((r) => r.unparseableCount > 0 || r.keyCollisions > 0);

  let titleEmoji = "📋";
  let titleStatus = "all clean";
  if (hasFatalError) {
    titleEmoji = "❌";
    titleStatus = "completed with errors";
  } else if (hasFix || hasWarning) {
    titleEmoji = "⚠️";
    const fixCount = results.reduce(
      (n, r) => n + (r.backfilled > 0 ? 1 : 0) + (r.formatFixed > 0 ? 1 : 0) + (r.dupsRemoved > 0 ? 1 : 0),
      0
    );
    titleStatus = fixCount > 0 ? `${fixCount} issue(s) found and fixed` : "issue(s) found, needs review";
  }

  const lines = [`${titleEmoji} ${JOB_LABEL} — ${monthLabel}: ${titleStatus}`];

  for (const r of results) {
    lines.push(buildTableLine(r, r.totalDaysInMonth));
  }

  const collisionLines = results
    .filter((r) => r.keyCollisions > 0)
    .map(
      (r) =>
        `⚠️ Needs a human: ${r.label} — ${r.keyCollisions} row(s) share a dedup key but differ (not auto-removed)`
    );
  lines.push(...collisionLines);

  let message = lines.join("\n");
  if (DRY_RUN) message = `(DRY RUN — nothing was changed)\n${message}`;
  return message;
}

// --- Main --------------------------------------------------------------

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_API_TOKEN");

  for (const key of TABLE_KEYS_TO_RUN) {
    if (!TABLES[key]) {
      throw new Error(`Unknown table key in AUDIT_TABLES: "${key}" (valid: ${ALL_TABLE_KEYS.join(", ")})`);
    }
  }

  const { monthStart, monthEnd, label: monthLabel } = process.env.AUDIT_MONTH
    ? parseAuditMonth(process.env.AUDIT_MONTH)
    : previousMonthRange();

  const totalDaysInMonth =
    Math.round((new Date(`${monthEnd}T00:00:00Z`) - new Date(`${monthStart}T00:00:00Z`)) / 86400000) + 1;

  console.log(`${JOB_LABEL} — auditing ${monthLabel} (${monthStart}..${monthEnd}), tables: ${TABLE_KEYS_TO_RUN.join(", ")}`);
  console.log(DRY_RUN ? "DRY RUN — no writes will be made, no Slack message will be sent." : "LIVE RUN.");

  const bq = new BigQuery({ projectId: BQ_PROJECT_ID });

  const results = [];
  for (const key of TABLE_KEYS_TO_RUN) {
    const cfg = TABLES[key];
    try {
      const r = await auditTable(bq, cfg, monthStart, monthEnd);
      r.totalDaysInMonth = totalDaysInMonth;
      results.push(r);
    } catch (err) {
      console.error(`[${cfg.label}] audit failed:`, err.message);
      results.push({
        key: cfg.key,
        label: cfg.label,
        hasFormatCheck: cfg.formatColumns.length > 0,
        missingDates: [],
        confirmedEmptyDates: [],
        backfilled: 0,
        unparseableCount: 0,
        badFormatCount: 0,
        formatFixed: 0,
        dupCount: 0,
        dupsRemoved: 0,
        keyCollisions: 0,
        totalDaysInMonth,
        error: err.message,
      });
    }
  }

  const message = buildSlackMessage({ monthLabel, results });
  console.log("\n" + message);

  if (!DRY_RUN) {
    await sendSlackMessage(message);
  }

  if (results.some((r) => r.error)) {
    process.exit(1);
  }
}

main().catch(async (err) => {
  console.error("Monthly data audit failed:", err.message);
  if (!DRY_RUN) {
    try {
      await sendSlackMessage(`⚠️ ${JOB_LABEL} failed: ${err.message}`);
    } catch (slackErr) {
      console.error("Additionally failed to post failure notice to Slack:", slackErr.message);
    }
  }
  process.exit(1);
});
