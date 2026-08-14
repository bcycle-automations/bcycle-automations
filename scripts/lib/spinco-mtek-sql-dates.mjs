// scripts/lib/spinco-mtek-sql-dates.mjs
// Shared SQL-snippet builders + date-range helpers for the SPINCO MTEK monthly
// data audit (scripts/spinco-mtek-monthly-data-audit.mjs).
//
// SPINCO's `reservations_raw_2025` table (dataset root-cargo-453703-k7.SPINCO) is
// the one table here with a string-date-formatting risk — confirmed live: its four
// date columns (class_start_date, creation_date, updated_date, cancelled_date) are
// a genuine mix of TWO different date systems, not just inconsistent padding like
// b.cycle's tables:
//   - ISO "YYYY-MM-DD" (the majority historically)
//   - US "M/D/YYYY" / "MM/DD/YYYY" (both padded and unpadded)
// This correlates with when the data was imported (different bulk-load eras used
// different conventions), not the class date itself. The two most recent months
// are effectively all zero-padded MM/DD/YYYY, matching the live sync script's own
// toMDYYYY() writer — so MM/DD/YYYY is the canonical target format going forward.
// The sync script's own getInitialSinceDate query already defensively parses both
// formats (COALESCE of two SAFE.PARSE_DATE attempts) — this file's tolerantDateAny()
// mirrors that same proven shape so the audit and the sync never disagree.
//
// SPINCO's `Sales` and `First Timers` tables have real typed DATE columns — no
// string-format risk there at all; only reservations_raw_2025 needs the tolerant
// parser and format-fix machinery below.

import { isoDate, addDaysUTC } from "./mtek-report.mjs";

// Backtick-quotes a BigQuery identifier (needed for e.g. SPINCO's `First Timers`
// table name, which has a space) — harmless to apply unconditionally.
export function quoteIdent(name) {
  return `\`${String(name).replace(/`/g, "\\`")}\``;
}

// Matches only the canonical "MM/DD/YYYY" form.
export function strictMDY(col) {
  return `REGEXP_CONTAINS(${col}, r'^\\d{2}/\\d{2}/\\d{4}$')`;
}

// Matches padded or unpadded M/D/YYYY (with optional surrounding whitespace).
export function looseMDY(col) {
  return `REGEXP_CONTAINS(${col}, r'^\\s*\\d{1,2}/\\s*\\d{1,2}/\\d{4}\\s*$')`;
}

// Rebuilds a loose M/D/YYYY-shaped value into strict "MM/DD/YYYY". NULL if the
// input isn't M/D/YYYY-shaped at all (e.g. an ISO string) — CONCAT of a NULL
// REGEXP_EXTRACT is NULL.
export function normalizeMDY(col) {
  return (
    `CONCAT(` +
    `LPAD(REGEXP_EXTRACT(${col}, r'^\\s*(\\d{1,2})/'), 2, '0'), '/', ` +
    `LPAD(REGEXP_EXTRACT(${col}, r'/\\s*(\\d{1,2})/'), 2, '0'), '/', ` +
    `REGEXP_EXTRACT(${col}, r'/(\\d{4})\\s*$'))`
  );
}

// Matches an ISO-shaped "YYYY-MM-DD..." prefix (tolerates a trailing time part).
export function isoLike(col) {
  return `REGEXP_CONTAINS(${col}, r'^\\d{4}-\\d{2}-\\d{2}')`;
}

// Reformats an ISO "YYYY-MM-DD" value to strict "MM/DD/YYYY". NULL if the input
// isn't ISO-shaped.
export function isoToMdy(col) {
  return `FORMAT_DATE('%m/%d/%Y', SAFE.PARSE_DATE('%Y-%m-%d', ${col}))`;
}

// The single normalization path for reservations_raw_2025's mixed-format columns:
// try the M/D/YYYY path first, fall back to the ISO path, leave NULL (caught by
// the unparseable-count diagnostic) if neither matches.
export function normalizeAnyToMdy(col) {
  return `COALESCE(${normalizeMDY(col)}, ${isoToMdy(col)})`;
}

// The only date expression that should be used to compare reservations_raw_2025's
// date-string columns against a DATE value. Mirrors the exact dual-parse shape
// already used in spinco-reservations-to-bigquery.mjs's getInitialSinceDate, so
// the audit and the sync agree on what a given string means.
export function tolerantDateAny(col) {
  return `COALESCE(SAFE.PARSE_DATE('%m/%d/%Y', ${normalizeMDY(col)}), SAFE.PARSE_DATE('%Y-%m-%d', ${col}))`;
}

// --- JS date-range helpers (identical shape to bcycle's, duplicated rather than
// cross-imported per this repo's established company-separation convention) ------

function daysInMonth(year, month1to12) {
  return new Date(Date.UTC(year, month1to12, 0)).getUTCDate();
}

export function previousMonthRange(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth();
  const prevMonthDate = new Date(Date.UTC(y, m - 1, 1));
  return monthRange(prevMonthDate.getUTCFullYear(), prevMonthDate.getUTCMonth() + 1);
}

export function monthRange(year, month1to12) {
  const monthStart = `${year}-${String(month1to12).padStart(2, "0")}-01`;
  const lastDay = daysInMonth(year, month1to12);
  const monthEnd = `${year}-${String(month1to12).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`;
  const label = new Date(`${monthStart}T00:00:00Z`).toLocaleDateString("en-US", {
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  });
  return { monthStart, monthEnd, label };
}

export function parseAuditMonth(value, now = new Date()) {
  const match = /^(\d{4})-(\d{2})$/.exec(String(value).trim());
  if (!match) throw new Error(`AUDIT_MONTH must be "YYYY-MM", got: ${value}`);
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (month < 1 || month > 12) throw new Error(`AUDIT_MONTH has an invalid month: ${value}`);

  const range = monthRange(year, month);
  const todayIso = isoDate(now);
  if (range.monthEnd >= todayIso) {
    throw new Error(
      `AUDIT_MONTH (${value}) is not fully in the past (ends ${range.monthEnd}, today is ${todayIso}) — refusing to audit an in-progress month.`
    );
  }
  return range;
}

export function groupContiguous(sortedDates) {
  if (sortedDates.length === 0) return [];
  const runs = [];
  let runStart = sortedDates[0];
  let runEnd = sortedDates[0];

  for (let i = 1; i < sortedDates.length; i++) {
    const d = sortedDates[i];
    if (d === addDaysUTC(runEnd, 1)) {
      runEnd = d;
    } else {
      runs.push({ minDate: runStart, maxDate: runEnd });
      runStart = d;
      runEnd = d;
    }
  }
  runs.push({ minDate: runStart, maxDate: runEnd });
  return runs;
}

export function chunkRange({ minDate, maxDate }, maxDays = 7) {
  const windows = [];
  let start = minDate;
  while (start <= maxDate) {
    const cappedEnd = addDaysUTC(start, maxDays - 1);
    const end = cappedEnd < maxDate ? cappedEnd : maxDate;
    windows.push({ minDate: start, maxDate: end });
    start = addDaysUTC(end, 1);
  }
  return windows;
}
