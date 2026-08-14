// scripts/lib/bcycle-mtek-sql-dates.mjs
// Shared SQL-snippet builders + date-range helpers for the b.cycle MTEK monthly
// data audit (scripts/bcycle-mtek-monthly-data-audit.mjs).
//
// Historical rows in SalesMTEK.order_date_utc/transaction_date and
// FirstTimersMTEK.string_field_4/string_field_5 (all-STRING date columns) were
// found in three formats: "MM/DD/YYYY" (good), "M/D/YYYY" (unpadded), and
// "MM/ D/YYYY" (space instead of a zero for single-digit days). A naive
// SAFE.PARSE_DATE('%m/%d/%Y', col) silently drops the space-padded rows,
// which is exactly the bug that caused a false "missing July sales" alarm.
// Every date-touching query in the audit MUST go through tolerantDate() below
// instead of parsing the raw column directly, or it reintroduces that bug.
//
// normalizeMDY() is deliberately byte-identical in shape to the transform used
// in the one-off manual fix, so the gap-detection query and the format-fix
// UPDATE can never disagree about what counts as "already correct."

import { isoDate, addDaysUTC } from "./mtek-report.mjs";

// Backtick-quotes a BigQuery identifier (needed for column names containing
// spaces/punctuation, e.g. ReservationsMTEK's "Reservation ID" — harmless to
// apply to plain snake_case names too, so callers can use it unconditionally.
export function quoteIdent(name) {
  return `\`${String(name).replace(/`/g, "\\`")}\``;
}

// Matches only the canonical "MM/DD/YYYY" form.
export function strictMDY(col) {
  return `REGEXP_CONTAINS(${col}, r'^\\d{2}/\\d{2}/\\d{4}$')`;
}

// Matches any of the three known historical variants (zero-padded, unpadded,
// or space-padded single-digit month/day).
export function looseMDY(col) {
  return `REGEXP_CONTAINS(${col}, r'^\\s*\\d{1,2}/\\s*\\d{1,2}/\\d{4}\\s*$')`;
}

// Rebuilds any of the loose variants into strict "MM/DD/YYYY". NULL on
// anything that doesn't loosely match (e.g. a leaked header row's literal
// column-name value) — CONCAT of a NULL REGEXP_EXTRACT is NULL.
export function normalizeMDY(col) {
  return (
    `CONCAT(` +
    `LPAD(REGEXP_EXTRACT(${col}, r'^\\s*(\\d{1,2})/'), 2, '0'), '/', ` +
    `LPAD(REGEXP_EXTRACT(${col}, r'/\\s*(\\d{1,2})/'), 2, '0'), '/', ` +
    `REGEXP_EXTRACT(${col}, r'/(\\d{4})\\s*$'))`
  );
}

// The only date expression that should ever be used to compare a raw MTEK
// date-string column against a DATE value (month-range filters, gap
// detection, dedup lookups). NULL for junk/header rows and any value that
// doesn't even loosely resemble a date.
export function tolerantDate(col) {
  return `SAFE.PARSE_DATE('%m/%d/%Y', ${normalizeMDY(col)})`;
}

// --- JS date-range helpers -------------------------------------------------

function daysInMonth(year, month1to12) {
  // Day 0 of the *next* month = last day of this month.
  return new Date(Date.UTC(year, month1to12, 0)).getUTCDate();
}

// { monthStart, monthEnd, label } for the full previous UTC calendar month
// relative to "now" (or an injected Date, for tests).
export function previousMonthRange(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth(); // 0-11, "this month" 1-12 is m+1
  // Previous month, 0-11 indexed with year rollover handled by Date().
  const prevMonthDate = new Date(Date.UTC(y, m - 1, 1));
  return monthRange(prevMonthDate.getUTCFullYear(), prevMonthDate.getUTCMonth() + 1);
}

// { monthStart, monthEnd, label } for a given calendar month (1-12).
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

// Parses a "YYYY-MM" string into a monthRange(), throwing on anything
// malformed or on a month that isn't entirely in the past (auditing an
// in-progress month would flag every not-yet-happened day as "missing").
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

// Groups a sorted array of "YYYY-MM-DD" strings into contiguous runs, e.g.
// ["07-01","07-02","07-04"] -> [["07-01","07-02"], ["07-04"]]. Each run is
// returned as { minDate, maxDate }.
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

// Splits a { minDate, maxDate } range into <=maxDays windows (same chunking
// convention as clampSyncWindow in mtek-report.mjs, but for a fixed range
// rather than an open-ended "walk forward to yesterday" one).
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
