// scripts/test-mtek-sales-event-time.mjs
// Unit tests (no network/BigQuery access) for the event_time derivation and
// age-cutoff logic in mtek-sales-to-bigquery.mjs — run with:
//   node scripts/test-mtek-sales-event-time.mjs
//
// mtek-sales-to-bigquery.mjs guards its main() behind an import.meta.url
// check specifically so this file can import its pure helpers without
// triggering a real sync (which would fail fast on missing env vars anyway).

import { eventTimeFromRow, isEventTooOldForMeta, META_MAX_EVENT_AGE_DAYS } from "./mtek-sales-to-bigquery.mjs";

const HEADER_INDEX = {
  "Order Date (UTC)": 0,
  "Order Time (UTC)": 1,
};

function rawRow(orderDateUtc, orderTimeUtc) {
  const row = [];
  row[HEADER_INDEX["Order Date (UTC)"]] = orderDateUtc;
  row[HEADER_INDEX["Order Time (UTC)"]] = orderTimeUtc;
  return row;
}

let failures = 0;

function assertEqual(actual, expected, label) {
  if (actual === expected) {
    console.log(`PASS: ${label}`);
  } else {
    failures++;
    console.error(`FAIL: ${label}\n  expected: ${expected}\n  actual:   ${actual}`);
  }
}

function assertThrows(fn, label) {
  try {
    fn();
    failures++;
    console.error(`FAIL: ${label}\n  expected a throw, but none occurred`);
  } catch (err) {
    console.log(`PASS: ${label} (threw: ${err.message})`);
  }
}

// 1. Same-day purchase: order and transaction on the same UTC day -> keeps
// the precise order timestamp, not a flattened noon-UTC one.
{
  const raw = rawRow("2026-09-20", "14:35:00");
  const row = { transaction_date: "09/20/2026" };
  const expected = Math.floor(Date.parse("2026-09-20T14:35:00Z") / 1000);
  assertEqual(eventTimeFromRow(raw, row, HEADER_INDEX), expected, "same-day purchase keeps the order time");
}

// 2. Renewal: old 2025 order date, recent transaction date -> maps to the
// transaction date (at noon UTC), not the stale order date.
{
  const raw = rawRow("2025-01-15", "08:00:00");
  const row = { transaction_date: "09/20/2026" };
  const expected = Math.floor(Date.UTC(2026, 8, 20, 12, 0, 0) / 1000);
  assertEqual(
    eventTimeFromRow(raw, row, HEADER_INDEX),
    expected,
    "renewal (2025 order date, recent transaction date) maps to the transaction date"
  );
}

// 3. Blank order time: Order Time (UTC) missing/empty -> defaults to
// 00:00:00 and, since it's still the same UTC day as the transaction,
// resolves to midnight UTC rather than throwing.
{
  const raw = rawRow("2026-09-20", "");
  const row = { transaction_date: "09/20/2026" };
  const expected = Math.floor(Date.parse("2026-09-20T00:00:00Z") / 1000);
  assertEqual(eventTimeFromRow(raw, row, HEADER_INDEX), expected, "blank order time defaults to midnight UTC");
}

// 4. A 3-week-old row's derived event_time must be filtered out as too old
// for Meta (event age > META_MAX_EVENT_AGE_DAYS).
{
  const now = new Date();
  const threeWeeksAgo = new Date(now.getTime() - 21 * 24 * 60 * 60 * 1000);
  const orderDateUtc = threeWeeksAgo.toISOString().slice(0, 10);
  const txDateStr = `${threeWeeksAgo.getUTCMonth() + 1}/${threeWeeksAgo.getUTCDate()}/${threeWeeksAgo.getUTCFullYear()}`;

  const raw = rawRow(orderDateUtc, "09:00:00");
  const row = { transaction_date: txDateStr };
  const eventTime = eventTimeFromRow(raw, row, HEADER_INDEX);
  const nowSeconds = Math.floor(now.getTime() / 1000);

  assertEqual(isEventTooOldForMeta(eventTime, nowSeconds), true, "3-week-old row is filtered out as too old for Meta");
}

// Bonus sanity check: a fresh (1-day-old) event must NOT be filtered out —
// guards against an inverted comparison in isEventTooOldForMeta.
{
  const now = new Date();
  const yesterday = new Date(now.getTime() - 1 * 24 * 60 * 60 * 1000);
  const eventTime = Math.floor(yesterday.getTime() / 1000);
  const nowSeconds = Math.floor(now.getTime() / 1000);
  assertEqual(isEventTooOldForMeta(eventTime, nowSeconds), false, "a 1-day-old event is NOT filtered out");
}

// Fallback / throw behavior: unparseable Transaction Date falls back to the
// order timestamp; if neither parses, it throws.
{
  const raw = rawRow("2026-09-20", "14:35:00");
  const row = { transaction_date: "not-a-date" };
  const expected = Math.floor(Date.parse("2026-09-20T14:35:00Z") / 1000);
  assertEqual(
    eventTimeFromRow(raw, row, HEADER_INDEX),
    expected,
    "unparseable Transaction Date falls back to the order timestamp"
  );
}
{
  const raw = rawRow("", "");
  const row = { transaction_date: "not-a-date" };
  assertThrows(() => eventTimeFromRow(raw, row, HEADER_INDEX), "throws when neither Transaction Date nor Order Date parse");
}

console.log(`\n${failures === 0 ? "All tests passed." : `${failures} test(s) failed.`} (Meta max event age: ${META_MAX_EVENT_AGE_DAYS} days)`);
if (failures > 0) process.exit(1);
