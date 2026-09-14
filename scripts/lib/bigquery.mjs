// scripts/lib/bigquery.mjs
// BigQuery's tabledata.insertAll has a ~10MB request size limit — a single
// large chunk (e.g. thousands of wide reservation rows) can exceed that and
// fail with HTTP 413. Insert in fixed-size batches instead of one call.
//
// insertAll also runs with skipInvalidRows=false, so a SINGLE malformed row
// rejects the whole request and nothing is written. The client then throws a
// PartialFailureError whose `message` is empty and whose real detail lives in
// `err.errors[]` — which is how a failure reached Slack as "sync failed: "
// with nothing after the colon. Two defences here:
//
//   1. coerceRows() reads the destination table's real schema and fixes the
//      values that BigQuery would reject outright. The case that broke the
//      2026-09-14 reservations sync: MTEK returns comma-joined IDs for
//      co-taught / substituted classes ("Instructor ID(s)" = "36107,104791"),
//      but that BigQuery column is INT64, so 9 bad rows rejected all 500 in
//      the batch. For multi-value IDs we keep the FIRST id (the full roster
//      still lives in the "Instructor Names" column); anything else that
//      cannot be coerced becomes NULL rather than failing the whole batch.
//      STRING columns are never touched, so the all-STRING Sales and
//      FirstTimers tables are unaffected.
//   2. summarizeInsertError() lifts the column, reason and offending value
//      out of err.errors[] and into the thrown message, so if a NEW kind of
//      bad value ever appears, both the Actions log and the Slack alert name
//      it instead of going silent.

const MAX_REPORTED_ROWS = 5;
const MAX_REPORTED_COERCIONS = 10;

// Candidate primary-key fields across the MTEK tables, best-effort, for
// pointing at the offending source record.
const ID_FIELDS = ["Reservation ID", "order_number", "Customer ID", "customer_id"];

function rowLabel(row) {
  for (const field of ID_FIELDS) {
    if (row?.[field] !== undefined && row?.[field] !== null) return `${field} ${row[field]}`;
  }
  return "unidentified row";
}

// ---------------------------------------------------------------- coercion

const INT_TYPES = new Set(["INTEGER", "INT64"]);
const FLOAT_TYPES = new Set(["FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"]);
const BOOL_TYPES = new Set(["BOOLEAN", "BOOL"]);

function coerceInt(value) {
  if (typeof value === "number") return Number.isInteger(value) ? value : Math.trunc(value);
  const text = String(value).trim();
  if (text === "") return { value: null, note: "empty -> NULL" };
  // MTEK sends comma-joined ids for co-taught / substituted classes.
  if (text.includes(",")) {
    const parts = text.split(",").map((p) => p.trim()).filter(Boolean);
    const first = Number(parts[0]);
    if (Number.isInteger(first)) {
      return { value: first, note: `multi-value ${JSON.stringify(text)} -> kept first id ${first}` };
    }
  }
  const n = Number(text);
  if (Number.isInteger(n)) return n;
  return { value: null, note: `${JSON.stringify(text)} is not an integer -> NULL` };
}

function coerceFloat(value) {
  if (typeof value === "number") return value;
  const text = String(value).trim().replace(/,/g, "");
  if (text === "") return { value: null, note: "empty -> NULL" };
  const n = Number(text);
  if (Number.isFinite(n)) return n;
  return { value: null, note: `${JSON.stringify(text)} is not a number -> NULL` };
}

function coerceBool(value) {
  if (typeof value === "boolean") return value;
  const text = String(value).trim().toLowerCase();
  if (text === "") return { value: null, note: "empty -> NULL" };
  if (["true", "t", "yes", "y", "1"].includes(text)) return true;
  if (["false", "f", "no", "n", "0"].includes(text)) return false;
  return { value: null, note: `${JSON.stringify(text)} is not a boolean -> NULL` };
}

function coerceDate(value) {
  if (value === "") return { value: null, note: "empty -> NULL" };
  const text = String(value);
  // Trim any time component; BigQuery DATE wants YYYY-MM-DD.
  const match = text.match(/^(\d{4}-\d{2}-\d{2})/);
  if (match) return match[1] === text ? text : { value: match[1], note: `${JSON.stringify(text)} -> ${match[1]}` };
  return { value: null, note: `${JSON.stringify(text)} is not an ISO date -> NULL` };
}

const COERCERS = [
  [INT_TYPES, coerceInt],
  [FLOAT_TYPES, coerceFloat],
  [BOOL_TYPES, coerceBool],
  [new Set(["DATE"]), coerceDate],
];

function coercerFor(type) {
  for (const [types, fn] of COERCERS) if (types.has(type)) return fn;
  return null; // STRING, TIMESTAMP, RECORD, ... left exactly as-is
}

export async function getFieldTypes(table) {
  const [metadata] = await table.getMetadata();
  const types = new Map();
  for (const field of metadata?.schema?.fields || []) {
    types.set(field.name, String(field.type || "").toUpperCase());
  }
  return types;
}

// Returns { rows, notes } — rows coerced in place-safe copies, notes is a
// human-readable list of every value this changed, so nothing is silent.
export function coerceRows(rows, fieldTypes) {
  const notes = [];
  if (!fieldTypes || fieldTypes.size === 0) return { rows, notes };

  const coerced = rows.map((row) => {
    let copy = null;
    for (const [column, value] of Object.entries(row)) {
      if (value === null || value === undefined) continue;
      const coerce = coercerFor(fieldTypes.get(column));
      if (!coerce) continue;
      const result = coerce(value);
      const newValue = result && typeof result === "object" && "value" in result ? result.value : result;
      if (newValue === value) continue;
      if (!copy) copy = { ...row };
      copy[column] = newValue;
      if (result?.note) notes.push(`${rowLabel(row)} — ${column}: ${result.note}`);
    }
    return copy || row;
  });

  return { rows: coerced, notes };
}

// ------------------------------------------------------------- diagnostics

function describeRowErrors(entry) {
  // entry: { row, errors: [{ reason, location, message, debugInfo }] }
  const row = entry?.row || {};
  return (entry?.errors || [])
    // "stopped" means the row was collateral damage of another row's failure,
    // not a cause — reporting those buries the real error.
    .filter((e) => e?.reason !== "stopped")
    .map((e) => {
      const column = e?.location || "(column not reported)";
      const value =
        column in row ? JSON.stringify(row[column]) : "(column not present in row)";
      return `${rowLabel(row)} — ${column}=${value} → ${e?.reason}: ${e?.message}`;
    });
}

export function summarizeInsertError(err, { batchNumber, batchRows } = {}) {
  const entries = Array.isArray(err?.errors) ? err.errors : [];
  const details = entries.flatMap(describeRowErrors);
  const where =
    batchNumber != null ? ` in batch ${batchNumber} (${batchRows} rows)` : "";

  if (!details.length) {
    const fallback =
      err?.message || `${entries.length} row error(s) with no detail returned`;
    return `BigQuery insert failed${where}: ${err?.name || "Error"}: ${fallback}`;
  }

  const shown = details.slice(0, MAX_REPORTED_ROWS);
  const hidden = details.length - shown.length;
  return [
    `BigQuery rejected ${entries.length} row(s)${where} — nothing was written ` +
      `(insertAll runs with skipInvalidRows=false, so one bad row fails the whole batch).`,
    ...shown.map((line) => `  • ${line}`),
    hidden > 0 ? `  • ...and ${hidden} more row error(s).` : null,
  ]
    .filter(Boolean)
    .join("\n");
}

// ------------------------------------------------------------------ insert

export async function insertInBatches(table, rows, batchSize = 500) {
  let fieldTypes = new Map();
  try {
    fieldTypes = await getFieldTypes(table);
  } catch (err) {
    // Non-fatal: without the schema we simply insert what we were given,
    // exactly as this function behaved before.
    console.warn(`Could not read destination schema (${err?.message || err}) — inserting uncoerced.`);
  }

  const { rows: safeRows, notes } = coerceRows(rows, fieldTypes);
  if (notes.length) {
    console.log(`Coerced ${notes.length} value(s) to match the table schema:`);
    for (const note of notes.slice(0, MAX_REPORTED_COERCIONS)) console.log(`  • ${note}`);
    if (notes.length > MAX_REPORTED_COERCIONS) {
      console.log(`  • ...and ${notes.length - MAX_REPORTED_COERCIONS} more.`);
    }
  }

  for (let i = 0; i < safeRows.length; i += batchSize) {
    const batch = safeRows.slice(i, i + batchSize);
    try {
      await table.insert(batch);
    } catch (err) {
      const summary = summarizeInsertError(err, {
        batchNumber: Math.floor(i / batchSize) + 1,
        batchRows: batch.length,
      });
      console.error(summary);
      // Re-throw with a populated message so callers that only log
      // err.message (and the Slack alert) surface the real cause.
      const wrapped = new Error(summary);
      wrapped.name = "BigQueryInsertError";
      wrapped.cause = err;
      throw wrapped;
    }
  }
}
