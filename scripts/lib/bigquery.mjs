// scripts/lib/bigquery.mjs
// BigQuery's tabledata.insertAll has a ~10MB request size limit — a single
// large chunk (e.g. thousands of wide reservation rows) can exceed that and
// fail with HTTP 413. Insert in fixed-size batches instead of one call.
//
// insertAll also runs with skipInvalidRows=false, so a SINGLE malformed row
// rejects the whole request and nothing is written. The client then throws a
// PartialFailureError whose `message` is empty and whose real detail lives in
// `err.errors[]` — which is how a failure reached Slack as "sync failed: "
// with nothing after the colon. summarizeInsertError() lifts the column,
// reason and offending value out of that structure and into the thrown
// message, so both the Actions log and the Slack alert name the bad data.

const MAX_REPORTED_ROWS = 5;

// Candidate primary-key fields across the MTEK tables, best-effort, for
// pointing at the offending source record.
const ID_FIELDS = ["Reservation ID", "order_number", "Customer ID", "customer_id"];

function rowLabel(row) {
  for (const field of ID_FIELDS) {
    if (row?.[field] !== undefined && row?.[field] !== null) return `${field} ${row[field]}`;
  }
  return "unidentified row";
}

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

export async function insertInBatches(table, rows, batchSize = 500) {
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
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
