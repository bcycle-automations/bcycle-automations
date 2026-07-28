// scripts/lib/bigquery.mjs
// BigQuery's tabledata.insertAll has a ~10MB request size limit — a single
// large chunk (e.g. thousands of wide reservation rows) can exceed that and
// fail with HTTP 413. Insert in fixed-size batches instead of one call.

export async function insertInBatches(table, rows, batchSize = 500) {
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    await table.insert(batch);
  }
}
