#!/usr/bin/env node
import process from 'node:process';

const {
  AIRTABLE_API_KEY,
  AIRTABLE_TOKEN,
  AIRTABLE_CUSTOMER_BASE_ID,
  AIRTABLE_BASE_ID,
} = process.env;

const apiKey = AIRTABLE_API_KEY || AIRTABLE_TOKEN;
if (!apiKey) {
  throw new Error('Missing env: AIRTABLE_API_KEY or AIRTABLE_TOKEN');
}

const baseId = AIRTABLE_CUSTOMER_BASE_ID || AIRTABLE_BASE_ID;
if (!baseId) {
  throw new Error('Missing env: AIRTABLE_CUSTOMER_BASE_ID or AIRTABLE_BASE_ID');
}

const TABLE_NAME = 'Class Reservations';
const VIEW_NAME = 'TO DELETE DO NOT TOUCH';
const AIRTABLE_BASE_URL = 'https://api.airtable.com/v0';

// Small safety guard – adjust if needed
const MAX_RECORDS_TO_DELETE = 50000;

async function fetchAllRecordIds() {
  const recordIds = [];
  let offset;

  console.log(
    `Fetching records from base=${baseId}, table="${TABLE_NAME}", view="${VIEW_NAME}"...`
  );

  do {
    const params = new URLSearchParams({
      view: VIEW_NAME,
      pageSize: '100',
    });

    if (offset) {
      params.set('offset', offset);
    }

    const url = `${AIRTABLE_BASE_URL}/${baseId}/${encodeURIComponent(
      TABLE_NAME
    )}?${params.toString()}`;

    const res = await fetch(url, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        Accept: 'application/json',
      },
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(
        `Error fetching records from Airtable: ${res.status} ${res.statusText} – ${text}`
      );
    }

    const data = await res.json();

    if (Array.isArray(data.records)) {
      for (const rec of data.records) {
        if (rec?.id) {
          recordIds.push(rec.id);
        }
      }
    }

    offset = data.offset;
  } while (offset);

  console.log(`Found ${recordIds.length} record(s) in the view to delete.`);
  return recordIds;
}

function chunk(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

async function deleteRecords(recordIds) {
  if (recordIds.length === 0) {
    console.log('No records to delete. Exiting.');
    return;
  }

  if (recordIds.length > MAX_RECORDS_TO_DELETE) {
    throw new Error(
      `Refusing to delete ${recordIds.length} records (limit ${MAX_RECORDS_TO_DELETE}). Check your view configuration.`
    );
  }

  const batches = chunk(recordIds, 10); // Airtable limit per delete call

  console.log(`Deleting records in ${batches.length} batch(es)...`);

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    const params = new URLSearchParams();
    for (const id of batch) {
      params.append('records[]', id);
    }

    const url = `${AIRTABLE_BASE_URL}/${baseId}/${encodeURIComponent(
      TABLE_NAME
    )}?${params.toString()}`;

    const res = await fetch(url, {
      method: 'DELETE',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        Accept: 'application/json',
      },
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(
        `Error deleting records (batch ${i + 1}/${batches.length}): ` +
          `${res.status} ${res.statusText} – ${text}`
      );
    }

    const data = await res.json();
    const deletedCount = Array.isArray(data.records) ? data.records.length : 0;

    console.log(
      `Batch ${i + 1}/${batches.length} deleted ${deletedCount} record(s).`
    );
  }

  console.log('All requested records deleted successfully.');
}

async function main() {
  try {
    const ids = await fetchAllRecordIds();
    await deleteRecords(ids);
  } catch (err) {
    console.error('❌ Failed to clear Airtable view:', err);
    process.exitCode = 1;
  }
}

await main();
