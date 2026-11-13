#!/usr/bin/env node

/**
 * HR Backfill class rate lock
 * Snapshots each instructor's current Base / Spin / Barre / Strength rates onto
 * existing "Classes (for Payroll)" rows and ticks "Rates locked", so a later raise
 * never changes a class that is already imported.
 *
 * Only touches classes that have an Instructor and are not yet locked. The values
 * copied are the lookups the class already shows, so pay does not change today.
 * Read-only unless DRY_RUN is "false". Logs counts only (public Actions log).
 */

const CONFIG = {
  baseId: process.env.AIRTABLE_BASE_ID || 'appBC0Ja4B5LKbZLW',
  classesTableId: process.env.AIRTABLE_CLASSES_TABLE_ID || 'tbl8RbWysEFdNuz37',
  token: process.env.AIRTABLE_TOKEN,
  dryRun: String(process.env.DRY_RUN ?? 'true').toLowerCase() !== 'false',
};

const FIELD = {
  instructor: 'fldHYrp33qRYiQTqJ',
  locked: 'fldBKpxm5CUh0Umhc',
  // [live lookup on the class, locked snapshot field]
  rates: [
    ['fldZC5hNJMyOA2HNf', 'fldIL5ulkomlGWE5I'], // Base
    ['flduEDzh6FBBxwlH6', 'fldP135DqKrvwxUxk'], // Spin bonus
    ['fldtt5wrACiSGYvGF', 'fldWvFTSOxEIkvo3d'], // Barre bonus
    ['fldaMvJ4xn7GnqyCs', 'fldfDJGprjNrtjXDs'], // Strength bonus
  ],
};

async function airtableRequest({ method = 'GET', body, query = '' }) {
  const base = `https://api.airtable.com/v0/${CONFIG.baseId}/${CONFIG.classesTableId}`;
  const response = await fetch(query ? `${base}?${query}` : base, {
    method,
    headers: { Authorization: `Bearer ${CONFIG.token}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) {
    throw new Error(`Airtable ${method} failed (${response.status}): ${await response.text()}`);
  }
  return response.json();
}

async function fetchUnlocked() {
  const fieldIds = [FIELD.instructor, FIELD.locked, ...FIELD.rates.map(([live]) => live)];
  const records = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ returnFieldsByFieldId: 'true', pageSize: '100' });
    fieldIds.forEach((id) => params.append('fields[]', id));
    params.set('filterByFormula', `AND({${FIELD.instructor}}, NOT({${FIELD.locked}}))`);
    if (offset) params.set('offset', offset);
    const page = await airtableRequest({ query: params.toString() });
    records.push(...(page.records || []));
    offset = page.offset || '';
  } while (offset);
  return records;
}

// Lookups come back as arrays; a blank one is stored as 0.
function numberFrom(value) {
  const first = Array.isArray(value) ? value[0] : value;
  return typeof first === 'number' ? first : 0;
}

async function run() {
  if (!CONFIG.token) throw new Error('Missing required environment variable: AIRTABLE_TOKEN');

  const classes = await fetchUnlocked();
  const updates = classes.map((record) => {
    const fields = { [FIELD.locked]: true };
    for (const [live, snapshot] of FIELD.rates) fields[snapshot] = numberFrom(record.fields?.[live]);
    return { id: record.id, fields };
  });
  const noBase = classes.filter((record) => numberFrom(record.fields?.[FIELD.rates[0][0]]) === 0).length;

  console.log(`Classes with an instructor and not yet locked: ${updates.length} | of those with no Base Rate: ${noBase}`);

  if (CONFIG.dryRun) {
    console.log('DRY_RUN: nothing was written. Set DRY_RUN=false to apply.');
    return;
  }
  for (let i = 0; i < updates.length; i += 10) {
    await airtableRequest({ method: 'PATCH', body: { records: updates.slice(i, i + 10) } });
  }
  console.log(`Locked rates on ${updates.length} class(es).`);
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
