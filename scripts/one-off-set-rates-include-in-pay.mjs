#!/usr/bin/env node

/**
 * One-off: seed "Include in pay? - New" on every Rates row in the HR base.
 *
 * Delete this script and its workflow once it has been run.
 */

const CONFIG = {
  baseId: 'appiwfeujJzUZPPBx',
  ratesTableId: 'tblufK9k5Tg5uCd74',
  includeInPayNewFieldId: 'fldaLetbJS8n8USHH',
  value: process.env.SET_VALUE || 'No',
  token: process.env.AIRTABLE_TOKEN,
  dryRun: String(process.env.DRY_RUN || 'true').toLowerCase() !== 'false',
};

async function airtable({ method = 'GET', body, query = '' }) {
  const url = `https://api.airtable.com/v0/${CONFIG.baseId}/${CONFIG.ratesTableId}${query ? `?${query}` : ''}`;
  const response = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${CONFIG.token}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    throw new Error(`Airtable ${method} failed (${response.status}): ${await response.text()}`);
  }

  return response.json();
}

async function run() {
  if (!CONFIG.token) throw new Error('Missing AIRTABLE_TOKEN');

  const records = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ pageSize: '100' });
    params.append('fields[]', CONFIG.includeInPayNewFieldId);
    if (offset) params.set('offset', offset);

    const page = await airtable({ query: params.toString() });
    records.push(...(page.records || []));
    offset = page.offset || '';
  } while (offset);

  const needsUpdate = records.filter(
    (record) => record.fields?.[CONFIG.includeInPayNewFieldId] !== CONFIG.value,
  );

  console.log(`Rates rows: ${records.length}`);
  console.log(`Already "${CONFIG.value}": ${records.length - needsUpdate.length}`);
  console.log(`To update: ${needsUpdate.length}`);

  if (CONFIG.dryRun) {
    console.log('DRY RUN — nothing written. Set DRY_RUN=false to apply.');
    return;
  }

  let updated = 0;
  for (let i = 0; i < needsUpdate.length; i += 10) {
    const batch = needsUpdate.slice(i, i + 10);
    await airtable({
      method: 'PATCH',
      body: {
        records: batch.map((record) => ({
          id: record.id,
          fields: { [CONFIG.includeInPayNewFieldId]: CONFIG.value },
        })),
      },
    });
    updated += batch.length;
  }

  console.log(`Updated ${updated} rows to "${CONFIG.value}".`);
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
