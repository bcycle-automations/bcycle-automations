#!/usr/bin/env node

/**
 * HR Backfill class EOM
 * Gives existing "Classes (for Payroll)" rows their EOM link. The PAYROLL
 * Classes sync assigns an EOM as classes are imported, so this is only for rows
 * created before that existed — or to rebuild links after an EOM is corrected.
 *
 * Classes dated before the first EOM are left alone. Runs read-only unless
 * DRY_RUN is "false".
 */

const CONFIG = {
  baseId: process.env.AIRTABLE_BASE_ID || 'appBC0Ja4B5LKbZLW',
  classesTableId: process.env.AIRTABLE_CLASSES_TABLE_ID || 'tbl8RbWysEFdNuz37',
  eomTableId: process.env.AIRTABLE_EOM_TABLE_ID || 'tbliX73kdfgme7r3R',
  token: process.env.AIRTABLE_TOKEN,
  timeZone: process.env.PAYROLL_TIME_ZONE || 'America/Toronto',
  dryRun: String(process.env.DRY_RUN ?? 'true').toLowerCase() !== 'false',
};

const FIELD = {
  classDate: 'fldBkXtX4DU9n5scl',
  classEom: 'flds49hl8DluvKyGk',
  eomStart: 'fldwo53Yx2PI2VVVJ',
  eomEnd: 'fldt7m3KygcrA7GZf',
};

async function airtableRequest({ method = 'GET', tableId, body, query = '' }) {
  const base = `https://api.airtable.com/v0/${CONFIG.baseId}/${tableId}`;
  const response = await fetch(query ? `${base}?${query}` : base, {
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

async function fetchAll(tableId, fieldIds, filterByFormula = '') {
  const records = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ returnFieldsByFieldId: 'true', pageSize: '100' });
    fieldIds.forEach((id) => params.append('fields[]', id));
    if (filterByFormula) params.set('filterByFormula', filterByFormula);
    if (offset) params.set('offset', offset);
    const page = await airtableRequest({ tableId, query: params.toString() });
    records.push(...(page.records || []));
    offset = page.offset || '';
  } while (offset);
  return records;
}

/** The class's date as the studio sees it, so a late class stays in its own month. */
function localDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: CONFIG.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

async function run() {
  if (!CONFIG.token) throw new Error('Missing required environment variable: AIRTABLE_TOKEN');

  const months = (await fetchAll(CONFIG.eomTableId, [FIELD.eomStart, FIELD.eomEnd]))
    .map((record) => ({ id: record.id, start: record.fields?.[FIELD.eomStart], end: record.fields?.[FIELD.eomEnd] }))
    .filter((month) => month.start && month.end);
  if (!months.length) throw new Error('The EOM table is empty — nothing to assign.');
  const earliestStart = months.map((month) => month.start).sort()[0];

  const classes = await fetchAll(
    CONFIG.classesTableId,
    [FIELD.classDate, FIELD.classEom],
    `NOT({${FIELD.classEom}})`,
  );

  const updates = [];
  const problems = [];
  let beforeFirstEom = 0;
  let noDate = 0;

  for (const record of classes) {
    const raw = record.fields?.[FIELD.classDate];
    const date = raw ? localDate(raw) : null;
    if (!date) {
      noDate += 1;
      continue;
    }
    if (date < earliestStart) {
      beforeFirstEom += 1;
      continue;
    }
    const matches = months.filter((month) => month.start <= date && date <= month.end);
    if (matches.length === 1) updates.push({ id: record.id, fields: { [FIELD.classEom]: [matches[0].id] } });
    else problems.push(`${date}: ${matches.length ? 'more than one EOM covers it' : 'no EOM covers it'}`);
  }

  console.log(
    `Classes without an EOM: ${classes.length} | to assign: ${updates.length} | before the first EOM (${earliestStart}): ${beforeFirstEom} | no date: ${noDate} | problems: ${problems.length}`,
  );

  if (CONFIG.dryRun) {
    console.log('DRY_RUN: nothing was written. Set DRY_RUN=false to apply.');
  } else {
    for (let i = 0; i < updates.length; i += 10) {
      await airtableRequest({
        method: 'PATCH',
        tableId: CONFIG.classesTableId,
        body: { records: updates.slice(i, i + 10) },
      });
    }
    console.log(`Assigned an EOM to ${updates.length} class(es).`);
  }

  if (problems.length) {
    const unique = [...new Set(problems)].sort();
    throw new Error(`Some classes could not be placed in an EOM:\n- ${unique.slice(0, 25).join('\n- ')}`);
  }
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
