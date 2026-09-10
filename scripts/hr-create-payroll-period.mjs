#!/usr/bin/env node

/**
 * HR Create Payroll period
 * Keeps the two-week Payroll period table (HR base) filled ahead of time, and links
 * every Time Punch that has no pay period to the one covering its date.
 *
 * Periods run Sunday to Saturday, 14 days each, stepping from the latest period
 * already in the table — the table is the reference, so the first one was seeded by
 * hand (2026-08-23 -> 2026-09-05). Runs every Sunday: on the Sunday a period starts
 * it creates the one two weeks out, and on the off-Sunday there is nothing to do.
 * Running weekly rather than fortnightly means a skipped GitHub cron catches up.
 */

const CONFIG = {
  baseId: process.env.AIRTABLE_BASE_ID || 'appiwfeujJzUZPPBx',
  periodsTableId: process.env.AIRTABLE_PAYROLL_PERIODS_TABLE_ID || 'tbl9qw4kqw0BY0DyJ',
  punchesTableId: process.env.AIRTABLE_PUNCHES_TABLE_ID || 'tblVxt2W7NanQmJFR',
  token: process.env.AIRTABLE_TOKEN,
  timeZone: process.env.PAYROLL_TIME_ZONE || 'America/Toronto',
};

// Field IDs rather than names, so renaming a column in Airtable can't break this.
const FIELD = {
  periodName: 'fldF5VDAT9leanu8m',
  periodStart: 'fldMjpx7WN4tgNrNs',
  periodEnd: 'fldCy4qUWOWq6SZdk',
  punchDate: 'fldwxo5JqKNOf4KvY',
  punchPeriod: 'fld4XJAYoen5lCaaM',
};

const PERIOD_DAYS = 14;

function addDays(isoDate, days) {
  const date = new Date(`${isoDate}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function todayLocal(now = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: CONFIG.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);
}

/**
 * Periods to add after `latestStart`, up to any that start within the next two
 * weeks. On the Sunday a period begins this yields exactly the next one; on the
 * off-Sunday it yields nothing; after a missed run it yields what was skipped.
 */
function periodsToCreate(latestStart, today) {
  const horizon = addDays(today, PERIOD_DAYS);
  const periods = [];
  for (let start = addDays(latestStart, PERIOD_DAYS); start <= horizon; start = addDays(start, PERIOD_DAYS)) {
    periods.push({ start, end: addDays(start, PERIOD_DAYS - 1) });
  }
  return periods;
}

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

async function fetchAll(tableId, fieldIds) {
  const records = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ returnFieldsByFieldId: 'true', pageSize: '100' });
    fieldIds.forEach((id) => params.append('fields[]', id));
    if (offset) params.set('offset', offset);
    const page = await airtableRequest({ tableId, query: params.toString() });
    records.push(...(page.records || []));
    offset = page.offset || '';
  } while (offset);
  return records;
}

async function writeInBatches(method, tableId, records) {
  const written = [];
  for (let i = 0; i < records.length; i += 10) {
    const page = await airtableRequest({
      method,
      tableId,
      body: { records: records.slice(i, i + 10) },
    });
    written.push(...(page.records || []));
  }
  return written;
}

async function run() {
  if (!CONFIG.token) throw new Error('Missing required environment variable: AIRTABLE_TOKEN');

  const periods = (await fetchAll(CONFIG.periodsTableId, [FIELD.periodStart, FIELD.periodEnd]))
    .map((record) => ({
      id: record.id,
      start: record.fields?.[FIELD.periodStart],
      end: record.fields?.[FIELD.periodEnd],
    }))
    .filter((period) => period.start && period.end);

  if (!periods.length) {
    throw new Error(
      'The Payroll period table is empty. Seed the first period by hand — every later period is stepped from the latest one.',
    );
  }

  const latestStart = periods.map((period) => period.start).sort().at(-1);
  const toCreate = periodsToCreate(latestStart, todayLocal());

  const created = await writeInBatches(
    'POST',
    CONFIG.periodsTableId,
    toCreate.map(({ start, end }) => ({
      fields: {
        [FIELD.periodName]: `${start} -> ${end}`,
        [FIELD.periodStart]: start,
        [FIELD.periodEnd]: end,
      },
    })),
  );
  toCreate.forEach((period, i) => periods.push({ id: created[i].id, ...period }));

  // Punches normally get their period at import; this sweeps up anything that
  // didn't (imported before the table existed, or added by hand).
  const punches = await fetchAll(CONFIG.punchesTableId, [FIELD.punchDate, FIELD.punchPeriod]);
  const updates = [];
  const uncovered = new Set();
  const ambiguous = new Set();

  for (const punch of punches) {
    const date = punch.fields?.[FIELD.punchDate];
    if (!date || (punch.fields?.[FIELD.punchPeriod] || []).length) continue;

    const matches = periods.filter((period) => period.start <= date && date <= period.end);
    if (matches.length === 1) updates.push({ id: punch.id, fields: { [FIELD.punchPeriod]: [matches[0].id] } });
    else (matches.length ? ambiguous : uncovered).add(date);
  }

  await writeInBatches('PATCH', CONFIG.punchesTableId, updates);

  const createdNames = toCreate.map(({ start, end }) => `${start} -> ${end}`);
  console.log(
    `Created ${toCreate.length} Payroll period(s)${createdNames.length ? `: ${createdNames.join(', ')}` : ''}. ` +
      `Linked ${updates.length} time punch(es) to a period.`,
  );

  if (uncovered.size || ambiguous.size) {
    const problems = [];
    if (uncovered.size) problems.push(`no period covers ${[...uncovered].sort().join(', ')}`);
    if (ambiguous.size) problems.push(`more than one period covers ${[...ambiguous].sort().join(', ')}`);
    throw new Error(`Some time punches could not be given a pay period: ${problems.join('; ')}.`);
  }
}

// Runs whenever this file is executed. HR_PAYROLL_PERIOD_SKIP_RUN=1 lets the date
// logic be imported and exercised on its own.
if (process.env.HR_PAYROLL_PERIOD_SKIP_RUN !== '1') {
  run().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export { periodsToCreate };
