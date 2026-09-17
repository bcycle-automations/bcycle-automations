#!/usr/bin/env node

/**
 * HR Create EOM
 * On the 15th of each month, creates next month's EOM (End of Month) record in
 * the HR base: Start = the 1st, End = the last day of that month.
 *
 * EOM lives in HR (tbl3UMRShm59z41JL) and is synced into HR - Instructors, where
 * instructor classes are assigned to it by the PAYROLL Classes job. Creating it
 * about two weeks ahead means no class fetch — even a week straddling month end,
 * or one run early by hand — can reach a month that doesn't exist yet.
 */

const CONFIG = {
  baseId: process.env.AIRTABLE_BASE_ID || 'appiwfeujJzUZPPBx',
  eomTableId: process.env.AIRTABLE_EOM_TABLE_ID || 'tbl3UMRShm59z41JL',
  token: process.env.AIRTABLE_TOKEN,
  timeZone: process.env.PAYROLL_TIME_ZONE || 'America/Toronto',
};

// Field IDs rather than names, so renaming a column in Airtable can't break this.
// Name is a formula in Airtable, so only the dates are written.
const FIELD = { start: 'fldduMMEtqShh6CPB', end: 'fld8fIYiRFAdcwN1l' };

// A guard, not a schedule: filling a long gap of months in one run is far more
// likely a mistake than a real backlog.
const MAX_NEW_MONTHS = 3;

/** Today's date in the payroll time zone, as YYYY-MM-DD. */
function localToday(now = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: CONFIG.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);
}

/** First and last day of the month `offset` months after `date`'s month. */
function monthWindow(date, offset = 0) {
  const [year, month] = date.split('-').map(Number);
  const start = new Date(Date.UTC(year, month - 1 + offset, 1));
  const end = new Date(Date.UTC(year, month + offset, 0));
  return { start: start.toISOString().slice(0, 10), end: end.toISOString().slice(0, 10) };
}

/** The months that should exist: every month from the earliest EOM through next month. */
function monthsNeeded(today, existingStarts) {
  const wanted = [];
  const next = monthWindow(today, 1);
  // Walk back from next month until a month already exists, so a missed run is
  // filled in rather than skipped. Nothing before the first EOM is invented.
  for (let offset = 1; offset >= -MAX_NEW_MONTHS; offset -= 1) {
    const window = monthWindow(today, offset);
    if (existingStarts.includes(window.start)) break;
    wanted.push(window);
    if (!existingStarts.length) break;
  }
  return { next, wanted: wanted.reverse() };
}

async function airtableRequest({ method = 'GET', body, query = '' }) {
  const base = `https://api.airtable.com/v0/${CONFIG.baseId}/${CONFIG.eomTableId}`;
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

async function fetchExistingStarts() {
  const starts = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ returnFieldsByFieldId: 'true', pageSize: '100' });
    params.append('fields[]', FIELD.start);
    if (offset) params.set('offset', offset);
    const page = await airtableRequest({ query: params.toString() });
    for (const record of page.records || []) {
      if (record.fields?.[FIELD.start]) starts.push(record.fields[FIELD.start]);
    }
    offset = page.offset || '';
  } while (offset);
  return starts;
}

async function run() {
  if (!CONFIG.token) throw new Error('Missing required environment variable: AIRTABLE_TOKEN');

  const today = localToday();
  const existingStarts = await fetchExistingStarts();
  const { wanted } = monthsNeeded(today, existingStarts);

  if (wanted.length > MAX_NEW_MONTHS) {
    throw new Error(
      `Refusing to create ${wanted.length} EOM records in one run (${wanted[0].start} .. ${wanted.at(-1).start}) — that looks like a mistake, not a backlog.`,
    );
  }

  if (!wanted.length) {
    console.log(`HR Create EOM: nothing to do on ${today}; next month already exists.`);
    return;
  }

  const created = await airtableRequest({
    method: 'POST',
    body: {
      records: wanted.map(({ start, end }) => ({ fields: { [FIELD.start]: start, [FIELD.end]: end } })),
      returnFieldsByFieldId: true,
    },
  });

  console.log(
    `HR Create EOM: created ${created.records.length} EOM record(s): ${wanted
      .map((month) => `${month.start} -> ${month.end}`)
      .join(', ')}.`,
  );
}

// Runs whenever this file is executed. HR_EOM_SKIP_RUN=1 lets the date logic be
// imported and exercised on its own.
if (process.env.HR_EOM_SKIP_RUN !== '1') {
  run().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export { monthWindow, monthsNeeded, localToday };
