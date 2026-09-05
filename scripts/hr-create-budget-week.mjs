#!/usr/bin/env node

/**
 * HR Create Budget week
 * Creates the upcoming payroll budget week in the HR base.
 *
 * Start Date = the coming Sunday (running on a Sunday yields the following
 * one, never today). End Date = Start + 6, so the week runs Sunday-Saturday.
 */

const CONFIG = {
  airtable: {
    baseId: process.env.AIRTABLE_BASE_ID || 'appiwfeujJzUZPPBx',
    budgetWeeksTableId: process.env.AIRTABLE_BUDGET_WEEKS_TABLE_ID || 'tblt2pfs356rDVDIa',
    token: process.env.AIRTABLE_TOKEN,
  },
  timeZone: process.env.PAYROLL_TIME_ZONE || 'America/Toronto',
};

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

function budgetWeekDates(now = new Date()) {
  const todayLocal = new Intl.DateTimeFormat('en-CA', {
    timeZone: CONFIG.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);

  const base = new Date(`${todayLocal}T00:00:00Z`);

  let daysAhead = (7 - base.getUTCDay()) % 7;
  if (daysAhead === 0) daysAhead = 7;

  const start = new Date(base);
  start.setUTCDate(start.getUTCDate() + daysAhead);

  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 6);

  return { startDate: isoDate(start), endDate: isoDate(end) };
}

async function airtableRequest({ method = 'GET', body, query = '' }) {
  const base = `https://api.airtable.com/v0/${CONFIG.airtable.baseId}/${CONFIG.airtable.budgetWeeksTableId}`;
  const response = await fetch(query ? `${base}?${query}` : base, {
    method,
    headers: {
      Authorization: `Bearer ${CONFIG.airtable.token}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Airtable ${method} failed (${response.status}): ${text}`);
  }

  return response.json();
}

async function run() {
  if (!CONFIG.airtable.token) {
    throw new Error('Missing required environment variable: AIRTABLE_TOKEN');
  }

  const { startDate, endDate } = budgetWeekDates();

  const params = new URLSearchParams({
    filterByFormula: `DATETIME_FORMAT({Start Date}, 'YYYY-MM-DD') = '${startDate}'`,
    maxRecords: '1',
  });
  const existing = await airtableRequest({ query: params.toString() });

  if (existing.records?.length) {
    console.log(`Budget week starting ${startDate} already exists (${existing.records[0].id}). Nothing to do.`);
    return;
  }

  const created = await airtableRequest({
    method: 'POST',
    body: { records: [{ fields: { 'Start Date': startDate, 'End Date': endDate } }] },
  });

  console.log(`Created Budget week ${startDate} -> ${endDate} (${created.records[0].id}).`);
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
