#!/usr/bin/env node

/**
 * HR Create Budget week
 * Every Sunday: creates the upcoming Budget week, assigns it — and any week missing
 * one — to its two-week Payroll period, and links any time punch without a period.
 *
 * Budget week: Start = the coming Sunday (running on a Sunday gives the following
 * one), End = Start + 6, so weeks run Sunday-Saturday.
 *
 * Payroll period: 14 days, Sunday-Saturday, stepped from the latest period in the
 * table (the first was seeded by hand: 2026-08-23 -> 2026-09-05). A period is only
 * created when a week needs it — never speculatively ahead — so it appears one week
 * before it starts, when its first Budget week does.
 */

const CONFIG = {
  baseId: process.env.AIRTABLE_BASE_ID || 'appiwfeujJzUZPPBx',
  budgetWeeksTableId: process.env.AIRTABLE_BUDGET_WEEKS_TABLE_ID || 'tblt2pfs356rDVDIa',
  periodsTableId: process.env.AIRTABLE_PAYROLL_PERIODS_TABLE_ID || 'tbl9qw4kqw0BY0DyJ',
  punchesTableId: process.env.AIRTABLE_PUNCHES_TABLE_ID || 'tblVxt2W7NanQmJFR',
  token: process.env.AIRTABLE_TOKEN,
  timeZone: process.env.PAYROLL_TIME_ZONE || 'America/Toronto',
};

// Field IDs rather than names, so renaming a column in Airtable can't break this.
const FIELD = {
  weekStart: 'fldkF8NgX28kgbuAl',
  weekEnd: 'fldxp99UioLif2Foy',
  weekPeriod: 'fld73OutMIP1NYSOe',
  weekNumber: 'fldf9En87sIO8JfIO',
  periodStart: 'fldMjpx7WN4tgNrNs',
  periodEnd: 'fldCy4qUWOWq6SZdk',
  punchDate: 'fldwxo5JqKNOf4KvY',
  punchPeriod: 'fld4XJAYoen5lCaaM',
};

const PERIOD_DAYS = 14;
// A guard, not a schedule: a week several periods past the latest one is far more
// likely a mistyped date than a real week, so it fails instead of filling the gap.
const MAX_NEW_PERIODS = 3;

function addDays(isoDate, days) {
  const date = new Date(`${isoDate}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function daysBetween(from, to) {
  return Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000);
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

  const startDate = addDays(todayLocal, daysAhead);
  return { startDate, endDate: addDays(startDate, 6) };
}

/** Periods to add after the latest one so that one of them covers `date`. */
function periodsNeededFor(date, latestStart) {
  const periods = [];
  let start = latestStart;
  while (addDays(start, PERIOD_DAYS - 1) < date) {
    start = addDays(start, PERIOD_DAYS);
    periods.push({ start, end: addDays(start, PERIOD_DAYS - 1) });
  }
  return periods;
}

/** 1 or 2: which week of its pay period a week starting on `weekStart` is. */
function weekOfPeriod(weekStart, periodStart) {
  return Math.floor(daysBetween(periodStart, weekStart) / 7) + 1;
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
    const page = await airtableRequest({ method, tableId, body: { records: records.slice(i, i + 10) } });
    written.push(...(page.records || []));
  }
  return written;
}

async function run() {
  if (!CONFIG.token) throw new Error('Missing required environment variable: AIRTABLE_TOKEN');

  const periods = (await fetchAll(CONFIG.periodsTableId, [FIELD.periodStart, FIELD.periodEnd]))
    .map((r) => ({ id: r.id, start: r.fields?.[FIELD.periodStart], end: r.fields?.[FIELD.periodEnd] }))
    .filter((p) => p.start && p.end);

  if (!periods.length) {
    throw new Error(
      'The Payroll period table is empty. Seed the first period by hand — every later period is stepped from the latest one.',
    );
  }
  const earliestStart = periods.map((p) => p.start).sort()[0];
  const latestStart = periods.map((p) => p.start).sort().at(-1);

  // 1. The coming Budget week.
  const weeks = (await fetchAll(CONFIG.budgetWeeksTableId, [FIELD.weekStart, FIELD.weekEnd, FIELD.weekPeriod, FIELD.weekNumber]))
    .map((r) => ({
      id: r.id,
      start: r.fields?.[FIELD.weekStart],
      end: r.fields?.[FIELD.weekEnd],
      periodIds: r.fields?.[FIELD.weekPeriod] || [],
      number: r.fields?.[FIELD.weekNumber] ?? null,
    }))
    .filter((w) => w.start && w.end);

  const { startDate, endDate } = budgetWeekDates();
  let createdWeek = false;
  if (!weeks.some((w) => w.start === startDate)) {
    const [record] = await writeInBatches('POST', CONFIG.budgetWeeksTableId, [
      { fields: { [FIELD.weekStart]: startDate, [FIELD.weekEnd]: endDate } },
    ]);
    weeks.push({ id: record.id, start: startDate, end: endDate, periodIds: [], number: null });
    createdWeek = true;
  }

  // 2. Every week from the first pay period on belongs to exactly one — the week
  //    just created and any made by hand. Periods are created only for weeks that
  //    need them, never ahead of that. Weeks before the first period are ignored.
  const relevantWeeks = weeks.filter((w) => w.start >= earliestStart);
  const furthestStart = relevantWeeks.map((w) => w.start).sort().at(-1);
  const newPeriods = furthestStart ? periodsNeededFor(furthestStart, latestStart) : [];

  if (newPeriods.length > MAX_NEW_PERIODS) {
    throw new Error(
      `A Budget week starting ${furthestStart} is ${newPeriods.length} pay periods past the latest one (${latestStart}) — probably a mistyped date. No periods were created.`,
    );
  }

  // Name is a formula in Airtable, so only the dates are written.
  const createdPeriods = await writeInBatches(
    'POST',
    CONFIG.periodsTableId,
    newPeriods.map(({ start, end }) => ({ fields: { [FIELD.periodStart]: start, [FIELD.periodEnd]: end } })),
  );
  newPeriods.forEach((p, i) => periods.push({ id: createdPeriods[i].id, ...p }));

  const weekUpdates = [];
  const problems = [];
  for (const week of relevantWeeks) {
    const matches = periods.filter((p) => p.start <= week.start && week.end <= p.end);
    if (matches.length !== 1) {
      problems.push(`Budget week ${week.start} -> ${week.end}: ${matches.length ? 'more than one pay period covers it' : 'no single pay period covers it'}`);
      continue;
    }
    const [period] = matches;
    const number = weekOfPeriod(week.start, period.start);
    // Re-checked every run, so a week relinked by hand is put back on its real period.
    if (week.periodIds.length !== 1 || week.periodIds[0] !== period.id || week.number !== number) {
      weekUpdates.push({ id: week.id, fields: { [FIELD.weekPeriod]: [period.id], [FIELD.weekNumber]: number } });
    }
  }
  await writeInBatches('PATCH', CONFIG.budgetWeeksTableId, weekUpdates);

  // 3. Punches get their period at import; this catches any that didn't. It links
  //    only to periods that exist — a punch never causes a period to be created.
  const punchUpdates = [];
  for (const punch of await fetchAll(CONFIG.punchesTableId, [FIELD.punchDate, FIELD.punchPeriod])) {
    const date = punch.fields?.[FIELD.punchDate];
    if (!date || date < earliestStart || (punch.fields?.[FIELD.punchPeriod] || []).length) continue;
    const matches = periods.filter((p) => p.start <= date && date <= p.end);
    if (matches.length === 1) punchUpdates.push({ id: punch.id, fields: { [FIELD.punchPeriod]: [matches[0].id] } });
    else problems.push(`Time punch ${punch.id} on ${date}: ${matches.length ? 'more than one pay period covers it' : 'no pay period covers it'}`);
  }
  await writeInBatches('PATCH', CONFIG.punchesTableId, punchUpdates);

  console.log(
    [
      createdWeek ? `Created Budget week ${startDate} -> ${endDate}.` : `Budget week ${startDate} already existed.`,
      `Created ${newPeriods.length} pay period(s)${newPeriods.length ? `: ${newPeriods.map((p) => `${p.start} -> ${p.end}`).join(', ')}` : ''}.`,
      `Assigned or corrected ${weekUpdates.length} Budget week(s).`,
      `Linked ${punchUpdates.length} time punch(es) to a pay period.`,
    ].join(' '),
  );

  if (problems.length) {
    throw new Error(`Could not place everything in a pay period:\n- ${problems.join('\n- ')}`);
  }
}

// Runs whenever this file is executed. HR_BUDGET_WEEK_SKIP_RUN=1 lets the date logic
// be imported and exercised on its own.
if (process.env.HR_BUDGET_WEEK_SKIP_RUN !== '1') {
  run().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export { budgetWeekDates, periodsNeededFor, weekOfPeriod };
