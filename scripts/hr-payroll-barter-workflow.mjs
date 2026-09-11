#!/usr/bin/env node

/**
 * HR Payroll Barter
 * MarianaTek "Promotion Redemptions" report (id 292) -> Airtable "Barter" sync.
 *
 * Runs against the HR base (NOT HR - Instructors). Driven by one Payroll period
 * record, which supplies the date window. Only the BARTER promotion is imported.
 */

import { fetchMtekReport } from './lib/mtek-report.mjs';

const CONFIG = {
  airtable: {
    baseId: process.env.AIRTABLE_BASE_ID || 'appiwfeujJzUZPPBx',
    periodsTableId: process.env.AIRTABLE_PAYROLL_PERIODS_TABLE_ID || 'tbl9qw4kqw0BY0DyJ',
    barterTableId: process.env.AIRTABLE_BARTER_TABLE_ID || 'tblYxeSSem1plIvIR',
    employeesTableId: process.env.AIRTABLE_EMPLOYEES_TABLE_ID || 'tbl0FzJ2s4Mk5jWIi',
    // "Active Employees - ALL" — the full table holds former staff and duplicated
    // names, which is how the time punch sync once matched people to stale records.
    employeesViewId: process.env.AIRTABLE_EMPLOYEES_VIEW_ID || 'viws8tSbvXfujLnwG',
    token: process.env.AIRTABLE_TOKEN,
  },
  mtek: {
    baseUrl: process.env.MTEK_BASE_URL || 'https://bcycle.marianatek.com',
    reportId: process.env.MTEK_BARTER_REPORT_ID || '292',
    reportSlug: process.env.MTEK_BARTER_REPORT_SLUG || 'promotion-redemptions',
    token: process.env.MTEK_API_TOKEN,
  },
  promotion: process.env.BARTER_PROMOTION || 'BARTER',
  timeZone: process.env.PAYROLL_TIME_ZONE || 'America/Toronto',
  recordId: process.env.AIRTABLE_RECORD_ID,
};

// Field IDs, so renaming a column in Airtable can't break the sync.
const PERIOD = {
  start: 'fldMjpx7WN4tgNrNs',
  end: 'fldCy4qUWOWq6SZdk',
  status: 'fldbyYPApgtlly5os',
  employeeStatus: 'fldnD89m4dqCjvLQ1',
  overallStatus: 'fldgcIhMaf0hgk1EE',
  notes: 'fldWxs9OlW2FsncnI',
};
const BARTER = {
  orderNumber: 'fldKGp4HhI9g0GQu5',
  period: 'fldMD49SlLilK1W6L',
  employee: 'fldHWpwvRibP5K5WM',
  customerId: 'fldS2ZTdXrfViRE1W',
  customerName: 'fldO6le34us3tBiOM',
  customerEmail: 'fldZhK7GGmSDdN4Uy',
  discount: 'fldhDsFZQTiLuZyHF',
  promotion: 'fldSsPuKnZLP5xoLC',
  promoCode: 'fldXGpb8DdM98Wejo',
  products: 'fldnUwrhA8C7iByJQ',
  date: 'flde3BcUBVzU3atXN',
};
const EMPLOYEE = {
  name: 'fldfx5XqnufDFx3il',
  email: 'fldtZABqLYuLRJef5',
  zingfitEmail: 'fldt1pMIEG1dGAJKs',
};

// Report columns this sync reads. If MTEK renames one, the run stops rather than
// importing blanks.
const REQUIRED_COLUMNS = [
  'Promotion',
  'Promo Code',
  'Discount Amount',
  'Order Products',
  'Order Number',
  'Date',
  'Customer ID',
  'Customer Email',
  'Customer Name',
];

function requireConfig() {
  const missing = [];
  if (!CONFIG.airtable.token) missing.push('AIRTABLE_TOKEN');
  if (!CONFIG.mtek.token) missing.push('MTEK_API_TOKEN');
  if (!CONFIG.recordId) missing.push('AIRTABLE_RECORD_ID');
  if (missing.length) {
    throw new Error(`Missing required environment variable(s): ${missing.join(', ')}`);
  }
}

async function airtableRequest({ method = 'GET', tableId, recordId = '', body, query = '' }) {
  const base = `https://api.airtable.com/v0/${CONFIG.airtable.baseId}/${tableId}`;
  const url = `${recordId ? `${base}/${recordId}` : base}${query ? `?${query}` : ''}`;
  const response = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${CONFIG.airtable.token}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) {
    throw new Error(`Airtable ${method} failed (${response.status}): ${await response.text()}`);
  }
  return response.json();
}

const byFieldId = 'returnFieldsByFieldId=true';

async function fetchPeriod() {
  return airtableRequest({ tableId: CONFIG.airtable.periodsTableId, recordId: CONFIG.recordId, query: byFieldId });
}

async function updatePeriod(fields) {
  return airtableRequest({
    method: 'PATCH',
    tableId: CONFIG.airtable.periodsTableId,
    recordId: CONFIG.recordId,
    body: { fields },
  });
}

async function fetchAll(tableId, fieldIds, viewId = '') {
  const records = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ returnFieldsByFieldId: 'true', pageSize: '100' });
    fieldIds.forEach((id) => params.append('fields[]', id));
    if (viewId) params.set('view', viewId);
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
    // Field IDs back, so a created row reads the same as a fetched one.
    const page = await airtableRequest({
      method,
      tableId,
      body: { records: records.slice(i, i + 10), returnFieldsByFieldId: true },
    });
    written.push(...(page.records || []));
  }
  return written;
}

function normalise(value) {
  return String(value ?? '').trim().toLowerCase();
}

const NOTES_MAX = 100000;
const NOTE_ENTRY_MAX = 5000;

/** "2026-09-09 12:05 EDT" — real zone abbreviation, so winter reads EST. */
function localStamp() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: CONFIG.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZoneName: 'short',
  }).formatToParts(new Date());
  const map = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const hour = map.hour === '24' ? '00' : map.hour;
  return `${map.year}-${map.month}-${map.day} ${hour}:${map.minute} ${map.timeZoneName}`;
}

/** Barter Notes is an append-only log: newest run on top, older runs kept below. */
async function appendNote(entry) {
  let existing = '';
  try {
    existing = String((await fetchPeriod()).fields?.[PERIOD.notes] || '');
  } catch {
    // Losing the previous notes must not mask the error we are trying to report.
  }
  const stamped = `[${localStamp()}] ${String(entry).slice(0, NOTE_ENTRY_MAX)}`;
  return (existing ? `${stamped}\n\n${existing}` : stamped).slice(0, NOTES_MAX);
}

/**
 * Turns the report into BARTER redemptions inside [start, end]. Throws on a
 * changed report shape — refusing to guess beats importing blanks.
 */
function barterRowsFromReport(report, startDate, endDate, promotion = CONFIG.promotion) {
  const headers = (report?.headers || []).map(String);
  const missing = REQUIRED_COLUMNS.filter((column) => !headers.includes(column));
  if (missing.length) {
    throw new Error(
      `MTEK's Promotion Redemptions report no longer has column(s) ${missing.join(', ')}. Nothing was imported — the report changed shape.`,
    );
  }
  const col = (row, name) => row[headers.indexOf(name)];

  const rows = [];
  for (const row of report.rows) {
    if (normalise(col(row, 'Promotion')) !== normalise(promotion)) continue;
    const date = String(col(row, 'Date') || '');
    if (!date || date < startDate || date > endDate) continue;

    const orderNumber = String(col(row, 'Order Number') || '').trim();
    if (!orderNumber) {
      throw new Error(
        `A ${promotion} redemption on ${date} has no Order Number, so it can't be deduplicated. Nothing was imported.`,
      );
    }
    rows.push({
      orderNumber,
      date,
      customerId: Number(col(row, 'Customer ID')) || null,
      customerName: String(col(row, 'Customer Name') || '').trim(),
      customerEmail: String(col(row, 'Customer Email') || '').trim(),
      discount: Number(col(row, 'Discount Amount')) || 0,
      promotion: String(col(row, 'Promotion') || '').trim(),
      promoCode: String(col(row, 'Promo Code') || '').trim(),
      products: String(col(row, 'Order Products') || '').trim(),
    });
  }
  return rows;
}

/**
 * Email first (Email or Zingfit e-mail), then full name. A key shared by two
 * active employees is ambiguous and left unmatched rather than guessed.
 */
function buildEmployeeMatcher(employeeRecords) {
  const byEmail = new Map();
  const byName = new Map();
  const add = (map, key, id) => {
    if (!key) return;
    if (!map.has(key)) map.set(key, new Set());
    map.get(key).add(id);
  };
  for (const record of employeeRecords) {
    const fields = record.fields || {};
    add(byEmail, normalise(fields[EMPLOYEE.email]), record.id);
    add(byEmail, normalise(fields[EMPLOYEE.zingfitEmail]), record.id);
    add(byName, normalise(fields[EMPLOYEE.name]), record.id);
  }
  return ({ customerEmail, customerName }) => {
    const email = byEmail.get(normalise(customerEmail));
    if (email?.size === 1) return [...email][0];
    if (email?.size > 1) return null;
    const name = byName.get(normalise(customerName));
    return name?.size === 1 ? [...name][0] : null;
  };
}

async function run() {
  requireConfig();

  // Downstream status cleared so a re-run can't show a previous run's COMPLETE
  // while it is still working.
  let phaseField = PERIOD.status;
  await updatePeriod({
    [PERIOD.overallStatus]: 'Started',
    [PERIOD.status]: 'Started',
    [PERIOD.employeeStatus]: null,
  });

  try {
    const period = await fetchPeriod();
    const startDate = period.fields?.[PERIOD.start];
    const endDate = period.fields?.[PERIOD.end];
    if (!startDate || !endDate) throw new Error('Start Date and/or End Date are missing on this Payroll period.');

    // One async report job returns the whole window from S3 in a single payload,
    // so there are no pages to lose.
    const report = await fetchMtekReport({
      baseUrl: CONFIG.mtek.baseUrl,
      token: CONFIG.mtek.token,
      reportId: CONFIG.mtek.reportId,
      slug: CONFIG.mtek.reportSlug,
      dateParams: { min_order_date: startDate, max_order_date: endDate },
    });

    // Promotions are redeemed at every studio every day, so a completely empty
    // report means the fetch or the dates are wrong — unless the period has only
    // just started, which the message says.
    if (!report.rows.length) {
      throw new Error(
        `MTEK returned no promotion redemptions at all between ${startDate} and ${endDate}. If the period has only just started, re-run the fetch later.`,
      );
    }

    const mtekRows = barterRowsFromReport(report, startDate, endDate);

    const existing = await fetchAll(CONFIG.airtable.barterTableId, [
      BARTER.orderNumber,
      BARTER.period,
      BARTER.employee,
      BARTER.customerName,
      BARTER.customerEmail,
      BARTER.discount,
      BARTER.date,
    ]);
    // Order numbers are unique across MTEK, so one already anywhere in the table
    // is the same redemption — never import it twice.
    const existingByOrder = new Map(
      existing.map((record) => [String(record.fields?.[BARTER.orderNumber] || '').trim(), record]),
    );

    const toCreate = [];
    const differs = [];
    const seen = new Set();
    let duplicatesSkipped = 0;
    for (const row of mtekRows) {
      seen.add(row.orderNumber);
      const current = existingByOrder.get(row.orderNumber);
      if (!current) {
        toCreate.push({
          [BARTER.orderNumber]: row.orderNumber,
          [BARTER.period]: [CONFIG.recordId],
          [BARTER.customerId]: row.customerId,
          [BARTER.customerName]: row.customerName,
          [BARTER.customerEmail]: row.customerEmail || null,
          [BARTER.discount]: row.discount,
          [BARTER.promotion]: row.promotion,
          [BARTER.promoCode]: row.promoCode,
          [BARTER.products]: row.products,
          [BARTER.date]: row.date,
        });
        continue;
      }
      duplicatesSkipped += 1;
      // Reported, not overwritten: it may be an MTEK edit or a correction made here.
      const fields = current.fields || {};
      if (Number(fields[BARTER.discount] || 0) !== row.discount || fields[BARTER.date] !== row.date) {
        differs.push(
          `${row.orderNumber} ${row.customerName}: Airtable ${fields[BARTER.date]} $${Number(fields[BARTER.discount] || 0).toFixed(2)} / MTEK ${row.date} $${row.discount.toFixed(2)}`,
        );
      }
    }

    const periodRows = existing.filter((record) => (record.fields?.[BARTER.period] || []).includes(CONFIG.recordId));
    const noLongerInMtek = periodRows
      .map((record) => String(record.fields?.[BARTER.orderNumber] || '').trim())
      .filter((orderNumber) => orderNumber && !seen.has(orderNumber))
      .map((orderNumber) => `${orderNumber}: not a ${CONFIG.promotion} redemption in MTEK any more`);

    const created = await writeInBatches(
      'POST',
      CONFIG.airtable.barterTableId,
      toCreate.map((fields) => ({ fields })),
    );

    phaseField = PERIOD.employeeStatus;
    await updatePeriod({
      [PERIOD.status]: 'COMPLETE - Barter found',
      [PERIOD.employeeStatus]: 'Started',
    });

    const matchEmployee = buildEmployeeMatcher(
      await fetchAll(
        CONFIG.airtable.employeesTableId,
        [EMPLOYEE.name, EMPLOYEE.email, EMPLOYEE.zingfitEmail],
        CONFIG.airtable.employeesViewId,
      ),
    );

    // New rows, plus earlier rows of this period still missing an employee — so
    // fixing an employee's email and re-fetching fills them in.
    const needEmployee = [
      ...created.map((record) => ({ id: record.id, fields: record.fields || {} })),
      ...periodRows.filter((record) => !(record.fields?.[BARTER.employee] || []).length),
    ];
    const employeeUpdates = [];
    const unmatched = [];
    for (const record of needEmployee) {
      const customer = {
        customerEmail: record.fields[BARTER.customerEmail],
        customerName: record.fields[BARTER.customerName],
      };
      const employeeId = matchEmployee(customer);
      if (employeeId) employeeUpdates.push({ id: record.id, fields: { [BARTER.employee]: [employeeId] } });
      else unmatched.push(`${customer.customerName || '(no name)'} <${customer.customerEmail || 'no email'}>`);
    }
    await writeInBatches('PATCH', CONFIG.airtable.barterTableId, employeeUpdates);

    const unmatchedCustomers = [...new Set(unmatched)];
    const totalDiscount = mtekRows.reduce((sum, row) => sum + row.discount, 0);

    const listed = (label, lines) => {
      if (!lines.length) return [];
      const shown = lines.slice(0, 25).map((line) => `- ${line}`);
      if (lines.length > 25) shown.push(`- ...and ${lines.length - 25} more`);
      return [`${label}:`, ...shown];
    };

    const summary = [
      `# of Redemptions in MTEK (all promotions): ${report.rows.length}`,
      `# of ${CONFIG.promotion} redemptions: ${mtekRows.length}`,
      `# of New: ${created.length}`,
      `# of Duplicates skipped: ${duplicatesSkipped}`,
      `# of Differs from MTEK: ${differs.length}`,
      `# of No longer in MTEK: ${noLongerInMtek.length}`,
      `# of Redemptions with no employee: ${unmatched.length}`,
      `Period ${CONFIG.promotion} discount total: $${totalDiscount.toFixed(2)}`,
    ].join(' | ');

    const note = [
      summary,
      ...listed('No employee found (customer)', unmatchedCustomers),
      ...listed('Differs from MTEK (Airtable kept, not overwritten)', differs),
      ...listed('No longer in MTEK', noLongerInMtek),
    ].join('\n');

    await updatePeriod({
      [PERIOD.employeeStatus]: unmatched.length ? 'PROBLEM' : 'COMPLETE - Employees assigned',
      // COMPLETE only when every redemption is tied to an employee.
      [PERIOD.overallStatus]: unmatched.length ? 'PROBLEM' : 'COMPLETE',
      [PERIOD.notes]: await appendNote(note),
    });

    console.log(`HR Payroll Barter completed for ${CONFIG.recordId}. ${summary}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    // Mark whichever phase was in flight, so a mid-run failure doesn't leave an
    // earlier phase reading COMPLETE next to an unexplained PROBLEM.
    await updatePeriod({
      [PERIOD.overallStatus]: 'PROBLEM',
      [phaseField]: 'PROBLEM',
      [PERIOD.notes]: await appendNote(message),
    });
    throw error;
  }
}

// Runs whenever this file is executed. HR_BARTER_SKIP_RUN=1 lets the parsing and
// matching be imported and exercised on their own.
if (process.env.HR_BARTER_SKIP_RUN !== '1') {
  run().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export { barterRowsFromReport, buildEmployeeMatcher };
