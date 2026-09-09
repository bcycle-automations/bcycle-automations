#!/usr/bin/env node

/**
 * HR Payroll Time Punches
 * MarianaTek time_clock_shifts -> Airtable "Time Punches" sync.
 *
 * Runs against the HR base (NOT HR - Instructors). Driven by one
 * "Budget week - Studio" record, which supplies the date window and the studio.
 */

const CONFIG = {
  airtable: {
    baseId: process.env.AIRTABLE_BASE_ID || 'appiwfeujJzUZPPBx',
    runsTableId: process.env.AIRTABLE_RUNS_TABLE_ID || 'tblbyFY6TlRi4BxOe',
    punchesTableId: process.env.AIRTABLE_PUNCHES_TABLE_ID || 'tblVxt2W7NanQmJFR',
    employeesTableId: process.env.AIRTABLE_EMPLOYEES_TABLE_ID || 'tbl0FzJ2s4Mk5jWIi',
    // "Active Employees - ALL". The full table holds 836 rows including former
    // staff, and 74 names are duplicated across it, so matching against everything
    // silently linked 14 people to a stale record instead of their current one.
    employeesViewId: process.env.AIRTABLE_EMPLOYEES_VIEW_ID || 'viws8tSbvXfujLnwG',
    ratesTableId: process.env.AIRTABLE_RATES_TABLE_ID || 'tblufK9k5Tg5uCd74',
    studiosTableId: process.env.AIRTABLE_STUDIOS_TABLE_ID || 'tblAXy4xm0kJMkWeQ',
    token: process.env.AIRTABLE_TOKEN,
  },
  mtek: {
    baseUrl: process.env.MTEK_BASE_URL || 'https://bcycle.marianatek.com',
    punchesPath: process.env.MTEK_PUNCHES_PATH || '/api/time_clock_shifts',
    token: process.env.MTEK_API_TOKEN,
  },
  timeZone: process.env.PAYROLL_TIME_ZONE || 'America/Toronto',
  recordId: process.env.AIRTABLE_RECORD_ID,
};

function requireConfig() {
  const missing = [];
  if (!CONFIG.airtable.token) missing.push('AIRTABLE_TOKEN');
  if (!CONFIG.mtek.token) missing.push('MTEK_API_TOKEN');
  if (!CONFIG.recordId) missing.push('AIRTABLE_RECORD_ID');

  if (missing.length) {
    throw new Error(`Missing required environment variable(s): ${missing.join(', ')}`);
  }
}

function airtableUrl(tableId, recordId = '', query = '') {
  const base = `https://api.airtable.com/v0/${CONFIG.airtable.baseId}/${tableId}`;
  const withRecord = recordId ? `${base}/${recordId}` : base;
  return query ? `${withRecord}?${query}` : withRecord;
}

async function airtableRequest({ method = 'GET', tableId, recordId = '', body, query = '' }) {
  const response = await fetch(airtableUrl(tableId, recordId, query), {
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

async function updateRunRecord(fields) {
  return airtableRequest({
    method: 'PATCH',
    tableId: CONFIG.airtable.runsTableId,
    recordId: CONFIG.recordId,
    body: { fields },
  });
}

async function fetchRunRecord() {
  return airtableRequest({
    method: 'GET',
    tableId: CONFIG.airtable.runsTableId,
    recordId: CONFIG.recordId,
  });
}

async function fetchAllRecords(tableId, fields = [], viewId = '') {
  const collected = [];
  let offset = '';

  do {
    const params = new URLSearchParams();
    fields.forEach((field) => params.append('fields[]', field));
    if (viewId) params.set('view', viewId);
    if (offset) params.set('offset', offset);

    const page = await airtableRequest({
      method: 'GET',
      tableId,
      query: params.toString(),
    });

    collected.push(...(page.records || []));
    offset = page.offset || '';
  } while (offset);

  return collected;
}

async function createPunchRecords(records) {
  const created = [];
  for (let i = 0; i < records.length; i += 10) {
    const batch = records.slice(i, i + 10);
    const response = await airtableRequest({
      method: 'POST',
      tableId: CONFIG.airtable.punchesTableId,
      body: { records: batch.map((fields) => ({ fields })) },
    });
    created.push(...response.records);
  }
  return created;
}

async function patchPunchRecords(updates) {
  for (let i = 0; i < updates.length; i += 10) {
    const batch = updates.slice(i, i + 10);
    await airtableRequest({
      method: 'PATCH',
      tableId: CONFIG.airtable.punchesTableId,
      body: { records: batch },
    });
  }
}

function getField(record, fieldName) {
  return record?.fields?.[fieldName];
}

/** Lookup fields come back as arrays; take the first value. */
function firstValue(value) {
  return Array.isArray(value) ? value[0] : value;
}

function localParts(input) {
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) return null;

  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: CONFIG.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date);

  const map = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  // en-CA renders midnight as "24"; normalise it back to "00".
  const hour = map.hour === '24' ? '00' : map.hour;
  return {
    date: `${map.year}-${map.month}-${map.day}`,
    time: `${hour}:${map.minute}`,
  };
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

/**
 * Notes is an append-only log: newest run on top, older runs kept below.
 * Re-reads the field first so a re-run adds to whatever is actually there.
 */
async function appendNote(entry) {
  let existing = '';
  try {
    existing = String(getField(await fetchRunRecord(), 'Notes') || '');
  } catch {
    // Losing the previous notes must not mask the error we are trying to report.
  }

  const stamped = `[${localStamp()}] ${String(entry).slice(0, NOTE_ENTRY_MAX)}`;
  return (existing ? `${stamped}\n\n${existing}` : stamped).slice(0, NOTES_MAX);
}

/** Mirrors the Total Hours formula: HH:MM difference, wrapping past midnight. */
function hoursBetween(timeIn, timeOut) {
  if (!timeIn || !timeOut) return 0;

  const toMinutes = (value) => {
    const [hours, minutes] = String(value).split(':');
    return (Number(hours) || 0) * 60 + (Number(minutes) || 0);
  };

  return ((((toMinutes(timeOut) - toMinutes(timeIn)) % 1440) + 1440) % 1440) / 60;
}

async function mtekRequest(path, params = {}) {
  const url = new URL(path, CONFIG.mtek.baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value));
    }
  });

  const response = await fetch(url.toString(), {
    headers: {
      Authorization: `Bearer ${CONFIG.mtek.token}`,
      Accept: 'application/vnd.api+json',
    },
  });

  const rawBody = await response.text();
  if (!response.ok) {
    throw new Error(`MTEK request failed (${response.status}) ${url}: ${rawBody}`);
  }

  try {
    return rawBody ? JSON.parse(rawBody) : {};
  } catch (error) {
    throw new Error(
      `MTEK response was not valid JSON (${url}): ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Collects every page plus the sideloaded `included` records, keyed by type+id
 * so employee/user/shift_type names can be resolved without an extra call each.
 */
async function fetchPaginatedMtek(path, params = {}) {
  const allResults = [];
  const included = new Map();
  let currentPage = 1;
  let totalPages = 1;

  while (currentPage <= totalPages) {
    const page = await mtekRequest(path, { ...params, page: currentPage, page_size: 100 });

    allResults.push(...(Array.isArray(page?.data) ? page.data : []));
    for (const item of Array.isArray(page?.included) ? page.included : []) {
      included.set(`${item.type}:${item.id}`, item);
    }

    const parsedPages = Number(page?.meta?.pagination?.pages);
    totalPages = Number.isFinite(parsedPages) && parsedPages > 0 ? parsedPages : currentPage;
    currentPage += 1;
  }

  return { results: allResults, included };
}

function relationshipId(shift, name) {
  return shift?.relationships?.[name]?.data?.id ?? null;
}

function employeeNameFromShift(shift, included) {
  const employeeId = relationshipId(shift, 'employee');
  if (!employeeId) return '';

  const employee = included.get(`employees:${employeeId}`);
  const userId = employee?.relationships?.user?.data?.id;
  if (!userId) return '';

  const user = included.get(`users:${userId}`);
  const attributes = user?.attributes || {};
  if (attributes.full_name) return attributes.full_name;

  return [attributes.first_name, attributes.last_name].filter(Boolean).join(' ');
}

function shiftTypeNameFromShift(shift, included) {
  const shiftTypeId = relationshipId(shift, 'shift_type');
  if (!shiftTypeId) return '';
  return included.get(`shift_types:${shiftTypeId}`)?.attributes?.name || '';
}

function normalise(value) {
  return String(value || '').trim().toLowerCase();
}

/** Shifts the local date by `days` and returns a UTC instant safely past the boundary. */
function paddedBound(dateString, days) {
  const date = new Date(`${dateString}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().replace(/\.\d{3}Z$/, 'Z');
}

/**
 * MTEK IDs already on this run record's punches. A second Fetch then skips
 * them instead of creating a duplicate set.
 */
async function fetchExistingMtekIds(punchRecordIds) {
  const existing = new Set();

  for (let i = 0; i < punchRecordIds.length; i += 50) {
    const chunk = punchRecordIds.slice(i, i + 50);
    const params = new URLSearchParams({
      filterByFormula: `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(',')})`,
      pageSize: '50',
    });
    params.append('fields[]', 'MTEK ID');

    const page = await airtableRequest({
      tableId: CONFIG.airtable.punchesTableId,
      query: params.toString(),
    });

    for (const record of page.records || []) {
      const mtekId = String(getField(record, 'MTEK ID') || '').trim();
      if (mtekId) existing.add(mtekId);
    }
  }

  return existing;
}

async function run() {
  requireConfig();

  // Downstream statuses are cleared so a re-run can't show a previous run's
  // COMPLETE while it is still working.
  let phaseField = 'Time punch Status';
  await updateRunRecord({
    'Overall Status': 'Started',
    'Time punch Status': 'Started',
    'Time in/out Status': 'Started',
    'Employee Status': null,
    'Rate type Status': null,
  });

  try {
    const runRecord = await fetchRunRecord();
    const startDate = firstValue(getField(runRecord, 'Start Date'));
    const endDate = firstValue(getField(runRecord, 'End Date'));
    const studioLinks = getField(runRecord, 'Studio') || [];

    if (!startDate || !endDate) {
      throw new Error('Start Date and/or End Date are missing — is this record linked to a Budget week?');
    }
    if (!studioLinks.length) {
      throw new Error('Studio is empty on this Budget week - Studio record.');
    }

    const studioRecord = await airtableRequest({
      method: 'GET',
      tableId: CONFIG.airtable.studiosTableId,
      recordId: studioLinks[0],
    });
    const locationId = String(getField(studioRecord, 'MTEK Location ID') || '').trim();
    if (!locationId) {
      throw new Error(`Studio "${getField(studioRecord, 'Studio name') || studioLinks[0]}" has no MTEK Location ID.`);
    }

    // MTEK's min/max_start_datetime bounds are applied loosely, so the window is
    // padded by a day on each side and the exact range is enforced locally below.
    const { results: shifts, included } = await fetchPaginatedMtek(CONFIG.mtek.punchesPath, {
      location: locationId,
      min_start_datetime: paddedBound(startDate, -1),
      max_start_datetime: paddedBound(endDate, 2),
      include: 'employee.user,shift_type',
    });

    const existingMtekIds = await fetchExistingMtekIds(getField(runRecord, 'Time Punch for payroll') || []);

    const punchRecordsToCreate = [];
    let openShiftCount = 0;
    let duplicatesSkipped = 0;

    for (const shift of shifts) {
      const attributes = shift?.attributes || {};
      const start = localParts(attributes.start_datetime);
      if (!start) continue;
      if (start.date < startDate || start.date > endDate) continue;

      const mtekId = String(shift?.id ?? '').trim();
      if (mtekId && existingMtekIds.has(mtekId)) {
        duplicatesSkipped += 1;
        continue;
      }

      const end = attributes.end_datetime ? localParts(attributes.end_datetime) : null;
      if (!end) openShiftCount += 1;

      // Total Hours is a formula over Time In/Out, so it is deliberately not
      // written here — that keeps a hand-corrected punch recalculating.
      punchRecordsToCreate.push({
        Date: start.date,
        'Time In': start.time,
        'Time Out': end ? end.time : '',
        'MTEK ID': mtekId,
        'Location ID': String(relationshipId(shift, 'location') ?? ''),
        'Employee Name': employeeNameFromShift(shift, included),
        'Rate Type': shiftTypeNameFromShift(shift, included),
        'Budget week - Studio': [CONFIG.recordId],
      });
    }

    const createdPunches = punchRecordsToCreate.length
      ? await createPunchRecords(punchRecordsToCreate)
      : [];

    phaseField = 'Employee Status';
    await updateRunRecord({
      'Time punch Status': 'COMPLETE - Time punches found',
      'Time in/out Status': 'COMPLETE - Time in/out found',
      'Employee Status': 'Started',
    });

    const employeeRecords = await fetchAllRecords(
      CONFIG.airtable.employeesTableId,
      ['Name'],
      CONFIG.airtable.employeesViewId,
    );
    const employeeMap = new Map();
    for (const rec of employeeRecords) {
      const name = normalise(getField(rec, 'Name'));
      if (name && !employeeMap.has(name)) employeeMap.set(name, rec.id);
    }

    let employeeNotFound = 0;
    const employeeUpdates = [];
    for (const punch of createdPunches) {
      const employeeId = employeeMap.get(normalise(getField(punch, 'Employee Name')));
      if (employeeId) employeeUpdates.push({ id: punch.id, fields: { Employee: [employeeId] } });
      else employeeNotFound += 1;
    }
    await patchPunchRecords(employeeUpdates);

    phaseField = 'Rate type Status';
    await updateRunRecord({
      'Employee Status': employeeNotFound ? 'PROBLEM' : 'COMPLETE - Employees assigned',
      'Rate type Status': 'Started',
    });

    const rateRecords = await fetchAllRecords(CONFIG.airtable.ratesTableId, ['Name', 'Rate']);
    const rateMap = new Map();
    for (const rec of rateRecords) {
      const name = normalise(getField(rec, 'Name'));
      if (name && !rateMap.has(name)) {
        rateMap.set(name, { id: rec.id, rate: Number(getField(rec, 'Rate')) || 0 });
      }
    }

    let rateNotFound = 0;
    let totalHours = 0;
    let totalWages = 0;
    const rateUpdates = [];

    for (const punch of createdPunches) {
      const hours = hoursBetween(getField(punch, 'Time In'), getField(punch, 'Time Out'));
      totalHours += hours;

      // Rates records are named "<Employee name> <Shift Type>".
      const key = normalise(`${getField(punch, 'Employee Name')} ${getField(punch, 'Rate Type')}`);
      const rate = rateMap.get(key);
      if (rate) {
        rateUpdates.push({ id: punch.id, fields: { Rate: [rate.id] } });
        totalWages += hours * rate.rate;
      } else {
        rateNotFound += 1;
      }
    }
    await patchPunchRecords(rateUpdates);

    const note = [
      `# of Time punches found: ${createdPunches.length}`,
      `# of Duplicates skipped: ${duplicatesSkipped}`,
      `# of Employees not found: ${employeeNotFound}`,
      `# of Rates not found: ${rateNotFound}`,
      `# of Punches with no clock-out: ${openShiftCount}`,
      `Total hours: ${totalHours.toFixed(2)}`,
      `Total wages: $${totalWages.toFixed(2)}`,
    ].join(' | ');

    await updateRunRecord({
      'Rate type Status': rateNotFound ? 'PROBLEM' : 'COMPLETE - Rates assigned',
      // COMPLETE only when nothing went unmatched — an unmatched employee or rate
      // means the run finished but the payroll numbers are not trustworthy yet.
      'Overall Status': employeeNotFound || rateNotFound ? 'PROBLEM' : 'COMPLETE',
      Notes: await appendNote(note),
    });

    console.log(`HR Payroll Time Punches completed for ${CONFIG.recordId}. ${note}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    // Mark whichever phase was in flight, so a mid-run failure doesn't leave
    // an earlier phase reading COMPLETE next to an unexplained PROBLEM.
    await updateRunRecord({
      'Overall Status': 'PROBLEM',
      [phaseField]: 'PROBLEM',
      Notes: await appendNote(message),
    });
    throw error;
  }
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
