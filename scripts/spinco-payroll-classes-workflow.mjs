#!/usr/bin/env node

/**
 * SPINCO PAYROLL Classes (SPINCO copy of bcycle-payroll-classes-workflow.mjs)
 * SPINCO MTEK -> HR base "SPINCO Classes (for Payroll)". Only the SPINCO corporate
 * studios (Studios rows whose Company Name starts with "SPINCO" and that have an
 * MTEK Location ID) are imported; the rest of the franchise network is ignored.
 */

const CONFIG = {
  airtable: {
    // Everything lives in the HR base (instructors are in Employees).
    baseId: process.env.AIRTABLE_BASE_ID || 'appiwfeujJzUZPPBx',
    runsTableId: process.env.AIRTABLE_RUNS_TABLE_ID || 'tbl157YNKNMHiKKTq', // SPINCO Payroll Class log
    classesTableId: process.env.AIRTABLE_CLASSES_TABLE_ID || 'tbl6TFtzRIuXkf7LN', // SPINCO Classes (for Payroll)
    employeesTableId: process.env.AIRTABLE_EMPLOYEES_TABLE_ID || 'tbl0FzJ2s4Mk5jWIi',
    // "Active Employees - ALL": the full table has duplicate names (former staff).
    employeesViewId: process.env.AIRTABLE_EMPLOYEES_VIEW_ID || 'viws8tSbvXfujLnwG',
    studiosTableId: process.env.AIRTABLE_STUDIOS_TABLE_ID || 'tblAXy4xm0kJMkWeQ',
    classTypesTableId: process.env.AIRTABLE_CLASS_TYPES_TABLE_ID || 'tbliwah9oWGgIs3Gg', // SPINCO Class Types
    payrollPeriodsTableId: process.env.AIRTABLE_PAYROLL_PERIODS_TABLE_ID || 'tblBobHdbWbelKS2C', // SPINCO Payroll period
    // Field IDs, so a renamed column can't break assignment.
    periodStartFieldId: 'fldex0yJv60mcGgrN',
    periodEndFieldId: 'fldZw1GSj3ib8p7OC',
    classPeriodFieldId: 'fldlMRXZ1WhuTyXP4', // Classes: Payroll Period
    runPeriodFieldId: 'flduVUaYwgFxliAiJ', // Class log: Payroll Period
    token: process.env.AIRTABLE_TOKEN,
  },
  mtek: {
    baseUrl: process.env.MTEK_SPINCO_BASE_URL || 'https://spinco.marianatek.com',
    classesPath: process.env.MTEK_CLASSES_PATH || '/api/class_sessions',
    token: process.env.MTEK_SPINCO_API_TOKEN,
  },
  recordId: process.env.AIRTABLE_RECORD_ID,
};

function requireConfig() {
  const missing = [];
  if (!CONFIG.airtable.token) missing.push('AIRTABLE_TOKEN');
  if (!CONFIG.mtek.token) missing.push('MTEK_SPINCO_API_TOKEN');
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

async function createClassRecords(records) {
  const created = [];
  for (let i = 0; i < records.length; i += 10) {
    const batch = records.slice(i, i + 10);
    const response = await airtableRequest({
      method: 'POST',
      tableId: CONFIG.airtable.classesTableId,
      body: { records: batch.map((fields) => ({ fields })) },
    });
    created.push(...response.records);
  }
  return created;
}

async function patchClassRecord(recordId, fields) {
  return airtableRequest({
    method: 'PATCH',
    tableId: CONFIG.airtable.classesTableId,
    recordId,
    body: { fields },
  });
}

// [Employees field, Classes (for Payroll) locked field]. SPINCO has no bonus.
const RATE_SNAPSHOT = [['SPINCO Class Base Rate', 'Base Rate (locked)']];

// A blank rate is stored as 0 and "Rates locked" is ticked, so the class formulas
// never fall back to the live Employee rate (a raise later must not touch it).
function rateSnapshot(instructorRecord) {
  // "Rates locked for" must equal the class's "Instructor (text)" (the linked
  // record's name), or the Airtable lock check would flag a change.
  const out = {
    'Rates locked': true,
    'Rates locked for': String(getField(instructorRecord, 'Name') ?? ''),
  };
  for (const [source, target] of RATE_SNAPSHOT) {
    const value = getField(instructorRecord, source);
    out[target] = typeof value === 'number' ? value : 0;
  }
  return out;
}

function localDateTimeString(input, timeZone) {
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) return null;

  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(date);

  const map = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${map.year}-${map.month}-${map.day}T${map.hour}:${map.minute}:${map.second}`;
}

async function mtekRequestUrl(url) {
  const headers = {
    Authorization: `Bearer ${CONFIG.mtek.token}`,
    Accept: 'application/vnd.api+json',
  };

  const response = await fetch(url, { headers });
  const rawBody = await response.text();

  if (!response.ok) {
    throw new Error(`MTEK request failed (${response.status}) ${url}: ${rawBody}`);
  }

  try {
    return rawBody ? JSON.parse(rawBody) : {};
  } catch (error) {
    throw new Error(
      `MTEK response was not valid JSON (${url}): ${error instanceof Error ? error.message : String(error)} | raw=${rawBody}`,
    );
  }
}

async function mtekRequestPath(path, params = {}) {
  const url = new URL(path, CONFIG.mtek.baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value));
    }
  });

  return mtekRequestUrl(url.toString());
}

async function fetchPaginatedMtek(path, params = {}) {
  const allResults = [];
  let currentPage = 1;
  let totalPages = 1;

  while (currentPage <= totalPages) {
    const page = await mtekRequestPath(path, {
      ...params,
      page: currentPage,
      page_size: 100,
    });

    const pageResults = Array.isArray(page?.data) ? page.data : [];
    allResults.push(...pageResults);

    const parsedPages = Number(page?.meta?.pagination?.pages);
    totalPages = Number.isFinite(parsedPages) && parsedPages > 0 ? parsedPages : currentPage;
    currentPage += 1;
  }

  return allResults;
}

function firstOf(value) {
  return Array.isArray(value) ? value[0] : value;
}

function getField(record, fieldName) {
  return record?.fields?.[fieldName];
}

async function fetchWindows(tableId, startFieldId, endFieldId) {
  const periods = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ returnFieldsByFieldId: 'true' });
    params.append('fields[]', startFieldId);
    params.append('fields[]', endFieldId);
    if (offset) params.set('offset', offset);
    const page = await airtableRequest({ tableId, query: params.toString() });
    for (const record of page.records || []) {
      const start = record.fields?.[startFieldId];
      const end = record.fields?.[endFieldId];
      if (start && end) periods.push({ id: record.id, start, end });
    }
    offset = page.offset || '';
  } while (offset);
  return periods;
}

/**
 * Gives every class its pay period, by local class date, before
 * anything is written — a missing or overlapping window fails the run cleanly
 * instead of leaving half-assigned classes. Classes dated before the first
 * window are left without one, so older date ranges still run.
 */
function assignWindows(classRecords, periods, fieldId = CONFIG.airtable.classPeriodFieldId, label = 'Payroll Period') {
  if (!periods.length) return;
  const earliestStart = periods.map((period) => period.start).sort()[0];
  const uncovered = new Set();
  const ambiguous = new Set();

  for (const fields of classRecords) {
    const date = String(fields['Class Date'] || '').slice(0, 10);
    if (!date) {
      throw new Error(`MTEK class ${fields['MTEK ID'] ?? '(no id)'} has no readable start time, so it can't be given a pay period. Nothing was imported.`);
    }
    if (date < earliestStart) continue;
    const matches = periods.filter((period) => period.start <= date && date <= period.end);
    if (matches.length === 1) fields[fieldId] = [matches[0].id];
    else (matches.length ? ambiguous : uncovered).add(date);
  }

  if (uncovered.size || ambiguous.size) {
    const problems = [];
    if (uncovered.size) problems.push(`no ${label} covers ${[...uncovered].sort().join(', ')}`);
    if (ambiguous.size) problems.push(`more than one ${label} covers ${[...ambiguous].sort().join(', ')}`);
    throw new Error(
      `Can't give every class its ${label}: ${problems.join('; ')}. Run SPINCO Create Budget week (it creates the Payroll periods), then re-run. Nothing was imported.`,
    );
  }
}

function sessionAttributes(session) {
  return session?.attributes || {};
}

function instructorNameFromSession(session) {
  const names = sessionAttributes(session).instructor_names;
  if (Array.isArray(names)) {
    if (names.length === 0) return '';
    // Keep every instructor so duos arrive as "A, B" and the Duo Needs fixing check can see them.
    return names.filter(Boolean).join(', ');
  }

  return typeof names === 'string' ? names : '';
}

// Lower-case and strip accents so MTEK's "Élizabeth Fortin" matches Airtable's "Elizabeth Fortin".
function normalizeName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .trim()
    .toLowerCase();
}

function locationIdFromSession(session) {
  return session?.relationships?.location?.data?.id ?? null;
}

async function run() {
  requireConfig();

  await updateRunRecord({ 'OVERALL Status': 'In progress' });

  try {
    const runRecord = await fetchRunRecord();
    const startDate = getField(runRecord, 'Start Date');
    const endDate = getField(runRecord, 'End Date');

    if (!startDate || !endDate) {
      throw new Error('Start Date and/or End Date are missing on the run record.');
    }

    const sessions = await fetchPaginatedMtek(CONFIG.mtek.classesPath, {
      min_date: startDate,
      max_date: endDate,
    });

    await updateRunRecord({ 'Classes Status': 'Started' });

    // Only SPINCO corporate studios: Company Name starts with "SPINCO" and has an MTEK
    // Location ID. SPINCO's MTEK also holds the franchise network, which isn't ours to pay.
    const studioRecords = await fetchAllRecords(CONFIG.airtable.studiosTableId, ['MTEK Location ID', 'Company Name']);
    const studioMap = new Map();
    for (const rec of studioRecords) {
      const locationId = String(getField(rec, 'MTEK Location ID') || '').trim();
      const company = String(firstOf(getField(rec, 'Company Name')) || '').trim().toUpperCase();
      if (locationId && company.startsWith('SPINCO')) studioMap.set(locationId, rec.id);
    }
    if (!studioMap.size) {
      throw new Error('No SPINCO studio has an MTEK Location ID on the Studios table.');
    }

    const classRecordsToCreate = [];
    let skippedOtherLocations = 0;
    for (const session of sessions) {
      const attributes = sessionAttributes(session);
      if (!studioMap.has(String(locationIdFromSession(session) ?? '').trim())) {
        skippedOtherLocations += 1;
        continue;
      }

      classRecordsToCreate.push({
        'MTEK Instructor': instructorNameFromSession(session),
        'MTEK Class Type': attributes.class_type_display || '',
        'Class Date': localDateTimeString(attributes.start_datetime, 'America/Toronto'),
        'Location ID': locationIdFromSession(session),
        'MTEK ID': session?.id ?? null,
        'Attendance Count (Checked in)': Number(attributes.checked_in_user_count || 0),
        'Payroll Class log': [CONFIG.recordId],
      });
    }

    const { payrollPeriodsTableId, periodStartFieldId, periodEndFieldId } = CONFIG.airtable;
    const periods = await fetchWindows(payrollPeriodsTableId, periodStartFieldId, periodEndFieldId);
    if (classRecordsToCreate.length) {
      assignWindows(classRecordsToCreate, periods);
    }

    // The run record (one week) belongs to the period its Start Date falls in, so
    // Payroll Period can show week 1 / week 2 and roll up the run's checks.
    const runPeriod = periods.filter((period) => period.start <= startDate && startDate <= period.end);
    if (runPeriod.length === 1) {
      await updateRunRecord({ [CONFIG.airtable.runPeriodFieldId]: [runPeriod[0].id] });
    }

    const createdClassRecords = classRecordsToCreate.length
      ? await createClassRecords(classRecordsToCreate)
      : [];

    await updateRunRecord({ 'Classes Status': 'COMPLETE - Classes found' });

    await updateRunRecord({ 'Instructors Status': 'Started' });
    const instructorRecords = await fetchAllRecords(
      CONFIG.airtable.employeesTableId,
      ['Name', 'MTEK Display Name', ...RATE_SNAPSHOT.map(([source]) => source)],
      CONFIG.airtable.employeesViewId,
    );
    // If a name appears more than once, keep the earliest-created instructor record.
    instructorRecords.sort((a, b) => Date.parse(a.createdTime) - Date.parse(b.createdTime));
    const instructorMap = new Map();
    // MTEK names people by their display name, which can differ from the Airtable Name,
    // so the "MTEK Display Name" field is matched too (Name wins when both match).
    for (const field of ['Name', 'MTEK Display Name']) {
      for (const rec of instructorRecords) {
        const name = normalizeName(getField(rec, field));
        if (name && !instructorMap.has(name)) instructorMap.set(name, rec.id);
      }
    }
    // Rates as of THIS import. Written onto each class so a later raise never
    // changes a class that has already been imported.
    const rateSnapshotById = new Map();
    for (const rec of instructorRecords) rateSnapshotById.set(rec.id, rateSnapshot(rec));

    let instructorNotFound = 0;
    for (const classRecord of createdClassRecords) {
      const mtekInstructor = normalizeName(getField(classRecord, 'MTEK Instructor'));
      const instructorId = instructorMap.get(mtekInstructor);
      if (instructorId) {
        await patchClassRecord(classRecord.id, {
          Instructor: [instructorId],
          ...rateSnapshotById.get(instructorId),
        });
      } else {
        instructorNotFound += 1;
      }
    }

    await updateRunRecord({ 'Instructors Status': 'COMPLETE - Instructors Assigned' });

    await updateRunRecord({ 'Attendance Status': 'Started' });
    await updateRunRecord({ 'Attendance Status': 'COMPLETE - Attendance found' });

    await updateRunRecord({ 'Studio Status': 'Started' });
    let studioNotFound = 0;
    for (const classRecord of createdClassRecords) {
      const locationId = String(getField(classRecord, 'Location ID') || '').trim();
      const studioId = studioMap.get(locationId);
      if (studioId) {
        await patchClassRecord(classRecord.id, { Studio: [studioId] });
      } else {
        studioNotFound += 1;
      }
    }

    await updateRunRecord({ 'Studio Status': 'COMPLETE - Studios found' });

    await updateRunRecord({ 'Class Type Status': 'Started' });
    const classTypeRecords = await fetchAllRecords(CONFIG.airtable.classTypesTableId, ['Name']);
    const classTypeMap = new Map();
    for (const rec of classTypeRecords) {
      const name = String(getField(rec, 'Name') || '').trim().toLowerCase();
      if (name) classTypeMap.set(name, rec.id);
    }

    let classTypeNotFound = 0;
    for (const classRecord of createdClassRecords) {
      const classTypeName = String(getField(classRecord, 'MTEK Class Type') || '')
        .trim()
        .toLowerCase();
      const classTypeId = classTypeMap.get(classTypeName);
      if (classTypeId) {
        await patchClassRecord(classRecord.id, { 'Class Type': [classTypeId] });
      } else {
        classTypeNotFound += 1;
      }
    }

    await updateRunRecord({ 'Class Type Status': 'COMPLETE - Class types found' });

    const note = [
      `# of Class found: ${createdClassRecords.length}`,
      `# of Classes skipped (not a SPINCO studio): ${skippedOtherLocations}`,
      `# of Instructors not found: ${instructorNotFound}`,
      `# of Studios not found: ${studioNotFound}`,
      `# of Class types not found: ${classTypeNotFound}`,
    ].join(' | ');

    await updateRunRecord({
      Notes: note,
      'OVERALL Status': 'COMPLETE',
    });

    console.log(
      `SPINCO PAYROLL Classes completed for ${CONFIG.recordId}. Created ${createdClassRecords.length} class records.`,
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await updateRunRecord({
      'OVERALL Status': 'PROBLEM',
      Notes: message.slice(0, 100000),
    });
    throw error;
  }
}

// Runs whenever this file is executed. BCYCLE_PAYROLL_CLASSES_SKIP_RUN=1 lets the
// pay-period assignment be imported and exercised on its own.
if (process.env.BCYCLE_PAYROLL_CLASSES_SKIP_RUN !== '1') {
  run().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export { assignWindows };
