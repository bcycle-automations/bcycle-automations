#!/usr/bin/env node

/**
 * b.cycle PAYROLL Classes
 * MTEK -> Airtable class sync workflow.
 */

const CONFIG = {
  airtable: {
    baseId: process.env.AIRTABLE_BASE_ID || 'appBC0Ja4B5LKbZLW',
    runsTableId: process.env.AIRTABLE_RUNS_TABLE_ID || 'tblFYdngL6XxXuvap',
    classesTableId: process.env.AIRTABLE_CLASSES_TABLE_ID || 'tbl8RbWysEFdNuz37',
    instructorsTableId: process.env.AIRTABLE_INSTRUCTORS_TABLE_ID || 'tbljLkeIdWibQF6SH',
    studiosTableId: process.env.AIRTABLE_STUDIOS_TABLE_ID || 'tblpogHdeAA2Z7HiD',
    classTypesTableId: process.env.AIRTABLE_CLASS_TYPES_TABLE_ID || 'tbliopHKhCtHLwGOf',
    // "Payroll Period" here is a synced copy of the HR base's table (the HR
    // Create Budget week job creates periods there). Field IDs, so a renamed
    // column can't break assignment.
    payrollPeriodsTableId: process.env.AIRTABLE_PAYROLL_PERIODS_TABLE_ID || 'tblGYpEKsV63NzRCT',
    periodStartFieldId: 'fldic4m4P8BVIieVv',
    periodEndFieldId: 'fld4Hg7iYvAgHTHeu',
    classPeriodFieldId: 'fldR0cD3vR4RE4pKY',
    token: process.env.AIRTABLE_TOKEN,
  },
  mtek: {
    // per request: MarianaTek base is bcycle
    baseUrl: process.env.MTEK_BASE_URL || 'https://bcycle.marianatek.com',
    classesPath: process.env.MTEK_CLASSES_PATH || '/api/class_sessions',
    token: process.env.MTEK_API_TOKEN,
  },
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

  const tokenPreview = `${CONFIG.mtek.token.slice(0, 4)}...${CONFIG.mtek.token.slice(-4)}`;
  console.log(
    `[MTEK] Token loaded from MTEK_API_TOKEN env var (length=${CONFIG.mtek.token.length}, preview=${tokenPreview})`,
  );
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

async function fetchAllRecords(tableId, fields = []) {
  const collected = [];
  let offset = '';

  do {
    const params = new URLSearchParams();
    fields.forEach((field) => params.append('fields[]', field));
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

  console.log(`[MTEK] Request URL: ${url}`);
  console.log(
    `[MTEK] Request Authorization header: Bearer <token length=${CONFIG.mtek.token.length}>`,
  );
  const response = await fetch(url, { headers });
  const rawBody = await response.text();

  console.log(`[MTEK] Response status: ${response.status}`);
  console.log(`[MTEK] Raw response body: ${rawBody}`);

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

function getField(record, fieldName) {
  return record?.fields?.[fieldName];
}

async function fetchPayrollPeriods() {
  const { payrollPeriodsTableId, periodStartFieldId, periodEndFieldId } = CONFIG.airtable;
  const periods = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ returnFieldsByFieldId: 'true' });
    params.append('fields[]', periodStartFieldId);
    params.append('fields[]', periodEndFieldId);
    if (offset) params.set('offset', offset);
    const page = await airtableRequest({ tableId: payrollPeriodsTableId, query: params.toString() });
    for (const record of page.records || []) {
      const start = record.fields?.[periodStartFieldId];
      const end = record.fields?.[periodEndFieldId];
      if (start && end) periods.push({ id: record.id, start, end });
    }
    offset = page.offset || '';
  } while (offset);
  return periods;
}

/**
 * Gives every class its pay period, by local class date, before anything is
 * written — a missing or overlapping period fails the run cleanly instead of
 * leaving half-assigned classes. Classes dated before the first period (the
 * table starts at 2026-08-23) are left without one.
 */
function assignPayrollPeriods(classRecords, periods) {
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
    if (matches.length === 1) fields[CONFIG.airtable.classPeriodFieldId] = [matches[0].id];
    else (matches.length ? ambiguous : uncovered).add(date);
  }

  if (uncovered.size || ambiguous.size) {
    const problems = [];
    if (uncovered.size) problems.push(`no Payroll Period covers ${[...uncovered].sort().join(', ')}`);
    if (ambiguous.size) problems.push(`more than one Payroll Period covers ${[...ambiguous].sort().join(', ')}`);
    throw new Error(
      `Can't give every class a pay period: ${problems.join('; ')}. Periods are created in the HR base by HR Create Budget week and synced here — check the sync, then re-run. Nothing was imported.`,
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
    return names[0] || names.join(', ');
  }

  return typeof names === 'string' ? names : '';
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

    const classRecordsToCreate = [];
    for (const session of sessions) {
      const attributes = sessionAttributes(session);

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

    if (classRecordsToCreate.length) assignPayrollPeriods(classRecordsToCreate, await fetchPayrollPeriods());

    const createdClassRecords = classRecordsToCreate.length
      ? await createClassRecords(classRecordsToCreate)
      : [];

    await updateRunRecord({ 'Classes Status': 'COMPLETE - Classes found' });

    await updateRunRecord({ 'Instructors Status': 'Started' });
    const instructorRecords = await fetchAllRecords(CONFIG.airtable.instructorsTableId, ['Name']);
    const instructorMap = new Map();
    for (const rec of instructorRecords) {
      const name = String(getField(rec, 'Name') || '').trim().toLowerCase();
      if (name) instructorMap.set(name, rec.id);
    }

    const mtekInstructorSample = sessions
      .map((session) => instructorNameFromSession(session))
      .filter(Boolean)
      .slice(0, 5);
    const airtableInstructorSample = instructorRecords
      .map((rec) => String(getField(rec, 'Name') || '').trim())
      .filter(Boolean)
      .slice(0, 5);
    console.log(`[DEBUG] MTEK instructor sample (first 5): ${JSON.stringify(mtekInstructorSample)}`);
    console.log(`[DEBUG] Airtable Name sample (first 5): ${JSON.stringify(airtableInstructorSample)}`);

    let instructorNotFound = 0;
    for (const classRecord of createdClassRecords) {
      const mtekInstructor = String(getField(classRecord, 'MTEK Instructor') || '')
        .trim()
        .toLowerCase();
      const instructorId = instructorMap.get(mtekInstructor);
      if (instructorId) {
        await patchClassRecord(classRecord.id, { Instructor: [instructorId] });
      } else {
        instructorNotFound += 1;
      }
    }

    await updateRunRecord({ 'Instructors Status': 'COMPLETE - Instructors Assigned' });

    await updateRunRecord({ 'Attendance Status': 'Started' });
    await updateRunRecord({ 'Attendance Status': 'COMPLETE - Attendance found' });

    await updateRunRecord({ 'Studio Status': 'Started' });
    const studioRecords = await fetchAllRecords(CONFIG.airtable.studiosTableId, ['MTEK Location ID']);
    const studioMap = new Map();
    for (const rec of studioRecords) {
      const locationId = String(getField(rec, 'MTEK Location ID') || '').trim();
      if (locationId) studioMap.set(locationId, rec.id);
    }

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
      `# of Instructors not found: ${instructorNotFound}`,
      `# of Studios not found: ${studioNotFound}`,
      `# of Class types not found: ${classTypeNotFound}`,
    ].join(' | ');

    await updateRunRecord({
      Notes: note,
      'OVERALL Status': 'COMPLETE',
    });

    console.log(
      `b.cycle PAYROLL Classes completed for ${CONFIG.recordId}. Created ${createdClassRecords.length} class records.`,
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

export { assignPayrollPeriods };
