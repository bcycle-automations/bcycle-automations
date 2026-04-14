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
    token: process.env.AIRTABLE_TOKEN,
  },
  mtek: {
    // per request: MarianaTek base is bcycle
    baseUrl: process.env.MTEK_BASE_URL || 'https://bcycle.marianatek.com',
    classesPath: process.env.MTEK_CLASSES_PATH || '/api/class_sessions',
    reservationsPath: process.env.MTEK_RESERVATIONS_PATH || '/api/reservations',
    classTypesPathTemplate: process.env.MTEK_CLASS_TYPES_PATH_TEMPLATE || '/api/class_types/{id}',
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
  let nextUrl = new URL(path, CONFIG.mtek.baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      nextUrl.searchParams.set(key, String(value));
    }
  });

  while (nextUrl) {
    const page = await mtekRequestUrl(nextUrl.toString());
    const pageResults = Array.isArray(page?.results) ? page.results : [];
    allResults.push(...pageResults);

    const next = page?.links?.next;
    nextUrl = next ? new URL(next, CONFIG.mtek.baseUrl) : null;
  }

  return allResults;
}

function getField(record, fieldName) {
  return record?.fields?.[fieldName];
}

function firstInstructorName(session) {
  if (!Array.isArray(session?.instructors) || session.instructors.length === 0) {
    return '';
  }

  return session.instructors[0]?.name || '';
}

function templatePath(pathTemplate, replacements = {}) {
  return Object.entries(replacements).reduce(
    (acc, [key, value]) => acc.replaceAll(`{${key}}`, encodeURIComponent(String(value))),
    pathTemplate,
  );
}

async function resolveClassTypeName(session) {
  const inlineName = session?.class_type?.name;
  if (inlineName) {
    return inlineName;
  }

  const classTypeId = session?.class_type?.id;
  if (!classTypeId) {
    return '';
  }

  const classTypePath = templatePath(CONFIG.mtek.classTypesPathTemplate, { id: classTypeId });
  const classTypeResponse = await mtekRequestPath(classTypePath);
  return classTypeResponse?.name || '';
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
      const classTypeName = await resolveClassTypeName(session);

      classRecordsToCreate.push({
        'MTEK Instructor': firstInstructorName(session),
        'MTEK Class Type': classTypeName,
        'Class Date': localDateTimeString(session?.start_datetime, 'America/Toronto'),
        'Location ID': session?.location?.id ?? null,
        'MTEK ID': session?.id ?? null,
        'Payroll Class log': [CONFIG.recordId],
      });
    }

    const createdClassRecords = classRecordsToCreate.length
      ? await createClassRecords(classRecordsToCreate)
      : [];

    await updateRunRecord({ 'Classes Status': 'COMPLETE - Classes found' });

    await updateRunRecord({ 'Instructors Status': 'Started' });
    const instructorRecords = await fetchAllRecords(CONFIG.airtable.instructorsTableId, ['Zingfit Name']);
    const instructorMap = new Map();
    for (const rec of instructorRecords) {
      const name = String(getField(rec, 'Zingfit Name') || '').trim().toLowerCase();
      if (name) instructorMap.set(name, rec.id);
    }

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
    for (const classRecord of createdClassRecords) {
      const classId = getField(classRecord, 'MTEK ID');
      if (!classId) {
        await patchClassRecord(classRecord.id, { 'Attendance Count (Checked in)': 0 });
        continue;
      }

      const reservations = await fetchPaginatedMtek(CONFIG.mtek.reservationsPath, {
        class_session: classId,
      });

      const checkedInCount = reservations.filter((reservation) => reservation?.status === 'check in').length;

      await patchClassRecord(classRecord.id, {
        'Attendance Count (Checked in)': checkedInCount,
      });
    }

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

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
