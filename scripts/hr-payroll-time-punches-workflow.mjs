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
    payrollPeriodsTableId: process.env.AIRTABLE_PAYROLL_PERIODS_TABLE_ID || 'tbl9qw4kqw0BY0DyJ',
    // Field IDs, so a renamed column can't break pay-period assignment.
    periodStartFieldId: 'fldMjpx7WN4tgNrNs',
    periodEndFieldId: 'fldCy4qUWOWq6SZdk',
    punchPeriodFieldId: 'fld4XJAYoen5lCaaM',
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
 * Collects every page plus the sideloaded `included` records (keyed by type+id so
 * names resolve without an extra call each), and proves the result is complete.
 * Anything short of that throws rather than under-counting payroll: a page with
 * no data or paging info, MTEK's total changing between pages (records shifted
 * mid-fetch), or fewer unique punches than MTEK reports. Counting unique IDs also
 * catches a skipped record hidden behind a duplicated one. `request` is
 * injectable so this can be exercised without MTEK.
 */
async function fetchPaginatedMtek(path, params = {}, request = mtekRequest) {
  const byId = new Map();
  const included = new Map();
  let reportedCount = null;
  let currentPage = 1;
  let totalPages = 1;

  while (currentPage <= totalPages) {
    const page = await request(path, { ...params, page: currentPage, page_size: 100 });
    const pages = Number(page?.meta?.pagination?.pages);
    const count = Number(page?.meta?.pagination?.count);

    if (
      !Array.isArray(page?.data) ||
      !Number.isInteger(pages) ||
      pages < 0 ||
      !Number.isInteger(count) ||
      count < 0
    ) {
      throw new Error(
        `MTEK page ${currentPage} came back without its data or paging info, so the fetch can't be confirmed complete. Nothing was imported — re-run the fetch.`,
      );
    }

    if (reportedCount === null) {
      reportedCount = count;
    } else if (count !== reportedCount) {
      throw new Error(
        `MTEK's total changed during the fetch (${reportedCount} -> ${count}), so records may have shifted between pages. Nothing was imported — re-run the fetch.`,
      );
    }

    // A record can land on two pages if data shifts mid-fetch; keep it once.
    for (const item of page.data) byId.set(String(item.id), item);
    for (const item of Array.isArray(page.included) ? page.included : []) {
      included.set(`${item.type}:${item.id}`, item);
    }

    totalPages = pages;
    currentPage += 1;
  }

  if (byId.size !== reportedCount) {
    throw new Error(
      `MTEK reported ${reportedCount} time punches but ${byId.size} were received. Nothing was imported — re-run the fetch.`,
    );
  }

  return { results: [...byId.values()], included };
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
 * Punches already linked to this run record. Keyed by MTEK ID for reconciling
 * against MTEK; the full list also covers any punch added by hand (no MTEK ID),
 * so week totals and the open-punch count include those too.
 */
async function fetchExistingPunches(punchRecordIds) {
  const byMtekId = new Map();
  const all = [];

  for (let i = 0; i < punchRecordIds.length; i += 50) {
    const chunk = punchRecordIds.slice(i, i + 50);
    const params = new URLSearchParams({
      filterByFormula: `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(',')})`,
      pageSize: '50',
    });
    ['MTEK ID', 'Date', 'Time In', 'Time Out', 'Employee Name', 'Hourly rate'].forEach((field) =>
      params.append('fields[]', field),
    );

    const page = await airtableRequest({
      tableId: CONFIG.airtable.punchesTableId,
      query: params.toString(),
    });

    for (const record of page.records || []) {
      const punch = {
        id: record.id,
        mtekId: String(getField(record, 'MTEK ID') || '').trim(),
        date: String(getField(record, 'Date') || ''),
        timeIn: String(getField(record, 'Time In') || ''),
        timeOut: String(getField(record, 'Time Out') || ''),
        employeeName: String(getField(record, 'Employee Name') || ''),
        hourlyRate: Number(firstValue(getField(record, 'Hourly rate'))) || 0,
      };
      all.push(punch);
      if (punch.mtekId) byMtekId.set(punch.mtekId, punch);
    }
  }

  return { byMtekId, all };
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

    // Everything MTEK holds for this studio and week, in local time.
    const mtekPunches = [];
    for (const shift of shifts) {
      const attributes = shift?.attributes || {};
      const start = localParts(attributes.start_datetime);
      if (!start) {
        throw new Error(
          `MTEK time punch ${shift?.id ?? '(no id)'} has no readable start time, so it can't be placed in a week. Nothing was imported — fix it in MTEK and re-run the fetch.`,
        );
      }
      if (start.date < startDate || start.date > endDate) continue;

      const end = attributes.end_datetime ? localParts(attributes.end_datetime) : null;
      mtekPunches.push({
        mtekId: String(shift?.id ?? '').trim(),
        date: start.date,
        timeIn: start.time,
        timeOut: end ? end.time : '',
        locationId: String(relationshipId(shift, 'location') ?? ''),
        employeeName: employeeNameFromShift(shift, included),
        rateType: shiftTypeNameFromShift(shift, included),
      });
    }

    // Everyone works every day, so an empty studio-week is always a mistake —
    // usually the wrong studio (a SPINCO location returns nothing from b.cycle's
    // MTEK) or the wrong dates. Fail loudly rather than report COMPLETE on nothing.
    if (!mtekPunches.length) {
      const studioName = getField(studioRecord, 'Studio name') || studioLinks[0];
      throw new Error(
        `No time punches found in MTEK for ${studioName} (location ${locationId}) between ${startDate} and ${endDate}. Check the studio and the week.`,
      );
    }

    const { byMtekId: existing, all: existingPunches } = await fetchExistingPunches(
      getField(runRecord, 'Time Punch for payroll') || [],
    );

    const punchRecordsToCreate = [];
    const clockOutFills = [];
    const differs = [];
    const seenMtekIds = new Set();
    let duplicatesSkipped = 0;

    for (const punch of mtekPunches) {
      seenMtekIds.add(punch.mtekId);
      const current = punch.mtekId ? existing.get(punch.mtekId) : undefined;

      if (!current) {
        // Total Hours is a formula over Time In/Out, so it is deliberately not
        // written here — that keeps a hand-corrected punch recalculating.
        punchRecordsToCreate.push({
          Date: punch.date,
          'Time In': punch.timeIn,
          'Time Out': punch.timeOut,
          'MTEK ID': punch.mtekId,
          'Location ID': punch.locationId,
          'Employee Name': punch.employeeName,
          'Rate Type': punch.rateType,
          'Budget week - Studio': [CONFIG.recordId],
        });
        continue;
      }

      duplicatesSkipped += 1;

      // Still open at the last fetch, closed in MTEK since. Written only into a
      // blank Time Out, so a hand correction is never overwritten.
      if (!current.timeOut && punch.timeOut) {
        clockOutFills.push({ id: current.id, fields: { 'Time Out': punch.timeOut } });
        current.timeOut = punch.timeOut;
      }

      // Any other disagreement is reported, not overwritten: it may be an MTEK
      // edit made after the fetch, or a deliberate correction made in Airtable.
      if (
        current.date !== punch.date ||
        current.timeIn !== punch.timeIn ||
        current.timeOut !== punch.timeOut
      ) {
        differs.push(
          `${punch.employeeName || current.employeeName} ${punch.date}: ` +
            `Airtable ${current.date} ${current.timeIn}-${current.timeOut || '?'} / ` +
            `MTEK ${punch.date} ${punch.timeIn}-${punch.timeOut || '?'}`,
        );
      }
    }

    const noLongerInMtek = existingPunches
      .filter((punch) => punch.mtekId && !seenMtekIds.has(punch.mtekId))
      .map((punch) => `${punch.employeeName} ${punch.date} ${punch.timeIn}: not in MTEK any more`);

    // Every punch belongs to exactly one Payroll period. Resolve them all before
    // writing anything, so a missing or overlapping period fails the run cleanly.
    if (punchRecordsToCreate.length) {
      const periods = await fetchPayrollPeriods();
      const uncovered = new Set();
      const ambiguous = new Set();
      for (const fields of punchRecordsToCreate) {
        const matches = periods.filter((period) => period.start <= fields.Date && fields.Date <= period.end);
        if (matches.length === 1) fields[CONFIG.airtable.punchPeriodFieldId] = [matches[0].id];
        else (matches.length ? ambiguous : uncovered).add(fields.Date);
      }
      if (uncovered.size || ambiguous.size) {
        const problems = [];
        if (uncovered.size) problems.push(`no Payroll period covers ${[...uncovered].sort().join(', ')}`);
        if (ambiguous.size) problems.push(`more than one Payroll period covers ${[...ambiguous].sort().join(', ')}`);
        throw new Error(
          `Can't give every punch a pay period: ${problems.join('; ')}. Fix the Payroll period table (or run HR Create Budget week) and re-run the fetch. Nothing was imported.`,
        );
      }
    }

    const createdPunches = punchRecordsToCreate.length
      ? await createPunchRecords(punchRecordsToCreate)
      : [];
    await patchPunchRecords(clockOutFills);

    // Open = no clock-out once this run is done, across every punch on the row.
    const openCount =
      createdPunches.filter((punch) => !getField(punch, 'Time Out')).length +
      existingPunches.filter((punch) => !punch.timeOut).length;

    phaseField = 'Employee Status';
    await updateRunRecord({
      'Time punch Status': 'COMPLETE - Time punches found',
      'Time in/out Status': openCount ? 'PROBLEM' : 'COMPLETE - Time in/out found',
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

    // Totals cover the whole row, not just this run's new punches — a re-fetch
    // that fills a clock-out changes the week's hours without creating anything.
    for (const punch of existingPunches) {
      const hours = hoursBetween(punch.timeIn, punch.timeOut);
      totalHours += hours;
      totalWages += hours * punch.hourlyRate;
    }

    const listed = (label, lines) => {
      if (!lines.length) return [];
      const shown = lines.slice(0, 25).map((line) => `- ${line}`);
      if (lines.length > 25) shown.push(`- ...and ${lines.length - 25} more`);
      return [`${label}:`, ...shown];
    };

    const summary = [
      `# of Time punches in MTEK: ${mtekPunches.length}`,
      `# of New punches: ${createdPunches.length}`,
      `# of Duplicates skipped: ${duplicatesSkipped}`,
      `# of Clock-outs filled: ${clockOutFills.length}`,
      `# of Punches with no clock-out: ${openCount}`,
      `# of Differs from MTEK: ${differs.length}`,
      `# of No longer in MTEK: ${noLongerInMtek.length}`,
      `# of Employees not found: ${employeeNotFound}`,
      `# of Rates not found: ${rateNotFound}`,
      `Week total hours: ${totalHours.toFixed(2)}`,
      `Week total wages: $${totalWages.toFixed(2)}`,
    ].join(' | ');

    const note = [
      summary,
      ...listed('Differs from MTEK (Airtable kept, not overwritten)', differs),
      ...listed('No longer in MTEK', noLongerInMtek),
    ].join('\n');

    await updateRunRecord({
      'Rate type Status': rateNotFound ? 'PROBLEM' : 'COMPLETE - Rates assigned',
      // COMPLETE only when nothing needs a human: an unmatched employee or rate,
      // or a punch with no clock-out, means the payroll numbers aren't final.
      'Overall Status': employeeNotFound || rateNotFound || openCount ? 'PROBLEM' : 'COMPLETE',
      Notes: await appendNote(note),
    });

    // Actions logs are public on this repo: counts only. Names, hours and the
    // wage total stay in Notes, which is private to Airtable.
    console.log(
      `HR Payroll Time Punches completed for ${CONFIG.recordId}. ${mtekPunches.length} punches in MTEK, ` +
        `${createdPunches.length} new, ${duplicatesSkipped} duplicates skipped, ${clockOutFills.length} clock-outs filled, ` +
        `${openCount} with no clock-out, ${differs.length} differ from MTEK, ${noLongerInMtek.length} no longer in MTEK, ` +
        `${employeeNotFound} employees not found, ${rateNotFound} rates not found.`,
    );
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

// Runs whenever this file is executed. HR_PUNCHES_SKIP_RUN=1 lets the fetch be
// imported and exercised on its own without syncing anything.
if (process.env.HR_PUNCHES_SKIP_RUN !== '1') {
  run().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export { fetchPaginatedMtek };
