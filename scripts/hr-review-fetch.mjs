#!/usr/bin/env node

/**
 * HR Review Fetch
 * Step 1 of the Review Check page: fetches ALL Coaching 1-on-1 (HQ), Coaching 1-on-1
 * (SPINCO HQ), Class Review and Instructor Eval time punches from MarianaTek for one
 * Budget week, across every studio (no location filter), into Time Punches.
 *
 * The per-studio "HR Payroll Time Punches" fetch ignores exactly these four shift types
 * (REVIEW_SHIFT_TYPES), so each punch is imported by one job only. Driven by one
 * "Review Check - Week" record (tblicI8oQqE4GjzBa). Actions logs are public: counts only.
 */

process.env.HR_PUNCHES_SKIP_RUN = '1';
const base = await import('./hr-payroll-time-punches-workflow.mjs');
const {
  CONFIG,
  REVIEW_SHIFT_TYPES,
  airtableRequest,
  createPunchRecords,
  employeeNameFromShift,
  fetchAllRecords,
  fetchPaginatedMtek,
  fetchWindows,
  getField,
  loadInstructorMap,
  localParts,
  normalise,
  paddedBound,
  patchPunchRecords,
  relationshipId,
  shiftTypeNameFromShift,
} = base;
const { eomMembership } = await import('./lib/eom-weekend.mjs');

// MarianaTek shift type IDs (GET /api/shift_types).
export const REVIEW_SHIFT_TYPE_IDS = { 'Coaching 1-on-1 (HQ)': '200', 'Coaching 1-on-1 (SPINCO HQ)': '201', 'Class Review': '300', 'Instructor Eval': '333' };

const RUN_TABLE = 'tblicI8oQqE4GjzBa';
const RUN = { week: 'fld6VrUtYHVJ2TEkB', fetchStatus: 'fldbRvriNeXEnv3q5', notes: 'fld7A2UXoeUPYKvIZ' };
const WEEK_TABLE = 'tblt2pfs356rDVDIa';
const WEEK = { start: 'fldkF8NgX28kgbuAl', end: 'fldxp99UioLif2Foy' };
const PUNCH_RUN_FIELD = 'fld967cC1Y35mEWbj';

/** shifts: [{shift, included}] -> punch rows inside [start, end] whose shift type is one of the four. */
export function toPunches(batches, start, end) {
  const out = [];
  const seen = new Set();
  for (const { results, included } of batches) {
    for (const shift of results) {
      const a = shift?.attributes || {};
      const s = localParts(a.start_datetime);
      if (!s) throw new Error(`MTEK time punch ${shift?.id ?? '(no id)'} has no readable start time. Nothing was imported.`);
      if (s.date < start || s.date > end) continue;
      const rateType = shiftTypeNameFromShift(shift, included);
      if (!REVIEW_SHIFT_TYPES.has(rateType)) continue;
      const mtekId = String(shift?.id ?? '').trim();
      if (!mtekId || seen.has(mtekId)) continue;
      seen.add(mtekId);
      const e = a.end_datetime ? localParts(a.end_datetime) : null;
      out.push({
        mtekId,
        date: s.date,
        timeIn: s.time,
        timeOut: e ? e.time : '',
        locationId: String(relationshipId(shift, 'location') ?? ''),
        employeeName: employeeNameFromShift(shift, included),
        rateType,
      });
    }
  }
  return out;
}

const stamp = () => new Date().toLocaleString('sv-SE', { timeZone: CONFIG.timeZone }).slice(0, 16);

async function listPunches(formula, fields) {
  const out = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ filterByFormula: formula, pageSize: '100' });
    fields.forEach((f) => params.append('fields[]', f));
    if (offset) params.set('offset', offset);
    const page = await airtableRequest({ tableId: CONFIG.airtable.punchesTableId, query: params.toString() });
    out.push(...(page.records || []));
    offset = page.offset || '';
  } while (offset);
  return out;
}

async function run() {
  const a = CONFIG.airtable;
  const runId = CONFIG.recordId;
  if (!a.token) throw new Error('AIRTABLE_TOKEN is required');
  if (!CONFIG.mtek.token) throw new Error('MTEK_API_TOKEN is required');
  if (!runId) throw new Error('AIRTABLE_RECORD_ID is required');

  const patchRun = (fields) => airtableRequest({ method: 'PATCH', tableId: RUN_TABLE, recordId: runId, body: { fields, typecast: true } });
  let existingNotes = '';
  const note = async (text, fields) => {
    await patchRun({ ...fields, [RUN.notes]: `[${stamp()}] ${text}\n\n${existingNotes}`.slice(0, 100000) });
  };

  await patchRun({ [RUN.fetchStatus]: 'Started' });
  try {
    const runRec = await airtableRequest({ tableId: RUN_TABLE, recordId: runId });
    existingNotes = String(getField(runRec, 'Notes') || '');
    const weekId = (getField(runRec, 'Budget week') || [])[0];
    if (!weekId) throw new Error('Run has no Budget week linked');
    const week = await airtableRequest({ tableId: WEEK_TABLE, recordId: weekId });
    const startDate = getField(week, 'Start Date');
    const endDate = getField(week, 'End Date');
    if (!startDate || !endDate) throw new Error('Budget week has no start/end date');

    // One fetch per shift type, no location filter: every studio, only these four types.
    const batches = [];
    for (const id of Object.values(REVIEW_SHIFT_TYPE_IDS)) {
      batches.push(
        await fetchPaginatedMtek(CONFIG.mtek.punchesPath, {
          shift_type: id,
          min_start_datetime: paddedBound(startDate, -1),
          max_start_datetime: paddedBound(endDate, 2),
          include: 'employee.user,shift_type',
        }),
      );
    }
    const mtekPunches = toPunches(batches, startDate, endDate);

    // Dedupe on MTEK ID across the whole table (these types come in through this job only).
    const types = Object.keys(REVIEW_SHIFT_TYPE_IDS);
    const existingRecs = await listPunches(
      `AND(IS_AFTER({Date},'${paddedBound(startDate, -2)}'),IS_BEFORE({Date},'${paddedBound(endDate, 3)}'),OR(${types.map((t) => `{Rate Type}='${t}'`).join(',')}))`,
      ['MTEK ID', 'Time Out', 'Review Check - Week'],
    );
    const existing = new Map(existingRecs.map((r) => [String(getField(r, 'MTEK ID') || '').trim(), r]));

    const toCreate = [];
    const patches = [];
    let skipped = 0;
    for (const p of mtekPunches) {
      const cur = existing.get(p.mtekId);
      if (!cur) {
        toCreate.push({
          Date: p.date,
          'Time In': p.timeIn,
          'Time Out': p.timeOut,
          'MTEK ID': p.mtekId,
          'Location ID': p.locationId,
          'Employee Name': p.employeeName,
          'Rate Type': p.rateType,
          [PUNCH_RUN_FIELD]: [runId],
        });
        continue;
      }
      skipped += 1;
      const fields = {};
      if (!String(getField(cur, 'Time Out') || '') && p.timeOut) fields['Time Out'] = p.timeOut; // blank only: hand edits survive
      if (!(getField(cur, 'Review Check - Week') || []).includes(runId)) fields[PUNCH_RUN_FIELD] = [runId];
      if (Object.keys(fields).length) patches.push({ id: cur.id, fields });
    }

    // Every new punch gets exactly one Payroll period and one EOM, or nothing is imported.
    if (toCreate.length) {
      const windows = [
        { label: 'Payroll period', fix: 'run HR Create Budget week', fieldId: a.punchPeriodFieldId, list: await fetchWindows(a.payrollPeriodsTableId, a.periodStartFieldId, a.periodEndFieldId) },
        { label: 'EOM', fix: 'run HR Create EOM', fieldId: a.punchEomFieldId, list: await fetchWindows(a.eomTableId, a.eomStartFieldId, a.eomEndFieldId) },
      ];
      for (const w of windows) {
        const bad = new Set();
        for (const fields of toCreate) {
          const matches = w.list.filter((x) => x.start <= fields.Date && fields.Date <= x.end);
          if (matches.length !== 1) { bad.add(fields.Date); continue; }
          if (w.label === 'EOM') {
            const m = eomMembership(fields.Date, w.list);
            fields[w.fieldId] = m.ids;
            fields[a.punchEomWeekEndFieldId] = m.viaWeekEnd;
          } else fields[w.fieldId] = [matches[0].id];
        }
        if (bad.size) throw new Error(`Can't give every punch its ${w.label} (${[...bad].sort().join(', ')}). ${w.fix} and re-run. Nothing was imported.`);
      }
    }

    const created = toCreate.length ? await createPunchRecords(toCreate) : [];
    await patchPunchRecords(patches);

    // Employee / Instructor (same matching as the studio fetch), then Rate ("<name> <shift type>").
    const employeeMap = new Map();
    for (const rec of await fetchAllRecords(a.employeesTableId, ['Name'], a.employeesViewId)) {
      const n = normalise(getField(rec, 'Name'));
      if (n && !employeeMap.has(n)) employeeMap.set(n, rec.id);
    }
    const instructorMap = await loadInstructorMap();
    const rateMap = new Map();
    for (const rec of await fetchAllRecords(a.ratesTableId, ['Name'])) {
      const n = normalise(getField(rec, 'Name'));
      if (n && !rateMap.has(n)) rateMap.set(n, rec.id);
    }
    let noEmployee = 0;
    let noRate = 0;
    const links = [];
    for (const punch of created) {
      const name = normalise(getField(punch, 'Employee Name'));
      const fields = {};
      if (employeeMap.get(name)) fields.Employee = [employeeMap.get(name)];
      else if (instructorMap.get(name)) fields[a.punchInstructorFieldId] = [instructorMap.get(name)];
      else noEmployee += 1;
      const rate = rateMap.get(normalise(`${getField(punch, 'Employee Name')} ${getField(punch, 'Rate Type')}`));
      if (rate) fields.Rate = [rate];
      else noRate += 1;
      if (Object.keys(fields).length) links.push({ id: punch.id, fields });
    }
    await patchPunchRecords(links);

    const summary =
      `Fetch ${startDate} -> ${endDate} (all studios, 4 shift types): ${mtekPunches.length} in MTEK, ${created.length} new, ${skipped} already imported, ` +
      `${patches.length} updated, ${noEmployee} employees not found, ${noRate} rates not found. Fix those in Step 3 of the Review Check page.`;
    await note(summary, { [RUN.fetchStatus]: 'COMPLETE' });
    console.log(summary);
  } catch (err) {
    await note(`FETCH FAILED: ${err.message}`, { [RUN.fetchStatus]: 'PROBLEM' }).catch(() => {});
    throw err;
  }
}

if (!process.env.REVIEW_FETCH_SKIP_RUN) {
  run().catch((e) => {
    console.error(e.message);
    process.exit(1);
  });
}
