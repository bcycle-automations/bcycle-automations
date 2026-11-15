#!/usr/bin/env node

/**
 * HR Review Check
 * Matches one Budget week's Coaching 1-on-1 (HQ / SPINCO HQ), Class Review and
 * Instructor Eval time punches (HR base) against the Class Reviews and Employee
 * Evaluations that were pushed to MTEK (HR - Instructors base).
 *
 * Driven by one "Review Check - Week" record (HR base, tblicI8oQqE4GjzBa).
 * Match key: shift type + employee name + local date. Hours are compared and a
 * difference is its own result ("Hours differ"), never silently accepted.
 * The run's items are derived data: they are rebuilt from scratch on every run.
 * Notes carries counts only (Actions logs are public).
 */

const CONFIG = {
  token: process.env.AIRTABLE_TOKEN,
  recordId: process.env.AIRTABLE_RECORD_ID,
  hrBase: process.env.AIRTABLE_BASE_ID || 'appiwfeujJzUZPPBx',
  instBase: process.env.AIRTABLE_INSTRUCTORS_BASE_ID || 'appBC0Ja4B5LKbZLW',
  timeZone: 'America/Toronto',
  hoursTolerance: 0.05,
  dryRun: process.env.REVIEW_CHECK_DRY_RUN === '1',
};

const T = {
  runs: 'tblicI8oQqE4GjzBa',
  items: 'tblwaGbPKyPCy3daH',
  weeks: 'tblt2pfs356rDVDIa',
  punches: 'tblVxt2W7NanQmJFR',
  classReviews: 'tbl2WKLLg5oIMRe1R',
  evals: 'tblIQ9QCGE7KkSSJm',
};

const RUN = { week: 'fld6VrUtYHVJ2TEkB', status: 'fld4T0Wu28IhyMqgK', notes: 'fld7A2UXoeUPYKvIZ', items: 'fldi6ImrakLFQmzv5' };
const WEEK = { start: 'fldkF8NgX28kgbuAl', end: 'fldxp99UioLif2Foy' };
const ITEM = {
  name: 'fldKfRb0NX05zFowq', run: 'fldZDQxlmEhdMtsyk', type: 'fldO1EFiT8E1WSjoZ', result: 'fldgcA7Vc1vA77vlk',
  date: 'fldZX9KLZPElprsbz', employee: 'fldXUUAo7mR9d0PCB', punchHours: 'fldOnAcYK7d97Gb7J',
  reviewHours: 'fldIcQzTa4HrMtLh0', punch: 'fldndC0kXl8k5wFo4', source: 'fld4xaf02wQHKLzxg', detail: 'fldYfy8baIfhE3Oso',
};
const PUNCH = {
  date: 'fldwxo5JqKNOf4KvY', employee: 'fld19AyljflWBAFH0', rateType: 'fldPfC30I2xFuDbPT',
  hours: 'fldSxDiBejz7REwpX', remove: 'fldFTCN6GKZNk5MkR', issues: 'fldQGFi71K3gNUTTm', ok: 'fldvaUwPOWgpo4TjL', mtekId: 'fldmvjHVL8Q1BMLez',
};
const CR = {
  mentor: 'fld1I0hc2cj97bXjQ', classDate: 'fldo22jFhbWwYT9on', reviewIn: 'flds95Ttaerz5iQnP',
  ignore: 'fldQiscLPt7rG4zdn', shiftType: 'flddrWRNxL42Q2sqn', hours: 'fldP2KOy49v4vVhXM', mtekDone: 'fldhWYgpodcHaqKYn',
};
const EV = { employee: 'fldW5YwWwyMhMTzOP', created: 'fld8Qmc3ZeyIwlykY', hours: 'fldgC6IVtulPpca1M', mtekDone: 'fldGHRnzsYPvlpnW4' };

export const TYPES = ['Coaching 1-on-1 (HQ)', 'Coaching 1-on-1 (SPINCO HQ)', 'Class Review', 'Instructor Eval'];

// ---------- pure helpers (exported for tests) ----------

export function normName(value) {
  return String(value ?? '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

export function localDate(iso, timeZone = CONFIG.timeZone) {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  return new Intl.DateTimeFormat('en-CA', { timeZone, year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
}

/** "MTEK done - 2026-11-13 - In 00:00 - Out 01:00 - Coaching 1-on-1 (HQ) (MTEK shift 10541)" -> "10541" */
export function shiftIdOf(text) {
  const m = String(text ?? '').match(/MTEK shift\s+(\d+)/i);
  return m ? m[1] : '';
}

/**
 * Pairs each punch with the review / coaching session / evaluation that created it, by MTEK shift ID.
 * (The push creates the MTEK shift on the day it is pushed, with a placeholder start time, so neither the
 * session date nor the employee name can be used to match: only the shift ID the push writes back.)
 *
 * punches: [{id, mtekId, type, name, date, hours, ok}]
 * records: [{source, type, name, shiftId, hours, ignore, sessionDate}]
 */
export function matchItems(punches, records, tolerance = CONFIG.hoursTolerance) {
  const byShift = new Map(records.filter((r) => r.shiftId).map((r) => [r.shiftId, r]));
  const items = [];
  for (const p of punches) {
    const r = p.mtekId ? byShift.get(p.mtekId) : undefined;
    const base = { type: p.type, name: p.name, date: p.date, punchHours: p.hours, punchId: p.id };
    if (!r) {
      // Accepted with the OK button on the Review Check page: no review is needed for this punch.
      items.push(
        p.ok
          ? { ...base, result: 'Ignored', reviewHours: null, source: '', detail: 'Accepted without review (OK)' }
          : { ...base, result: 'Punch without review', reviewHours: null, source: '', detail: 'This MTEK shift was not pushed from a review, coaching session or evaluation' },
      );
    } else if (r.ignore) {
      items.push({ ...base, result: 'Ignored', reviewHours: r.hours, source: r.source, detail: 'Review is marked IGNORE/DO NOT PAY' });
    } else {
      const diff = Math.abs(p.hours - r.hours) > tolerance;
      items.push({
        ...base,
        result: diff ? 'Hours differ' : 'Matched',
        reviewHours: r.hours,
        source: r.source,
        detail: diff ? `Punch ${p.hours} h vs review ${r.hours} h` : r.sessionDate ? `Session ${r.sessionDate}` : '',
      });
    }
  }
  return items.sort((a, b) => a.date.localeCompare(b.date) || a.name.localeCompare(b.name));
}

// ---------- Airtable ----------

async function at(baseId, path, { method = 'GET', body, query } = {}) {
  const url = new URL(`https://api.airtable.com/v0/${baseId}/${path}`);
  for (const [k, v] of Object.entries(query || {})) {
    if (Array.isArray(v)) v.forEach((x) => url.searchParams.append(k, x));
    else url.searchParams.set(k, v);
  }
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const res = await fetch(url, {
      method,
      headers: { Authorization: `Bearer ${CONFIG.token}`, 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (res.status === 429) { await new Promise((r) => setTimeout(r, 1500 * (attempt + 1))); continue; }
    const text = await res.text();
    if (!res.ok) throw new Error(`Airtable ${method} ${path} -> ${res.status}: ${text.slice(0, 300)}`);
    return text ? JSON.parse(text) : {};
  }
  throw new Error(`Airtable ${method} ${path}: rate limited`);
}

async function listAll(baseId, tableId, { formula, fields }) {
  const out = [];
  let offset;
  do {
    const res = await at(baseId, tableId, {
      query: { filterByFormula: formula, returnFieldsByFieldId: 'true', 'fields[]': fields, pageSize: '100', ...(offset ? { offset } : {}) },
    });
    out.push(...res.records);
    offset = res.offset;
  } while (offset);
  return out;
}

const first = (v) => (Array.isArray(v) ? v[0] : v);
const linkName = (v) => { const x = first(v); return typeof x === 'string' ? x : x?.name || ''; };

function pad(dateStr, days) {
  const d = new Date(`${dateStr}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

async function loadPunches(start, end) {
  const recs = await listAll(CONFIG.hrBase, T.punches, {
    formula: `AND(IS_AFTER({${PUNCH.date}},'${pad(start, -1)}'),IS_BEFORE({${PUNCH.date}},'${pad(end, 1)}'),OR(${TYPES.map((t) => `{${PUNCH.rateType}}='${t}'`).join(',')}))`,
    fields: [PUNCH.date, PUNCH.employee, PUNCH.rateType, PUNCH.hours, PUNCH.remove, PUNCH.issues, PUNCH.ok, PUNCH.mtekId],
  });
  return recs
    .filter((r) => !r.fields[PUNCH.remove])
    .map((r) => ({
      id: r.id,
      type: r.fields[PUNCH.rateType],
      name: r.fields[PUNCH.employee] || '',
      date: r.fields[PUNCH.date],
      hours: Number(r.fields[PUNCH.hours]) || 0,
      mtekId: String(r.fields[PUNCH.mtekId] || '').trim(),
      issues: String(r.fields[PUNCH.issues] || ''),
      ok: Boolean(r.fields[PUNCH.ok]),
    }))
    .filter((p) => TYPES.includes(p.type) && p.date >= start && p.date <= end);
}

// Every review / coaching session / evaluation that was pushed to MTEK carries "(MTEK shift N)" in its
// "MTEK done" text. Those few records are all that is needed: punches are matched by shift ID.
async function loadRecords() {
  const out = [];
  const reviews = await listAll(CONFIG.instBase, T.classReviews, {
    formula: `{${CR.mtekDone}}!=''`,
    fields: [CR.mentor, CR.classDate, CR.reviewIn, CR.ignore, CR.shiftType, CR.hours, CR.mtekDone],
  });
  for (const r of reviews) {
    const shiftId = shiftIdOf(r.fields[CR.mtekDone]);
    if (!shiftId) continue;
    out.push({
      source: `Class Reviews ${r.id}`,
      type: r.fields[CR.shiftType],
      name: linkName(r.fields[CR.mentor]),
      shiftId,
      hours: Number(r.fields[CR.hours]) || 0,
      ignore: Boolean(r.fields[CR.ignore]),
      sessionDate: localDate(first(r.fields[CR.reviewIn]) || r.fields[CR.classDate]),
    });
  }
  const evals = await listAll(CONFIG.instBase, T.evals, {
    formula: `{${EV.mtekDone}}!=''`,
    fields: [EV.employee, EV.created, EV.hours, EV.mtekDone],
  });
  for (const r of evals) {
    const shiftId = shiftIdOf(r.fields[EV.mtekDone]);
    if (!shiftId) continue;
    out.push({
      source: `Employee Evaluations ${r.id}`,
      type: 'Instructor Eval',
      name: linkName(r.fields[EV.employee]),
      shiftId,
      hours: Number(r.fields[EV.hours]) || 0,
      ignore: false,
      sessionDate: localDate(r.fields[EV.created]),
    });
  }
  return out;
}

async function run() {
  if (!CONFIG.token) throw new Error('AIRTABLE_TOKEN is required');
  if (!CONFIG.recordId) throw new Error('AIRTABLE_RECORD_ID is required');

  const runRec = await at(CONFIG.hrBase, `${T.runs}/${CONFIG.recordId}`, { query: { returnFieldsByFieldId: 'true' } });
  let existingNotes = String(runRec.fields[RUN.notes] || '');
  const stamp = () => new Date().toLocaleString('sv-SE', { timeZone: CONFIG.timeZone }).slice(0, 16);
  const writeRun = (fields, note) =>
    at(CONFIG.hrBase, `${T.runs}/${CONFIG.recordId}`, {
      method: 'PATCH',
      body: { fields: { ...fields, [RUN.notes]: `[${stamp()}] ${note}\n\n${existingNotes}`.slice(0, 100000) } },
    });

  await at(CONFIG.hrBase, `${T.runs}/${CONFIG.recordId}`, { method: 'PATCH', body: { fields: { [RUN.status]: 'Started' } } });
  try {
    const weekId = first(runRec.fields[RUN.week]);
    if (!weekId) throw new Error('Run has no Budget week linked');
    const week = await at(CONFIG.hrBase, `${T.weeks}/${weekId}`, { query: { returnFieldsByFieldId: 'true' } });
    const start = week.fields[WEEK.start];
    const end = week.fields[WEEK.end];
    if (!start || !end) throw new Error('Budget week has no start/end date');

    const punches = await loadPunches(start, end);
    const records = await loadRecords();
    const items = matchItems(punches, records);
    const count = (r) => items.filter((i) => i.result === r).length;
    // Payroll issues per punch (no employee / rate / Desjardins ID / clock out, $0 wages) come from the
    // punch's own `Payroll issues` formula; the page lists them live and fixes them in place.
    const punchIssues = punches.filter((p) => p.issues).length;
    const summary = `${start} -> ${end}: ${punches.length} punches, ${records.length} reviews/coaching/evals. Matched ${count('Matched')}, hours differ ${count('Hours differ')}, punch without review ${count('Punch without review')}, ignored ${count('Ignored')}, punches with payroll issues ${punchIssues}.`;
    if (CONFIG.dryRun) {
      console.log(summary);
      return;
    }

    // Rebuild: remove the run's previous items, then write fresh ones.
    const oldIds = [...new Set(runRec.fields[RUN.items] || [])];
    for (let i = 0; i < oldIds.length; i += 10) {
      await at(CONFIG.hrBase, T.items, { method: 'DELETE', query: { 'records[]': oldIds.slice(i, i + 10) } });
    }
    for (let i = 0; i < items.length; i += 10) {
      await at(CONFIG.hrBase, T.items, {
        method: 'POST',
        body: {
          typecast: true,
          records: items.slice(i, i + 10).map((it) => ({
            fields: {
              [ITEM.name]: `${it.date} ${it.name} - ${it.type}`,
              [ITEM.run]: [CONFIG.recordId],
              [ITEM.type]: it.type,
              [ITEM.result]: it.result,
              [ITEM.date]: it.date,
              [ITEM.employee]: it.name,
              ...(it.punchHours != null ? { [ITEM.punchHours]: it.punchHours } : {}),
              ...(it.reviewHours != null ? { [ITEM.reviewHours]: it.reviewHours } : {}),
              ...(it.punchId ? { [ITEM.punch]: [it.punchId] } : {}),
              [ITEM.source]: it.source,
              [ITEM.detail]: it.detail,
            },
          })),
        },
      });
    }
    const problems = count('Hours differ') + count('Punch without review') + punchIssues;
    await writeRun({ [RUN.status]: problems ? 'PROBLEM' : 'COMPLETE' }, summary);
    console.log(summary);
  } catch (err) {
    await writeRun({ [RUN.status]: 'PROBLEM' }, `FAILED: ${err.message}`).catch(() => {});
    throw err;
  }
}

if (!process.env.REVIEW_CHECK_SKIP_RUN) {
  run().catch((err) => {
    console.error(err.message);
    process.exit(1);
  });
}
