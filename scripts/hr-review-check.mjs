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
  hours: 'fldSxDiBejz7REwpX', remove: 'fldFTCN6GKZNk5MkR',
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

/**
 * punches: [{id, type, name, date, hours}]; records: [{source, type, name, date, hours, ignore}]
 * Returns items: [{result, type, name, date, punchHours, reviewHours, punchId, source, detail}]
 */
export function matchItems(punches, records, tolerance = CONFIG.hoursTolerance) {
  const key = (x) => `${x.type}|${normName(x.name)}|${x.date}`;
  const groups = new Map();
  const slot = (k) => {
    if (!groups.has(k)) groups.set(k, { punches: [], records: [] });
    return groups.get(k);
  };
  for (const p of punches) slot(key(p)).punches.push(p);
  for (const r of records) slot(key(r)).records.push(r);

  const items = [];
  for (const g of groups.values()) {
    const ps = [...g.punches];
    const rs = [...g.records];
    // Pair each record with the unused punch of closest hours.
    for (const r of rs) {
      let best = -1;
      for (let i = 0; i < ps.length; i += 1) {
        if (best < 0 || Math.abs(ps[i].hours - r.hours) < Math.abs(ps[best].hours - r.hours)) best = i;
      }
      const p = best >= 0 ? ps.splice(best, 1)[0] : null;
      if (r.ignore) {
        items.push({ result: 'Ignored', type: r.type, name: r.name, date: r.date, punchHours: p?.hours ?? null, reviewHours: r.hours, punchId: p?.id ?? null, source: r.source, detail: 'Marked IGNORE/DO NOT PAY' + (p ? ' (a punch exists for it)' : '') });
      } else if (!p) {
        items.push({ result: 'Review without punch', type: r.type, name: r.name, date: r.date, punchHours: null, reviewHours: r.hours, punchId: null, source: r.source, detail: r.mtekDone ? 'Pushed to MTEK but no punch found' : 'Not pushed to MTEK yet' });
      } else {
        const diff = Math.abs(p.hours - r.hours) > tolerance;
        items.push({ result: diff ? 'Hours differ' : 'Matched', type: r.type, name: r.name, date: r.date, punchHours: p.hours, reviewHours: r.hours, punchId: p.id, source: r.source, detail: diff ? `Punch ${p.hours} h vs review ${r.hours} h` : '' });
      }
    }
    for (const p of ps) {
      items.push({ result: 'Punch without review', type: p.type, name: p.name, date: p.date, punchHours: p.hours, reviewHours: null, punchId: p.id, source: '', detail: 'No review, coaching session or evaluation on this date' });
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
    fields: [PUNCH.date, PUNCH.employee, PUNCH.rateType, PUNCH.hours, PUNCH.remove],
  });
  return recs
    .filter((r) => !r.fields[PUNCH.remove])
    .map((r) => ({
      id: r.id,
      type: r.fields[PUNCH.rateType],
      name: r.fields[PUNCH.employee] || '',
      date: r.fields[PUNCH.date],
      hours: Number(r.fields[PUNCH.hours]) || 0,
    }))
    .filter((p) => TYPES.includes(p.type) && p.date >= start && p.date <= end);
}

async function loadRecords(start, end) {
  const out = [];
  const from = pad(start, -3);
  const to = pad(end, 4);
  const reviews = await listAll(CONFIG.instBase, T.classReviews, {
    formula: `AND(IS_AFTER({${CR.classDate}},'${pad(from, -1)}'),IS_BEFORE({${CR.classDate}},'${pad(to, 1)}'))`,
    fields: [CR.mentor, CR.classDate, CR.reviewIn, CR.ignore, CR.shiftType, CR.hours, CR.mtekDone],
  });
  for (const r of reviews) {
    const type = r.fields[CR.shiftType];
    if (!TYPES.includes(type)) continue;
    const date = localDate(first(r.fields[CR.reviewIn]) || r.fields[CR.classDate]);
    if (date < start || date > end) continue;
    out.push({
      source: `Class Reviews ${r.id}`,
      type,
      name: linkName(r.fields[CR.mentor]),
      date,
      hours: Number(r.fields[CR.hours]) || 0,
      ignore: Boolean(r.fields[CR.ignore]),
      mtekDone: Boolean(r.fields[CR.mtekDone]),
    });
  }
  const evals = await listAll(CONFIG.instBase, T.evals, {
    formula: `AND(IS_AFTER(CREATED_TIME(),'${pad(from, -1)}'),IS_BEFORE(CREATED_TIME(),'${pad(to, 1)}'))`,
    fields: [EV.employee, EV.created, EV.hours, EV.mtekDone],
  });
  for (const r of evals) {
    const hours = Number(r.fields[EV.hours]) || 0;
    if (!(hours > 0) && !r.fields[EV.mtekDone]) continue;
    const date = localDate(r.fields[EV.created]);
    if (date < start || date > end) continue;
    out.push({
      source: `Employee Evaluations ${r.id}`,
      type: 'Instructor Eval',
      name: linkName(r.fields[EV.employee]),
      date,
      hours,
      ignore: false,
      mtekDone: Boolean(r.fields[EV.mtekDone]),
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
    const records = await loadRecords(start, end);
    const items = matchItems(punches, records);
    const count = (r) => items.filter((i) => i.result === r).length;
    const summary = `${start} -> ${end}: ${punches.length} punches, ${records.length} reviews/coaching/evals. Matched ${count('Matched')}, hours differ ${count('Hours differ')}, punch without review ${count('Punch without review')}, review without punch ${count('Review without punch')}, ignored ${count('Ignored')}.`;
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
    const problems = count('Hours differ') + count('Punch without review') + count('Review without punch');
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
