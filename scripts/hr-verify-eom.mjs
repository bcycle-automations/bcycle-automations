#!/usr/bin/env node

/**
 * HR Verify EOM
 * Month-end verification for ONE HR "EOM" record (clicked from the "Verify
 * month-end" link, bridged by Make). It re-derives, from first principles, what
 * should be in that EOM and compares with what is actually linked:
 *
 *   - every time punch / class whose own date is in the EOM is linked to it, and
 *     is attributed to exactly one payroll period (the one covering its date)
 *   - (time punches only) every punch whose week (ending Saturday) finishes in the
 *     EOM is linked to it AND has "In EOM via week-end rule" ticked
 *   - nothing is linked to the EOM that doesn't belong (by date or by the rule)
 *   - the checkbox isn't ticked where the rule doesn't apply
 *   - the EOM's own live checks (date range, no employee, ...) are reported
 *
 * It doesn't just report: anything it finds wrong (a missing or wrong EOM link,
 * a wrong week-end checkbox) it FIXES using the shared rule, then counts it as
 * fixed. The month only reads PROBLEM if something couldn't be put right (e.g. a
 * record with no date, or the EOM missing from the synced base). The EOM's own
 * live checks (date range, no employee, ...) are listed as "still open" for the
 * people closing the month, but don't by themselves make the result PROBLEM.
 *
 * Time punches live in the HR base; classes in HR - Instructors, whose EOM is a
 * synced copy matched here by Start Date. The result is written back to the HR
 * EOM record (Month-end verification / notes / verified at) and syncs across.
 * "Month-end verification status" goes Running at the start (clearing the old
 * result) and Complete at the end, or FAILED if the run itself errors, so the
 * EOM record always shows whether the automation has finished.
 * Nothing but those four fields is ever written.
 */

import { eomMembership, addDays } from './lib/eom-weekend.mjs';
import { TARGETS, airtable, fetchAll, loadMonths, loadPeriods, localDay, patchInBatches } from './lib/eom-targets.mjs';

const token = process.env.AIRTABLE_TOKEN;
const recordId = process.env.AIRTABLE_RECORD_ID;

const HR = TARGETS.punches;
const EOM_FIELD = {
  name: 'fldPpqbbTxeNZ1zmh',
  status: 'fldKLk86LHgL79KzP',
  verification: 'fld9PlAxJVO8nshbb',
  notes: 'fldaCqFd8ruPm3rIK',
  verifiedAt: 'fld1BMmklfRCZIksm',
  // The EOM's own live checks, reported alongside.
  checks: {
    'Date Range Check': 'fldkHcKnKNub8X8tG',
    'No Employee Check': 'fldrQCUYgBkyBBao6',
    'No Rate Check': 'flduqb6SVwaIMigpx',
    '$0 Wage Check': 'fldOr6QBBm9t9Mh4l',
    'No Desjardins ID Check': 'fldnU5W687xBE5lWV',
    'No Clock Out Check': 'fldbSRJx0stiTvJod',
  },
};
const SAMPLE = 8;

async function verifyTarget(target, eom, notes) {
  const eomName = eom.name;
  const records = await fetchAll({
    token,
    baseId: target.baseId,
    tableId: target.tableId,
    fieldIds: [target.dateFieldId, target.eomLinkFieldId, target.flagFieldId, target.period.linkFieldId],
    // Anything dated in (or up to a week before) the EOM, or already linked to it.
    filter: `OR(AND(IS_AFTER({${target.dateFieldId}}, '${addDays(eom.start, -9)}'), IS_BEFORE({${target.dateFieldId}}, '${addDays(eom.end, 2)}')), FIND('${eomName}', ARRAYJOIN({${target.eomLinkFieldId}})))`,
  });
  const months = await loadMonths(token, target);
  const periods = await loadPeriods(token, target);
  const thisMonth = months.find((m) => m.start === eom.start && m.end === eom.end);
  if (!thisMonth) {
    notes.problems.push(`${target.label}: this EOM (${eom.start} -> ${eom.end}) isn't in ${target.baseId}'s EOM table yet — check the sync.`);
    return;
  }

  const missing = [];
  const wronglyIn = [];
  const flagWrong = [];
  const noPeriod = [];
  const wrongPeriod = [];
  const unfixable = [];
  const fixes = new Map(); // record id -> fields to write
  const fix = (id, fields) => fixes.set(id, { ...(fixes.get(id) || {}), ...fields });
  const datesSeen = new Set();
  let belong = 0;
  let viaRule = 0;
  let inMonthByDate = 0;
  let withPeriod = 0;

  for (const record of records) {
    const day = localDay(record.fields?.[target.dateFieldId], target.dateIsDateTime);
    const linkedIds = record.fields?.[target.eomLinkFieldId] || [];
    const linked = linkedIds.includes(thisMonth.id);
    const flag = Boolean(record.fields?.[target.flagFieldId]);
    if (!day) {
      if (linked) unfixable.push(`${record.id} (linked here but has no date)`);
      continue;
    }
    const membership = eomMembership(day, months, { weekEnd: target.weekEndRule });
    const shouldBeIn = membership.ids.includes(thisMonth.id);

    // --- EOM attribution -------------------------------------------------
    if (shouldBeIn) {
      belong += 1;
      if (membership.extra.some((m) => m.id === thisMonth.id)) viaRule += 1;
      let broken = false;
      if (!linked) {
        missing.push(`${day} ${record.id}`);
        broken = true;
      }
      if (flag !== membership.viaWeekEnd) {
        flagWrong.push(`${day} ${record.id}`);
        broken = true;
      }
      if (broken) {
        if (membership.own.length > 1) unfixable.push(`${day} ${record.id} (more than one EOM covers this date)`);
        else fix(record.id, { [target.eomLinkFieldId]: membership.ids, [target.flagFieldId]: membership.viaWeekEnd });
      }
    } else if (linked) {
      wronglyIn.push(`${day} ${record.id}`);
      fix(record.id, { [target.eomLinkFieldId]: membership.ids, [target.flagFieldId]: membership.viaWeekEnd });
    }

    // --- Payroll period attribution (records dated inside this EOM) ------
    if (membership.own.some((m) => m.id === thisMonth.id)) {
      inMonthByDate += 1;
      datesSeen.add(day);
      const covering = periods.filter((pr) => pr.start <= day && day <= pr.end);
      const current = record.fields?.[target.period.linkFieldId] || [];
      if (covering.length !== 1) {
        unfixable.push(`${day} ${record.id} (${covering.length ? 'more than one' : 'no'} payroll period covers this date)`);
      } else if (current.length === 1 && current[0] === covering[0].id) {
        withPeriod += 1;
      } else {
        (current.length ? wrongPeriod : noPeriod).push(`${day} ${record.id}`);
        fix(record.id, { [target.period.linkFieldId]: [covering[0].id] });
        withPeriod += 1;
      }
    }
  }

  // Put right what the rules say is wrong, then it's no longer a problem.
  const updates = [...fixes].map(([id, fields]) => ({ id, fields }));
  if (updates.length) await patchInBatches({ token, baseId: target.baseId, tableId: target.tableId, updates });

  notes.lines.push(
    `${target.label}: ${inMonthByDate} dated in this month, all with an EOM and a payroll period` +
      (target.weekEndRule ? ` — plus ${viaRule} pulled in by the week-end rule (${belong} belong to this EOM in total).` : '.'),
  );
  if (updates.length) {
    const parts = [
      missing.length && `${missing.length} added to this EOM`,
      wronglyIn.length && `${wronglyIn.length} removed from it`,
      flagWrong.length && `${flagWrong.length} week-end checkbox(es) corrected`,
      noPeriod.length && `${noPeriod.length} given a payroll period`,
      wrongPeriod.length && `${wrongPeriod.length} payroll period(s) corrected`,
    ].filter(Boolean);
    notes.lines.push(`${target.label}: fixed ${updates.length} record(s) — ${parts.join(', ')}.`);
  }
  if (unfixable.length) {
    notes.problems.push(
      `${target.label}: ${unfixable.length} record(s) couldn't be put right: ${unfixable.slice(0, SAMPLE).join('; ')}${unfixable.length > SAMPLE ? `; +${unfixable.length - SAMPLE} more` : ''}`,
    );
  }

  // Days with nothing at all usually mean an import hasn't run (or a closed day).
  // Listed for the month-end team; not a placement failure.
  const today = new Date().toISOString().slice(0, 10);
  const lastDay = eom.end < today ? eom.end : today;
  const emptyDays = [];
  for (let d = eom.start; d <= lastDay; d = addDays(d, 1)) if (!datesSeen.has(d)) emptyDays.push(d);
  if (emptyDays.length) {
    notes.open.push(`${target.label}: nothing dated ${emptyDays.slice(0, 12).join(', ')}${emptyDays.length > 12 ? ` (+${emptyDays.length - 12} more)` : ''} — check the import ran for those days`);
  }
}

function setEom(fields) {
  return airtable({ token, baseId: HR.baseId, tableId: HR.eomTableId, recordId, method: 'PATCH', body: { fields } });
}

async function run() {
  if (!token) throw new Error('Missing required environment variable: AIRTABLE_TOKEN');
  if (!recordId) throw new Error('Missing required environment variable: AIRTABLE_RECORD_ID');

  const eomRecord = await airtable({
    token,
    baseId: HR.baseId,
    tableId: HR.eomTableId,
    recordId,
    query: `returnFieldsByFieldId=true`,
  });
  await setEom({
    [EOM_FIELD.status]: 'Running',
    [EOM_FIELD.verification]: null,
    [EOM_FIELD.notes]: null,
  });
  const f = eomRecord.fields || {};
  const eom = { id: recordId, name: f[EOM_FIELD.name], start: f[HR.eomStartFieldId], end: f[HR.eomEndFieldId] };
  if (!eom.start || !eom.end || !eom.name) throw new Error(`EOM ${recordId} has no name/Start/End date.`);

  const notes = { lines: [`EOM ${eom.start} -> ${eom.end}`], problems: [], open: [] };

  // HR's own record ids for the punches; classes resolve their own EOM by dates.
  await verifyTarget(HR, eom, notes);
  await verifyTarget(TARGETS.classes, eom, notes);

  const badChecks = Object.entries(EOM_FIELD.checks)
    .filter(([, id]) => ['ISSUE', 'PROBLEM'].includes(String(f[id] ?? '')))
    .map(([label, id]) => `${label} reads ${f[id]}`);

  const ok = notes.problems.length === 0;
  const text = [
    ...notes.lines,
    '',
    ok ? 'ALL GOOD — every time punch and class is in the right EOM (anything wrong was fixed above).' : 'PROBLEM — these could not be put right:',
    ...notes.problems.map((p) => `- ${p}`),
    ...(badChecks.length || notes.open.length
      ? ['', "Still open for the month-end team (doesn't change the result above):", ...notes.open.map((o) => `- ${o}`), ...badChecks.map((c) => `- ${c}`)]
      : []),
  ].join('\n');
  console.log(text);

  await setEom({
    [EOM_FIELD.status]: 'Complete',
    [EOM_FIELD.verification]: ok ? 'ALL GOOD' : 'PROBLEM',
    [EOM_FIELD.notes]: text,
    [EOM_FIELD.verifiedAt]: new Date().toISOString(),
  });
}

run().catch(async (error) => {
  console.error(error);
  // Make the failure visible on the EOM record instead of leaving it on "Running".
  if (token && recordId) {
    await setEom({
      [EOM_FIELD.status]: 'FAILED',
      [EOM_FIELD.notes]: `The verification action failed: ${String(error?.message || error).slice(0, 1500)}`,
    }).catch(() => {});
  }
  process.exit(1);
});
