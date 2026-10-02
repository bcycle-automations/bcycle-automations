#!/usr/bin/env node

/**
 * HR Verify EOM
 * Month-end verification for ONE HR "EOM" record (clicked from the "Verify
 * month-end" link, bridged by Make). It re-derives, from first principles, what
 * should be in that EOM and compares with what is actually linked:
 *
 *   - every time punch / class whose own date is in the EOM is linked to it
 *   - every record whose week (ending Saturday) finishes in the EOM is linked to
 *     it AND has "In EOM via week-end rule" ticked
 *   - nothing is linked to the EOM that doesn't belong (by date or by the rule)
 *   - the checkbox isn't ticked where the rule doesn't apply
 *   - the EOM's own live checks (date range, no employee, ...) are reported
 *
 * Time punches live in the HR base; classes in HR - Instructors, whose EOM is a
 * synced copy matched here by Start Date. The result is written back to the HR
 * EOM record (Month-end verification / notes / verified at) and syncs across.
 * Nothing but those three fields is ever written.
 */

import { eomMembership, addDays } from './lib/eom-weekend.mjs';
import { TARGETS, airtable, fetchAll, loadMonths, localDay } from './lib/eom-targets.mjs';

const token = process.env.AIRTABLE_TOKEN;
const recordId = process.env.AIRTABLE_RECORD_ID;

const HR = TARGETS.punches;
const EOM_FIELD = {
  name: 'fldPpqbbTxeNZ1zmh',
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
    fieldIds: [target.dateFieldId, target.eomLinkFieldId, target.flagFieldId],
    // Anything dated in (or up to a week before) the EOM, or already linked to it.
    filter: `OR(AND(IS_AFTER({${target.dateFieldId}}, '${addDays(eom.start, -9)}'), IS_BEFORE({${target.dateFieldId}}, '${addDays(eom.end, 2)}')), FIND('${eomName}', ARRAYJOIN({${target.eomLinkFieldId}})))`,
  });
  const months = await loadMonths(token, target);
  const thisMonth = months.find((m) => m.start === eom.start && m.end === eom.end);
  if (!thisMonth) {
    notes.problems.push(`${target.label}: this EOM (${eom.start} -> ${eom.end}) isn't in ${target.baseId}'s EOM table yet — check the sync.`);
    return;
  }

  const missing = [];
  const wronglyIn = [];
  const flagWrong = [];
  let belong = 0;
  let viaRule = 0;

  for (const record of records) {
    const day = localDay(record.fields?.[target.dateFieldId], target.dateIsDateTime);
    const linked = (record.fields?.[target.eomLinkFieldId] || []).includes(thisMonth.id);
    const flag = Boolean(record.fields?.[target.flagFieldId]);
    if (!day) {
      if (linked) wronglyIn.push(`${record.id} (no date)`);
      continue;
    }
    const membership = eomMembership(day, months);
    const shouldBeIn = membership.ids.includes(thisMonth.id);
    if (shouldBeIn) {
      belong += 1;
      if (membership.extra.some((m) => m.id === thisMonth.id)) viaRule += 1;
      if (!linked) missing.push(`${day} ${record.id}`);
      if (flag !== membership.viaWeekEnd) flagWrong.push(`${day} ${record.id} (checkbox ${flag ? 'ticked' : 'empty'}, should be ${membership.viaWeekEnd ? 'ticked' : 'empty'})`);
    } else if (linked) {
      wronglyIn.push(`${day} ${record.id}`);
    }
  }

  notes.lines.push(`${target.label}: ${belong} belong to this EOM (${belong - viaRule} by date, ${viaRule} only by the week-end rule).`);
  const report = (list, text) => {
    if (!list.length) return;
    notes.problems.push(`${target.label}: ${list.length} ${text}: ${list.slice(0, SAMPLE).join('; ')}${list.length > SAMPLE ? `; +${list.length - SAMPLE} more` : ''}`);
  };
  report(missing, 'not linked to this EOM but should be');
  report(wronglyIn, 'linked to this EOM but do not belong (by date or by the week-end rule)');
  report(flagWrong, 'with the week-end checkbox wrong');
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
  const f = eomRecord.fields || {};
  const eom = { id: recordId, name: f[EOM_FIELD.name], start: f[HR.eomStartFieldId], end: f[HR.eomEndFieldId] };
  if (!eom.start || !eom.end || !eom.name) throw new Error(`EOM ${recordId} has no name/Start/End date.`);

  const notes = { lines: [`EOM ${eom.start} -> ${eom.end}`], problems: [] };

  // HR's own record ids for the punches; classes resolve their own EOM by dates.
  await verifyTarget(HR, eom, notes);
  await verifyTarget(TARGETS.classes, eom, notes);

  const badChecks = Object.entries(EOM_FIELD.checks)
    .filter(([, id]) => ['ISSUE', 'PROBLEM'].includes(String(f[id] ?? '')))
    .map(([label, id]) => `${label} reads ${f[id]}`);
  if (badChecks.length) notes.problems.push(`Live EOM checks not clear: ${badChecks.join('; ')}`);

  const ok = notes.problems.length === 0;
  const text = [...notes.lines, '', ok ? 'Nothing missed — every time punch and class is in the right EOM.' : 'PROBLEMS:', ...notes.problems.map((p) => `- ${p}`)].join('\n');
  console.log(text);

  await airtable({
    token,
    baseId: HR.baseId,
    tableId: HR.eomTableId,
    recordId,
    method: 'PATCH',
    body: {
      fields: {
        [EOM_FIELD.verification]: ok ? 'ALL GOOD' : 'PROBLEM',
        [EOM_FIELD.notes]: text,
        [EOM_FIELD.verifiedAt]: new Date().toISOString(),
      },
    },
  });
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
