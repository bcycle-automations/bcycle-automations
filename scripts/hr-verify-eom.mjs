#!/usr/bin/env node

/**
 * HR Verify EOM
 * Month-end verification for ONE HR "EOM" record (clicked from the "Verify
 * month-end" link, bridged by Make). It re-derives, from first principles, what
 * should be in that EOM and compares with what is actually linked:
 *
 *   - every time punch / class whose own date is in the EOM is linked to it
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
import { TARGETS, airtable, fetchAll, loadMonths, localDay, patchInBatches } from './lib/eom-targets.mjs';

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
  const unfixable = [];
  const updates = [];
  let belong = 0;
  let viaRule = 0;

  for (const record of records) {
    const day = localDay(record.fields?.[target.dateFieldId], target.dateIsDateTime);
    const linked = (record.fields?.[target.eomLinkFieldId] || []).includes(thisMonth.id);
    const flag = Boolean(record.fields?.[target.flagFieldId]);
    if (!day) {
      if (linked) unfixable.push(`${record.id} (linked here but has no date)`);
      continue;
    }
    const membership = eomMembership(day, months, { weekEnd: target.weekEndRule });
    const shouldBeIn = membership.ids.includes(thisMonth.id);
    let broken = false;
    if (shouldBeIn) {
      belong += 1;
      if (membership.extra.some((m) => m.id === thisMonth.id)) viaRule += 1;
      if (!linked) {
        missing.push(`${day} ${record.id}`);
        broken = true;
      }
      if (flag !== membership.viaWeekEnd) {
        flagWrong.push(`${day} ${record.id}`);
        broken = true;
      }
    } else if (linked) {
      wronglyIn.push(`${day} ${record.id}`);
      broken = true;
    }
    if (broken) {
      if (membership.own.length > 1) {
        unfixable.push(`${day} ${record.id} (more than one EOM covers this date)`);
      } else {
        updates.push({
          id: record.id,
          fields: { [target.eomLinkFieldId]: membership.ids, [target.flagFieldId]: membership.viaWeekEnd },
        });
      }
    }
  }

  // Put right what the rule says is wrong, then it's no longer a problem.
  if (updates.length) await patchInBatches({ token, baseId: target.baseId, tableId: target.tableId, updates });

  notes.lines.push(`${target.label}: ${belong} belong to this EOM (${belong - viaRule} by date, ${viaRule} only by the week-end rule).`);
  if (updates.length) {
    notes.lines.push(
      `${target.label}: fixed ${updates.length} record(s) — ${missing.length} added to this EOM, ${wronglyIn.length} removed from it, ${flagWrong.length} week-end checkbox(es) corrected. e.g. ${updates.slice(0, 3).map((u) => u.id).join(', ')}`,
    );
  }
  if (unfixable.length) {
    notes.problems.push(
      `${target.label}: ${unfixable.length} record(s) couldn't be put right: ${unfixable.slice(0, SAMPLE).join('; ')}${unfixable.length > SAMPLE ? `; +${unfixable.length - SAMPLE} more` : ''}`,
    );
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

  const notes = { lines: [`EOM ${eom.start} -> ${eom.end}`], problems: [] };

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
    ...(badChecks.length ? ['', `Still open for the month-end team (doesn't change the result above): ${badChecks.join('; ')}`] : []),
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
