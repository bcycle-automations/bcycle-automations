#!/usr/bin/env node

/**
 * HR Backfill punch instructors
 * For time punches that have NEITHER an Employee nor an Instructor, look the
 * "Employee Name" up in the Instructors table (same HR base) and link the
 * instructor. This is the same fallback the HR Payroll Time Punches sync now
 * does for new punches; this applies it to punches already imported.
 *
 * Only punches dated on/after FROM_DATE (default 2026-09-01) are considered.
 * An Active instructor wins when a name appears more than once. Runs read-only
 * unless DRY_RUN is "false".
 */

import { fetchAll, patchInBatches } from './lib/eom-targets.mjs';

const token = process.env.AIRTABLE_TOKEN;
const baseId = process.env.HR_BASE_ID || 'appiwfeujJzUZPPBx';
const punchesTableId = process.env.HR_PUNCHES_TABLE_ID || 'tblVxt2W7NanQmJFR';
const instructorsTableId = process.env.AIRTABLE_INSTRUCTORS_TABLE_ID || 'tblGfu4QRovWm7oX0';
const fromDate = process.env.FROM_DATE || '2026-09-01';
const dryRun = String(process.env.DRY_RUN ?? 'true').toLowerCase() !== 'false';

const F = {
  date: 'fldwxo5JqKNOf4KvY',
  name: 'fld19AyljflWBAFH0',
  employee: 'fldjQkvgi3IsQjh05',
  instructor: 'fld8IZMWZMnp5zY6B',
  instrName: 'fldfiP2nCrPWevw9T',
  instrStatus: 'fldHbQFBNAA4ENlgS',
};

const norm = (v) => String(v || '').replace(/\s+/g, ' ').trim().toLowerCase();

async function run() {
  if (!token) throw new Error('Missing required environment variable: AIRTABLE_TOKEN');

  const instructors = await fetchAll({ token, baseId, tableId: instructorsTableId, fieldIds: [F.instrName, F.instrStatus] });
  const map = new Map();
  const active = new Set();
  for (const rec of instructors) {
    const name = norm(rec.fields?.[F.instrName]);
    if (!name) continue;
    const isActive = rec.fields?.[F.instrStatus]?.name === 'Active' || rec.fields?.[F.instrStatus] === 'Active';
    if (!map.has(name) || (isActive && !active.has(name))) {
      map.set(name, rec.id);
      if (isActive) active.add(name);
    }
  }

  const punches = await fetchAll({
    token,
    baseId,
    tableId: punchesTableId,
    fieldIds: [F.date, F.name, F.employee, F.instructor],
    filter: `AND(IS_AFTER({${F.date}}, '${new Date(Date.parse(fromDate) - 86400000).toISOString().slice(0, 10)}'), NOT({${F.employee}}), NOT({${F.instructor}}))`,
  });

  const updates = [];
  const unmatched = new Set();
  for (const punch of punches) {
    const name = norm(punch.fields?.[F.name]);
    const id = map.get(name);
    if (id) updates.push({ id: punch.id, fields: { [F.instructor]: [id] } });
    else unmatched.add(name || '(no name)');
  }

  // Names are private; the public Actions log gets counts only.
  console.log(
    `Punches with no Employee and no Instructor since ${fromDate}: ${punches.length} | matched to an instructor: ${updates.length} | still unmatched: ${punches.length - updates.length} (${unmatched.size} distinct name(s))`,
  );
  if (dryRun) {
    console.log('DRY_RUN: nothing was written. Set DRY_RUN=false to apply.');
    return;
  }
  await patchInBatches({ token, baseId, tableId: punchesTableId, updates });
  console.log(`Linked an instructor on ${updates.length} punch(es).`);
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
