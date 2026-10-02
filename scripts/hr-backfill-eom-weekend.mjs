#!/usr/bin/env node

/**
 * HR Backfill EOM week-end
 * Applies the EOM week-end rule to time punches (HR base) and classes
 * (HR - Instructors): every record keeps the EOM its own date falls in, and
 * ALSO joins the EOM its week (ending Saturday) finishes in, with
 * "In EOM via week-end rule" ticked. Re-running is safe — it only touches
 * records whose EOM links or checkbox differ from the rule.
 *
 * Records dated before the first EOM are left alone. Runs read-only unless
 * DRY_RUN is "false". TARGET is punches | classes | both (default both).
 */

import { eomMembership, addDays } from './lib/eom-weekend.mjs';
import { TARGETS, fetchAll, loadMonths, localDay, patchInBatches } from './lib/eom-targets.mjs';

const token = process.env.AIRTABLE_TOKEN;
const dryRun = String(process.env.DRY_RUN ?? 'true').toLowerCase() !== 'false';
const which = String(process.env.TARGET || 'both').toLowerCase();

async function backfill(target) {
  const months = await loadMonths(token, target);
  if (!months.length) throw new Error(`${target.label}: the EOM table is empty — nothing to assign.`);
  const earliestStart = months[0].start;

  const records = await fetchAll({
    token,
    baseId: target.baseId,
    tableId: target.tableId,
    fieldIds: [target.dateFieldId, target.eomLinkFieldId, target.flagFieldId],
    filter: `IS_AFTER({${target.dateFieldId}}, '${addDays(earliestStart, -2)}')`,
  });

  const updates = [];
  const problems = [];
  let before = 0;
  let noDate = 0;
  let flagged = 0;

  for (const record of records) {
    const day = localDay(record.fields?.[target.dateFieldId], target.dateIsDateTime);
    if (!day) {
      noDate += 1;
      continue;
    }
    if (day < earliestStart) {
      before += 1;
      continue;
    }
    const membership = eomMembership(day, months);
    if (membership.own.length !== 1) {
      problems.push(`${day}: ${membership.own.length ? 'more than one EOM covers it' : 'no EOM covers it'}`);
      continue;
    }
    if (membership.viaWeekEnd) flagged += 1;
    const current = new Set(record.fields?.[target.eomLinkFieldId] || []);
    const wanted = new Set(membership.ids);
    const sameLinks = current.size === wanted.size && [...wanted].every((id) => current.has(id));
    const sameFlag = Boolean(record.fields?.[target.flagFieldId]) === membership.viaWeekEnd;
    if (!sameLinks || !sameFlag) {
      updates.push({
        id: record.id,
        fields: { [target.eomLinkFieldId]: membership.ids, [target.flagFieldId]: membership.viaWeekEnd },
      });
    }
  }

  console.log(
    `${target.label}: checked ${records.length} | week-end rule applies to ${flagged} | to update: ${updates.length} | before first EOM (${earliestStart}): ${before} | no date: ${noDate} | problems: ${problems.length}`,
  );
  if (!dryRun && updates.length) {
    await patchInBatches({ token, baseId: target.baseId, tableId: target.tableId, updates });
    console.log(`${target.label}: updated ${updates.length} record(s).`);
  }
  return problems.map((p) => `${target.label} ${p}`);
}

async function run() {
  if (!token) throw new Error('Missing required environment variable: AIRTABLE_TOKEN');
  const names = which === 'both' ? ['punches', 'classes'] : [which];
  for (const name of names) if (!TARGETS[name]) throw new Error(`TARGET must be punches, classes or both (got "${which}").`);

  const problems = [];
  for (const name of names) problems.push(...(await backfill(TARGETS[name])));

  if (dryRun) console.log('DRY_RUN: nothing was written. Set DRY_RUN=false to apply.');
  if (problems.length) {
    const unique = [...new Set(problems)].sort();
    throw new Error(`Some records could not be placed in an EOM:\n- ${unique.slice(0, 25).join('\n- ')}`);
  }
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
