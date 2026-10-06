/**
 * The two places EOM links are kept (the week-end rule applies to punches only), plus tiny Airtable helpers used
 * by the backfill and the month-end verification.
 *
 *  punches: HR base, "Time Punches"        -> HR "EOM" (source table)
 *  classes: HR - Instructors, "Classes (for Payroll)" -> HR - Instructors "EOM"
 *           (a synced copy, so its record ids differ from the HR EOM's; the two
 *           are matched by Start Date)
 */

export const TARGETS = {
  punches: {
    label: 'Time punches',
    weekEndRule: true,
    baseId: process.env.HR_BASE_ID || 'appiwfeujJzUZPPBx',
    tableId: process.env.HR_PUNCHES_TABLE_ID || 'tblVxt2W7NanQmJFR',
    dateFieldId: 'fldwxo5JqKNOf4KvY',
    dateIsDateTime: false,
    eomLinkFieldId: 'fldRzRhdN0dv3OHnR',
    flagFieldId: 'fldh4VnWcNn1YXJQ3',
    eomTableId: process.env.HR_EOM_TABLE_ID || 'tbl3UMRShm59z41JL',
    eomStartFieldId: 'fldduMMEtqShh6CPB',
    eomEndFieldId: 'fld8fIYiRFAdcwN1l',
  },
  classes: {
    label: 'Classes',
    // The week-end rule does NOT apply to classes: one EOM, by the class's own date.
    weekEndRule: false,
    baseId: process.env.INSTRUCTORS_BASE_ID || 'appBC0Ja4B5LKbZLW',
    tableId: process.env.INSTRUCTORS_CLASSES_TABLE_ID || 'tbl8RbWysEFdNuz37',
    dateFieldId: 'fldBkXtX4DU9n5scl',
    dateIsDateTime: true,
    eomLinkFieldId: 'flds49hl8DluvKyGk',
    flagFieldId: 'fldrZyYGCYzN6hp2Z',
    eomTableId: process.env.INSTRUCTORS_EOM_TABLE_ID || 'tbliX73kdfgme7r3R',
    eomStartFieldId: 'fldwo53Yx2PI2VVVJ',
    eomEndFieldId: 'fldt7m3KygcrA7GZf',
  },
};

export const TIME_ZONE = process.env.PAYROLL_TIME_ZONE || 'America/Toronto';

export async function airtable({ token, baseId, method = 'GET', tableId, recordId = '', body, query = '' }) {
  const base = `https://api.airtable.com/v0/${baseId}/${tableId}${recordId ? `/${recordId}` : ''}`;
  const response = await fetch(query ? `${base}?${query}` : base, {
    method,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) throw new Error(`Airtable ${method} ${tableId} failed (${response.status}): ${await response.text()}`);
  return response.json();
}

export async function fetchAll({ token, baseId, tableId, fieldIds, filter = '' }) {
  const records = [];
  let offset = '';
  do {
    const params = new URLSearchParams({ returnFieldsByFieldId: 'true', pageSize: '100' });
    fieldIds.forEach((id) => params.append('fields[]', id));
    if (filter) params.set('filterByFormula', filter);
    if (offset) params.set('offset', offset);
    const page = await airtable({ token, baseId, tableId, query: params.toString() });
    records.push(...(page.records || []));
    offset = page.offset || '';
  } while (offset);
  return records;
}

export async function patchInBatches({ token, baseId, tableId, updates }) {
  for (let i = 0; i < updates.length; i += 10) {
    await airtable({ token, baseId, tableId, method: 'PATCH', body: { records: updates.slice(i, i + 10) } });
  }
}

/** The record's date as the studio sees it (YYYY-MM-DD), so a late class stays in its own day. */
export function localDay(value, isDateTime) {
  if (!value) return null;
  if (!isDateTime) return String(value).slice(0, 10);
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return new Intl.DateTimeFormat('en-CA', { timeZone: TIME_ZONE, year: 'numeric', month: '2-digit', day: '2-digit' }).format(date);
}

export async function loadMonths(token, target) {
  const records = await fetchAll({
    token,
    baseId: target.baseId,
    tableId: target.eomTableId,
    fieldIds: [target.eomStartFieldId, target.eomEndFieldId],
  });
  return records
    .map((r) => ({ id: r.id, start: r.fields?.[target.eomStartFieldId], end: r.fields?.[target.eomEndFieldId] }))
    .filter((m) => m.start && m.end)
    .sort((a, b) => a.start.localeCompare(b.start));
}
