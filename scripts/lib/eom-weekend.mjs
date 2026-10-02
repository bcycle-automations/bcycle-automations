/**
 * EOM week-end rule, shared by the payroll syncs, the backfill and the month-end
 * verification so they can never disagree.
 *
 * A time punch / class always belongs to the EOM whose Start..End covers its own
 * date. On top of that, if its WEEK (which ends on a Saturday) finishes in a
 * different EOM, it is ALSO linked to that EOM and "In EOM via week-end rule" is
 * checked. Dates are plain YYYY-MM-DD strings (the studio's local date), so the
 * Saturday maths is done in UTC to stay out of time-zone trouble.
 */

/** The Saturday on or after a YYYY-MM-DD date. */
export function weekEndSaturday(date) {
  const [y, m, d] = String(date).slice(0, 10).split('-').map(Number);
  const utc = new Date(Date.UTC(y, m - 1, d));
  utc.setUTCDate(utc.getUTCDate() + ((6 - utc.getUTCDay() + 7) % 7));
  return utc.toISOString().slice(0, 10);
}

/** YYYY-MM-DD shifted by a number of days. */
export function addDays(date, days) {
  const [y, m, d] = String(date).slice(0, 10).split('-').map(Number);
  const utc = new Date(Date.UTC(y, m - 1, d));
  utc.setUTCDate(utc.getUTCDate() + days);
  return utc.toISOString().slice(0, 10);
}

/**
 * Which EOMs a date belongs to.
 *   own:       windows covering the date itself (should be exactly one)
 *   extra:     windows covering the week's Saturday that are not `own`
 *   ids:       own + extra (what the EOM link should hold)
 *   viaWeekEnd true when `extra` is not empty (what the checkbox should say)
 * `months` is [{ id, start, end }].
 */
export function eomMembership(date, months) {
  const day = String(date).slice(0, 10);
  const own = months.filter((m) => m.start <= day && day <= m.end);
  const saturday = weekEndSaturday(day);
  const ownIds = new Set(own.map((m) => m.id));
  const extra = months.filter((m) => m.start <= saturday && saturday <= m.end && !ownIds.has(m.id));
  return {
    own,
    extra,
    ids: [...own, ...extra].map((m) => m.id),
    viaWeekEnd: extra.length > 0,
    saturday,
  };
}
