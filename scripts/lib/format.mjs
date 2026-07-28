// scripts/lib/format.mjs
// Formatting helpers to match the string conventions already used in the
// *MTEK BigQuery tables (built from CSV exports, not ISO-formatted).

// "2026-07-21T15:05:02.498561+00:00" or "2026-07-25" -> "07/21/2026"
// Zero-padded so it's compatible with BigQuery's PARSE_DATE('%m/%d/%Y', ...),
// which existing messy historical rows aren't always consistent with.
export function toMDYYYY(isoDateOrDateTime) {
  if (!isoDateOrDateTime) return "";
  const datePart = String(isoDateOrDateTime).slice(0, 10); // YYYY-MM-DD
  const [year, month, day] = datePart.split("-");
  if (!year || !month || !day) return "";
  return `${month}/${day}/${year}`;
}

// "09:30:00" -> "9:30 AM" (matches existing non-zero-padded-hour convention)
export function toTime12h(hhmmss) {
  if (!hhmmss) return "";
  const [hStr, mStr] = String(hhmmss).split(":");
  let h = parseInt(hStr, 10);
  const m = (mStr || "00").padStart(2, "0");
  const ampm = h >= 12 ? "PM" : "AM";
  h = h % 12;
  if (h === 0) h = 12;
  return `${h}:${m} ${ampm}`;
}

// true -> "true", false -> "false", null -> ""
export function boolToString(v) {
  if (v === null || v === undefined) return "";
  return String(v).toLowerCase();
}

// null/undefined -> "" (matches existing empty-string convention over NULL)
export function nullToEmpty(v) {
  return v === null || v === undefined ? "" : String(v);
}
