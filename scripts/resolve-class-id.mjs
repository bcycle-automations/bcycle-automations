/* eslint-disable no-console */
/**
 * Resolve Class ID Automation (robust datetime handling)
 *
 * Key fixes:
 * - Airtable "fx" formula date strings are NOT reliably parsed by `new Date()` in Node.
 * - We parse common Airtable/UI formats deterministically.
 * - We query MTEK within a small ±WINDOW_MINUTES range and pick the closest match.
 */

const AIRTABLE_API_BASE = "https://api.airtable.com/v0";

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}
function getEnv(name, fallback = "") {
  return process.env[name] ?? fallback;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function httpJson(url, { method = "GET", headers = {}, body } = {}) {
  const res = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      ...headers,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    throw new Error(
      `HTTP ${res.status} ${res.statusText} for ${method} ${url}\nResponse: ${text}`
    );
  }
  return json;
}

/* ---------------- Datetime parsing ---------------- */

/**
 * Convert Airtable-provided date value into a UTC ISO string (ending with Z).
 *
 * Handles:
 * 1) ISO strings like: 2026-02-02T12:15:00.000Z (safe)
 * 2) ISO strings w/ offset: 2026-02-02T07:15:00-05:00 (safe)
 * 3) Formula/UI-like strings: "2/2/2026 7:15am EST" or "2/2/2026 7:15 AM"
 *
 * Notes:
 * - If no timezone is present in the string, we fall back to DEFAULT_TZ_OFFSET env (e.g. "-05:00").
 */
function airtableDatetimeToUtcIso(raw) {
  if (raw == null || raw === "") throw new Error(`Missing Airtable datetime value`);

  // If Airtable gives a number (rare), treat as epoch ms
  if (typeof raw === "number") {
    const d = new Date(raw);
    if (Number.isNaN(d.getTime())) throw new Error(`Invalid epoch datetime: ${raw}`);
    return d.toISOString().replace(".000Z", "Z");
  }

  const s = String(raw).trim();

  // Case 1: ISO with Z or offset → Date parsing is reliable
  // Examples: 2026-02-02T12:15:00.000Z, 2026-02-02T07:15:00-05:00
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) {
    const d = new Date(s);
    if (Number.isNaN(d.getTime())) throw new Error(`Invalid ISO datetime: ${s}`);
    return d.toISOString().replace(".000Z", "Z");
  }

  // Case 2: "M/D/YYYY h:mma [TZ]" or "M/D/YYYY h:mm AM [TZ]"
  // Examples: "2/2/2026 7:15am EST", "02/02/2026 7:15 AM", etc.
  // We parse this ourselves to avoid locale-dependent Date parsing.
  const m = s.match(
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)?\s*(EST|EDT|ET|UTC|GMT)?$/i
  );

  if (!m) {
    throw new Error(
      `Unrecognized Airtable datetime format: "${s}". ` +
        `Use a real Airtable Date/Time field OR ensure formula outputs ISO like 2026-02-02T12:15:00Z.`
    );
  }

  let [, MM, DD, YYYY, hh, mm, ampmRaw, tzAbbrevRaw] = m;

  const month = Number(MM);
  const day = Number(DD);
  const year = Number(YYYY);
  let hour = Number(hh);
  const minute = Number(mm);

  const ampm = ampmRaw ? ampmRaw.toLowerCase() : null;

  // Convert 12-hour to 24-hour if am/pm provided
  if (ampm) {
    if (ampm === "am") {
      if (hour === 12) hour = 0;
    } else if (ampm === "pm") {
      if (hour !== 12) hour += 12;
    }
  }

  // Determine timezone offset
  const tzAbbrev = tzAbbrevRaw ? tzAbbrevRaw.toUpperCase() : null;

  // Default if timezone not explicitly present
  const defaultOffset = String(getEnv("DEFAULT_TZ_OFFSET", "-05:00")).trim();

  // Map common abbreviations
  // EST = UTC-5, EDT = UTC-4, ET = defaultOffset (you decide via env)
  let offset = defaultOffset;

  if (tzAbbrev === "UTC" || tzAbbrev === "GMT") offset = "+00:00";
  if (tzAbbrev === "EST") offset = "-05:00";
  if (tzAbbrev === "EDT") offset = "-04:00";
  if (tzAbbrev === "ET") offset = defaultOffset;

  // Build a stable ISO with offset: YYYY-MM-DDTHH:mm:00±HH:MM
  const pad2 = (n) => String(n).padStart(2, "0");
  const isoWithOffset = `${year}-${pad2(month)}-${pad2(day)}T${pad2(hour)}:${pad2(
    minute
  )}:00${offset}`;

  const d = new Date(isoWithOffset);
  if (Number.isNaN(d.getTime())) throw new Error(`Invalid parsed datetime: ${isoWithOffset}`);

  return d.toISOString().replace(".000Z", "Z");
}

function addMinutesUtcIso(utcIso, minutes) {
  const d = new Date(utcIso);
  if (Number.isNaN(d.getTime())) throw new Error(`Invalid UTC ISO: ${utcIso}`);
  d.setUTCMinutes(d.getUTCMinutes() + minutes);
  return d.toISOString().replace(".000Z", "Z");
}

/* ---------------- Airtable ---------------- */

async function airtableGetRecord({ baseId, tableName, recordId, token }) {
  const url = new URL(`${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}/${recordId}`);
  // Asking for string cell format helps when the field is formula/display-oriented
  url.searchParams.set("cellFormat", "string");
  return httpJson(url.toString(), { headers: { Authorization: `Bearer ${token}` } });
}

async function airtableListRecordsAll({
  baseId,
  tableName,
  viewName,
  token,
  maxRecords,
}) {
  const records = [];
  let offset;

  while (true) {
    const url = new URL(`${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("view", viewName);
    url.searchParams.set("pageSize", "100");
    url.searchParams.set("cellFormat", "string"); // important for formula fields
    if (offset) url.searchParams.set("offset", offset);

    const data = await httpJson(url.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (Array.isArray(data.records)) records.push(...data.records);

    if (maxRecords && records.length >= maxRecords) {
      return records.slice(0, maxRecords);
    }

    offset = data.offset;
    if (!offset) break;

    await sleep(120);
  }

  return records;
}

async function airtableUpdateRecord({ baseId, tableName, recordId, token, fields }) {
  const url = `${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}/${recordId}`;
  return httpJson(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}` },
    body: { fields },
  });
}

/* ---------------- MTEK ---------------- */

async function mtekGetLocationIdByName({ mtekBaseUrl, token, roomName }) {
  const url = new URL(`${mtekBaseUrl}/api/locations`);
  url.searchParams.set("page_size", "1");
  url.searchParams.set("name", roomName);

  const data = await httpJson(url.toString(), {
    headers: { Authorization: `Bearer ${token}` },
  });

  const first = Array.isArray(data?.data) ? data.data[0] : null;
  return first?.id ?? null;
}

async function mtekListClassSessionsInWindow({
  mtekBaseUrl,
  token,
  locationId,
  minUtcIso,
  maxUtcIso,
  pageSize = 20,
}) {
  const url = new URL(`${mtekBaseUrl}/api/class_sessions`);
  url.searchParams.set("page_size", String(pageSize));
  url.searchParams.set("location", String(locationId));
  url.searchParams.set("min_datetime", minUtcIso);
  url.searchParams.set("max_datetime", maxUtcIso);

  const data = await httpJson(url.toString(), {
    headers: { Authorization: `Bearer ${token}` },
  });

  return { url: url.toString(), data };
}

/**
 * Pick the closest class session by datetime within the returned window.
 * Tries to find exact match first, otherwise returns closest.
 */
function pickClosestSessionId({ sessions, targetUtcIso }) {
  if (!Array.isArray(sessions) || sessions.length === 0) return null;

  const targetMs = new Date(targetUtcIso).getTime();
  if (!Number.isFinite(targetMs)) throw new Error(`Bad targetUtcIso: ${targetUtcIso}`);

  // MTEK data format can vary; common is item.attributes.start_datetime / datetime.
  // We'll look for a few likely keys.
  function getSessionUtcIso(item) {
    const a = item?.attributes ?? {};
    return (
      a.start_datetime ||
      a.startDatetime ||
      a.datetime ||
      a.starts_at ||
      a.startsAt ||
      null
    );
  }

  let exact = null;
  let best = null;
  let bestDelta = Infinity;

  for (const item of sessions) {
    const dt = getSessionUtcIso(item);
    if (!dt) continue;

    const ms = new Date(dt).getTime();
    if (!Number.isFinite(ms)) continue;

    const delta = Math.abs(ms - targetMs);

    if (delta === 0) {
      exact = item;
      break;
    }
    if (delta < bestDelta) {
      bestDelta = delta;
      best = item;
    }
  }

  const chosen = exact || best;
  return chosen?.id ?? null;
}

/* ---------------- Main ---------------- */

async function main() {
  // Secrets
  const airtableToken = requireEnv("AIRTABLE_TOKEN");
  const mtekToken = requireEnv("MTEK_API_TOKEN");

  // Config
  const baseId = requireEnv("AIRTABLE_BASE_ID");
  const tableName = requireEnv("AIRTABLE_TABLE_NAME");
  const viewName = requireEnv("AIRTABLE_VIEW_NAME");

  const fieldRoom = requireEnv("AIRTABLE_FIELD_ROOM");
  const fieldDate = requireEnv("AIRTABLE_FIELD_DATE_UTC"); // can be real datetime OR formula output
  const fieldClassId = requireEnv("AIRTABLE_FIELD_CLASS_ID");

  const mtekBaseUrl = requireEnv("MTEK_BASE_URL");

  const windowMinutes = Number(String(getEnv("WINDOW_MINUTES", "2")).trim());
  if (!Number.isFinite(windowMinutes) || windowMinutes < 0) {
    throw new Error(`Invalid WINDOW_MINUTES: ${getEnv("WINDOW_MINUTES")}`);
  }

  // Safety cap for FULL runs only (optional). Set to "" to disable.
  const maxRecordsRaw = String(getEnv("MAX_RECORDS", "")).trim();
  const maxRecords =
    maxRecordsRaw === ""
      ? null
      : Number.isFinite(Number(maxRecordsRaw))
      ? Number(maxRecordsRaw)
      : null;

  const dispatchRecordId = String(getEnv("DISPATCH_RECORD_ID", "")).trim();

  console.log("Starting Resolve Class ID job (robust datetime)");
  console.log(`Airtable base=${baseId} table="${tableName}" view="${viewName}"`);
  console.log(`Using Airtable date field name: "${fieldDate}"`);
  console.log(`WINDOW_MINUTES=${windowMinutes} (query ± window around target)`);
  console.log(`DEFAULT_TZ_OFFSET=${String(getEnv("DEFAULT_TZ_OFFSET", "-05:00")).trim()} (only used if Airtable string has no tz)`);

  let records = [];

  if (dispatchRecordId) {
    const one = await airtableGetRecord({
      baseId,
      tableName,
      recordId: dispatchRecordId,
      token: airtableToken,
    });
    records = [one];
  } else {
    records = await airtableListRecordsAll({
      baseId,
      tableName,
      viewName,
      token: airtableToken,
      maxRecords,
    });
  }

  console.log(`Records to process: ${records.length}`);

  let updated = 0;
  let skipped = 0;
  let notFound = 0;
  let errors = 0;

  for (const r of records) {
    const fields = r.fields || {};
    const room = fields[fieldRoom];
    const dateValue = fields[fieldDate];
    const existingClassId = fields[fieldClassId];

    if (!room || !dateValue) {
      skipped++;
      console.log(`SKIP ${r.id}: missing room/date (room="${room}" date="${dateValue}")`);
      continue;
    }

    if (existingClassId !== undefined && existingClassId !== null && String(existingClassId).trim() !== "") {
      skipped++;
      console.log(`SKIP ${r.id}: already has Class ID = ${existingClassId}`);
      continue;
    }

    try {
      const targetUtcIso = airtableDatetimeToUtcIso(dateValue);
      const minUtcIso = addMinutesUtcIso(targetUtcIso, -windowMinutes);
      const maxUtcIso = addMinutesUtcIso(targetUtcIso, +windowMinutes);

      console.log(
        `DEBUG ${r.id}: room="${room}" rawDate="${dateValue}" => targetUtc="${targetUtcIso}" window=[${minUtcIso} .. ${maxUtcIso}]`
      );

      const locationId = await mtekGetLocationIdByName({
        mtekBaseUrl,
        token: mtekToken,
        roomName: String(room),
      });

      if (!locationId) {
        notFound++;
        console.log(`NOT FOUND ${r.id}: location not found for room="${room}"`);
        continue;
      }

      const { url: mtekUrl, data } = await mtekListClassSessionsInWindow({
        mtekBaseUrl,
        token: mtekToken,
        locationId,
        minUtcIso,
        maxUtcIso,
        pageSize: 20,
      });

      console.log(`DEBUG ${r.id}: MTEK URL: ${mtekUrl}`);

      const sessions = Array.isArray(data?.data) ? data.data : [];
      const classSessionId = pickClosestSessionId({ sessions, targetUtcIso });

      if (!classSessionId) {
        notFound++;
        console.log(
          `NOT FOUND ${r.id}: no class_session in window (location=${locationId} targetUtc=${targetUtcIso})`
        );
        continue;
      }

      const classIdNum = Number(classSessionId);
      if (!Number.isFinite(classIdNum)) {
        throw new Error(`MTEK class_session_id is not numeric: ${classSessionId}`);
      }

      await airtableUpdateRecord({
        baseId,
        tableName,
        recordId: r.id,
        token: airtableToken,
        fields: {
          [fieldClassId]: classIdNum,
        },
      });

      updated++;
      console.log(
        `UPDATED ${r.id}: room="${room}" rawDate="${dateValue}" => targetUtc="${targetUtcIso}" location=${locationId} class_session_id=${classIdNum}`
      );

      await sleep(120);
    } catch (e) {
      errors++;
      console.error(`ERROR ${r.id}:`, e?.message || e);
      await sleep(250);
    }
  }

  console.log("Done");
  console.log(JSON.stringify({ processed: records.length, updated, skipped, notFound, errors }, null, 2));

  if (errors > 0) process.exitCode = 1;
}

main().catch((e) => {
  console.error("Fatal:", e?.message || e);
  process.exit(1);
});
