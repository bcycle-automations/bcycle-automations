/* eslint-disable no-console */
/**
 * Resolve Class ID Automation
 * - Full run: pulls ALL records from Airtable view (with optional MAX_RECORDS safety cap)
 * - Single-record run: if DISPATCH_RECORD_ID is provided, fetches that record directly by ID (does NOT depend on view or MAX_RECORDS)
 * - Time handling: Airtable `date` is treated as America/Toronto wall-clock time (EST/EDT) and converted to TRUE UTC (DST-aware)
 * - Writes Airtable "Class ID" as NUMBER (not string)
 */

const AIRTABLE_API_BASE = "https://api.airtable.com/v0";
const TIMEZONE = "America/Toronto";

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

/**
 * Parse Airtable datetime string into wall-clock components.
 * IMPORTANT: we IGNORE the trailing "Z" if present because user confirmed the value is local time.
 */
function parseWallClockParts(input) {
  const s = String(input).trim();
  // Strip timezone marker + milliseconds, we treat as local wall time
  const cleaned = s.replace(/\.\d{1,3}Z$/, "").replace(/Z$/, "");

  const m = cleaned.match(
    /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (!m) throw new Error(`Unrecognized Airtable date format: ${input}`);

  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  const hour = m[4] != null ? Number(m[4]) : 0;
  const minute = m[5] != null ? Number(m[5]) : 0;
  const second = m[6] != null ? Number(m[6]) : 0;

  if ([year, month, day, hour, minute, second].some(Number.isNaN)) {
    throw new Error(`Invalid Airtable date parts: ${input}`);
  }
  return { year, month, day, hour, minute, second };
}

/**
 * Compute timezone offset (minutes) for a given UTC instant in a given IANA timezone.
 * Returns offset such that:
 *   localTime = utcTime + offset
 * Example: Toronto winter (EST) => offset = -300 minutes
 */
function getTimeZoneOffsetMinutes(dateUtc, timeZone) {
  const dtf = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const parts = dtf.formatToParts(dateUtc);
  const map = Object.fromEntries(parts.map((p) => [p.type, p.value]));

  const y = Number(map.year);
  const mo = Number(map.month);
  const d = Number(map.day);
  const h = Number(map.hour);
  const mi = Number(map.minute);
  const s = Number(map.second);

  // Interpret the rendered LOCAL wall time as if it were UTC:
  const localAsUtcMs = Date.UTC(y, mo - 1, d, h, mi, s);

  // offset = local - utc
  return (localAsUtcMs - dateUtc.getTime()) / 60000;
}

/**
 * Convert an America/Toronto wall-clock datetime to TRUE UTC ISO string (DST-aware).
 *
 * If Airtable wall clock is 10:30:
 * - In winter (EST, UTC-5) => 15:30Z
 * - In summer (EDT, UTC-4) => 14:30Z
 */
function torontoWallClockToUtcIso(input) {
  const { year, month, day, hour, minute, second } = parseWallClockParts(input);

  // Treat wall-clock components as if they were UTC (a guess instant)
  const wallClockAsUtcMs = Date.UTC(year, month - 1, day, hour, minute, second);
  const guessUtc = new Date(wallClockAsUtcMs);

  if (Number.isNaN(guessUtc.getTime())) {
    throw new Error(`Invalid date from Airtable: ${input}`);
  }

  // Compute Toronto offset at that instant (DST-aware)
  const offsetMin = getTimeZoneOffsetMinutes(guessUtc, TIMEZONE);

  // Convert wall-clock to real UTC:
  // wall = utc + offset  =>  utc = wall - offset
  const realUtcMs = wallClockAsUtcMs - offsetMin * 60 * 1000;
  const realUtc = new Date(realUtcMs);

  return realUtc.toISOString().replace(".000Z", "Z");
}

/* ---------------- Airtable ---------------- */

async function airtableGetRecord({ baseId, tableName, recordId, token }) {
  const url = `${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}/${recordId}`;
  return httpJson(url, { headers: { Authorization: `Bearer ${token}` } });
}

async function airtableListRecordsAll({ baseId, tableName, viewName, token, maxRecords }) {
  const records = [];
  let offset;

  while (true) {
    const url = new URL(`${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("view", viewName);
    url.searchParams.set("pageSize", "100");
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

async function mtekFindClassSessionId({ mtekBaseUrl, token, locationId, utcIso }) {
  const url = new URL(`${mtekBaseUrl}/api/class_sessions`);
  url.searchParams.set("page_size", "1");
  url.searchParams.set("location", String(locationId));
  url.searchParams.set("min_datetime", utcIso);
  url.searchParams.set("max_datetime", utcIso);

  const data = await httpJson(url.toString(), {
    headers: { Authorization: `Bearer ${token}` },
  });

  const first = Array.isArray(data?.data) ? data.data[0] : null;
  return first?.id ?? null;
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
  const fieldDate = requireEnv("AIRTABLE_FIELD_DATE_UTC");
  const fieldClassId = requireEnv("AIRTABLE_FIELD_CLASS_ID");

  const mtekBaseUrl = requireEnv("MTEK_BASE_URL");

  // Safety cap for FULL runs only (optional). Set to "" to disable.
  const maxRecordsRaw = String(getEnv("MAX_RECORDS", "")).trim();
  const maxRecords =
    maxRecordsRaw === "" ? null : Number.isFinite(Number(maxRecordsRaw)) ? Number(maxRecordsRaw) : null;

  const dispatchRecordId = String(getEnv("DISPATCH_RECORD_ID", "")).trim();

  console.log("Starting Resolve Class ID job");
  console.log(`Airtable 'date' treated as wall-clock in ${TIMEZONE} (DST-aware)`);
  console.log(`Airtable base=${baseId} table="${tableName}" view="${viewName}"`);
  if (dispatchRecordId) console.log(`Dispatch scope: record_id=${dispatchRecordId}`);
  if (!dispatchRecordId && maxRecords) console.log(`MAX_RECORDS safety cap (full runs): ${maxRecords}`);

  let records = [];

  if (dispatchRecordId) {
    // IMPORTANT: for single-record runs, fetch by ID directly (do not rely on view/pagination/MAX_RECORDS)
    const one = await airtableGetRecord({
      baseId,
      tableName,
      recordId: dispatchRecordId,
      token: airtableToken,
    });
    records = [one];
  } else {
    // Full run: pull entire view (paginated), optional safety cap
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
    const dateLocal = fields[fieldDate];
    const existingClassId = fields[fieldClassId];

    if (!room || !dateLocal) {
      skipped++;
      console.log(`SKIP ${r.id}: missing room/date (room="${room}" date="${dateLocal}")`);
      continue;
    }

    if (existingClassId !== undefined && existingClassId !== null && String(existingClassId).trim() !== "") {
      skipped++;
      console.log(`SKIP ${r.id}: already has Class ID = ${existingClassId}`);
      continue;
    }

    try {
      // Convert Airtable local wall time -> true UTC
      const utcIso = torontoWallClockToUtcIso(dateLocal);

      // Resolve location
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

      // Resolve class session
      const classSessionId = await mtekFindClassSessionId({
        mtekBaseUrl,
        token: mtekToken,
        locationId,
        utcIso,
      });

      if (!classSessionId) {
        notFound++;
        console.log(
          `NOT FOUND ${r.id}: class_session not found (location=${locationId} utc=${utcIso})`
        );
        continue;
      }

      // Airtable "Class ID" is a NUMBER field
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
        `UPDATED ${r.id}: room="${room}" local="${dateLocal}" => utc="${utcIso}" location=${locationId} class_session_id=${classIdNum}`
      );

      await sleep(120);
    } catch (e) {
      errors++;
      console.error(`ERROR ${r.id}:`, e?.message || e);
      await sleep(250);
    }
  }

  console.log("Done");
  console.log(
    JSON.stringify(
      { processed: records.length, updated, skipped, notFound, errors },
      null,
      2
    )
  );

  if (errors > 0) process.exitCode = 1;
}

main().catch((e) => {
  console.error("Fatal:", e?.message || e);
  process.exit(1);
});
