/* eslint-disable no-console */

const AIRTABLE_API_BASE = "https://api.airtable.com/v0";
const TIMEZONE = "America/Toronto"; // EST/EDT automatically (DST-aware)

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
 * Parse Airtable date field into numeric components WITHOUT trusting timezone markers.
 * We intentionally ignore "Z" because user confirmed the value is actually local time.
 *
 * Accepts:
 *  - "2026-02-01T10:30:00.000Z"
 *  - "2026-02-01T10:30:00Z"
 *  - "2026-02-01T10:30"
 *  - "2026-02-01 10:30"
 */
function parseWallClockParts(input) {
  const s = String(input).trim();

  // Normalize: remove trailing .000Z / Z (we treat it as local wall-clock time)
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
 * Get the timezone offset (in minutes) for a given UTC Date in a specific IANA timezone.
 * Offset returned is: (local-as-UTC - actual-UTC) in minutes.
 *
 * Example: In winter Toronto is UTC-5.
 * For a UTC instant, local clock is 5 hours earlier -> offset = -300.
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

  // This is the "local wall time" rendered, interpreted as if it were UTC
  const localAsUtcMs = Date.UTC(y, mo - 1, d, h, mi, s);

  // Offset in minutes
  return (localAsUtcMs - dateUtc.getTime()) / 60000;
}

/**
 * Convert a Toronto wall-clock datetime to a true UTC ISO string.
 * DST is handled automatically because offset is computed for that date.
 *
 * Key formula:
 *   utcMs = wallClockAsUtcMs - offsetMinutes(wallClockInstantGuess)*60*1000
 *
 * Where wallClockAsUtcMs is Date.UTC(year,month,day,hour,minute,second)
 */
function torontoWallClockToUtcIso(input) {
  const { year, month, day, hour, minute, second } = parseWallClockParts(input);

  // Treat the wall clock numbers as if they were UTC (a "guess")
  const wallClockAsUtcMs = Date.UTC(year, month - 1, day, hour, minute, second);
  const guessDateUtc = new Date(wallClockAsUtcMs);

  if (Number.isNaN(guessDateUtc.getTime())) {
    throw new Error(`Invalid wall-clock date from Airtable: ${input}`);
  }

  // Compute correct offset for that moment in Toronto (DST-aware)
  const offsetMin = getTimeZoneOffsetMinutes(guessDateUtc, TIMEZONE);

  // Convert wall-clock to real UTC
  const utcMs = wallClockAsUtcMs - offsetMin * 60 * 1000;
  const utcDate = new Date(utcMs);

  return utcDate.toISOString().replace(".000Z", "Z");
}

async function airtableListRecords({ baseId, tableName, viewName, token, maxRecords }) {
  const records = [];
  let offset;

  while (records.length < maxRecords) {
    const url = new URL(`${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("view", viewName);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);

    const data = await httpJson(url.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (Array.isArray(data.records)) records.push(...data.records);

    offset = data.offset;
    if (!offset) break;

    await sleep(120);
  }

  return records.slice(0, maxRecords);
}

async function airtableUpdateRecord({ baseId, tableName, recordId, token, fields }) {
  const url = `${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}/${recordId}`;
  return httpJson(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}` },
    body: { fields },
  });
}

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

async function main() {
  const airtableToken = requireEnv("AIRTABLE_TOKEN");
  const mtekToken = requireEnv("MTEK_API_TOKEN");

  const baseId = requireEnv("AIRTABLE_BASE_ID");
  const tableName = requireEnv("AIRTABLE_TABLE_NAME");
  const viewName = requireEnv("AIRTABLE_VIEW_NAME");

  const fieldRoom = requireEnv("AIRTABLE_FIELD_ROOM");
  const fieldDate = requireEnv("AIRTABLE_FIELD_DATE_UTC");
  const fieldClassId = requireEnv("AIRTABLE_FIELD_CLASS_ID");

  const mtekBaseUrl = requireEnv("MTEK_BASE_URL");

  const maxRecords = Number(getEnv("MAX_RECORDS", "500"));
  const dispatchRecordId = (getEnv("DISPATCH_RECORD_ID") || "").trim();

  console.log("Starting Resolve Class ID job");
  console.log(`Airtable 'date' treated as wall-clock in ${TIMEZONE} (DST-aware)`);
  console.log(`Airtable base=${baseId} table="${tableName}" view="${viewName}"`);
  if (dispatchRecordId) console.log(`Dispatch scope: record_id=${dispatchRecordId}`);

  let records = await airtableListRecords({
    baseId,
    tableName,
    viewName,
    token: airtableToken,
    maxRecords,
  });

  if (dispatchRecordId) {
    records = records.filter((r) => r.id === dispatchRecordId);
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

    if (existingClassId) {
      skipped++;
      console.log(`SKIP ${r.id}: already has Class ID = ${existingClassId}`);
      continue;
    }

    try {
      const utcIso = torontoWallClockToUtcIso(dateLocal);

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

      await airtableUpdateRecord({
        baseId,
        tableName,
        recordId: r.id,
        token: airtableToken,
        fields: {
  [fieldClassId]: Number(classSessionId),
        },
      });

      updated++;
      console.log(
        `UPDATED ${r.id}: room="${room}" local="${dateLocal}" => utc="${utcIso}" location=${locationId} class_session_id=${classSessionId}`
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
