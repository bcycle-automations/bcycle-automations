/* eslint-disable no-console */

const AIRTABLE_API_BASE = "https://api.airtable.com/v0";
const TIMEZONE = "America/Toronto"; // interpret Airtable "date" as local studio time

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
 * Parse Airtable date input into components WITHOUT trusting timezone markers.
 * Accepts:
 *  - "2026-02-01T10:00:00.000Z"
 *  - "2026-02-01T10:00:00Z"
 *  - "2026-02-01T10:00"
 *  - "2026-02-01 10:00"
 */
function parseDateParts(input) {
  const s = String(input).trim().replace(/\.\d{1,3}Z$/, "Z"); // normalize
  const m =
    s.match(
      /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?(?:Z)?$/
    ) ||
    s.match(
      /^(\d{4})-(\d{2})-(\d{2})$/
    );

  if (!m) throw new Error(`Unrecognized date format from Airtable: ${input}`);

  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  const hour = m[4] != null ? Number(m[4]) : 0;
  const minute = m[5] != null ? Number(m[5]) : 0;
  const second = m[6] != null ? Number(m[6]) : 0;

  if (
    [year, month, day, hour, minute, second].some((n) => Number.isNaN(n))
  ) {
    throw new Error(`Invalid numeric datetime parts from Airtable: ${input}`);
  }

  return { year, month, day, hour, minute, second };
}

/**
 * Convert a "wall clock" datetime in America/Toronto to true UTC ISO string.
 * IMPORTANT: Airtable shows "UTC", but user confirmed it's actually local EST/EDT time.
 *
 * Approach (no deps):
 * - Build a UTC "guess" using the same numeric components.
 * - Ask Intl to render that instant in America/Toronto.
 * - Parse that rendered wall-clock as if it were UTC (runner is UTC), compute offset.
 * - Apply offset to the guess to get the real UTC instant for that wall-clock time.
 */
function torontoWallTimeToUtcIso(input) {
  const { year, month, day, hour, minute, second } = parseDateParts(input);

  const utcGuess = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  if (Number.isNaN(utcGuess.getTime())) {
    throw new Error(`Invalid date after UTC guess build: ${input}`);
  }

  // Render utcGuess in America/Toronto as a wall time string, then parse as UTC (runner)
  const tzWallAsString = utcGuess.toLocaleString("en-US", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  // tzWallAsString like "02/01/2026, 05:00:00" (Toronto wall time for utcGuess)
  const tzWallParsedAsUtc = new Date(tzWallAsString);
  if (Number.isNaN(tzWallParsedAsUtc.getTime())) {
    throw new Error(`Failed to parse timezone wall time string: ${tzWallAsString}`);
  }

  // Offset between the guess instant and the Toronto wall-clock moment representation
  const offsetMs = utcGuess.getTime() - tzWallParsedAsUtc.getTime();

  // Real UTC instant for the intended Toronto wall time
  const realUtc = new Date(utcGuess.getTime() + offsetMs);

  return realUtc.toISOString().replace(".000Z", "Z");
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
  console.log(`Timezone interpretation: ${TIMEZONE} (Airtable 'date' treated as local)`);
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
      const utcIso = torontoWallTimeToUtcIso(dateLocal);

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
          [fieldClassId]: String(classSessionId),
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
