/* eslint-disable no-console */
/**
 * Resolve Class ID Automation
 * - Full run: pulls records from Airtable view (paginated) with optional MAX_RECORDS safety cap (500 OK)
 * - Single-record run: if DISPATCH_RECORD_ID is provided, fetches that record directly by ID
 * - Time handling: TRUST Airtable's API datetime as true UTC (do NOT convert again)
 * - Writes Airtable "Class ID" as NUMBER
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

/**
 * Airtable datetime → UTC ISO.
 * IMPORTANT:
 * - Airtable already returns correct UTC instants for Date/Time fields when you read via API.
 * - Do NOT strip Z / do NOT apply timezone math again (that causes +4/+5h double conversion bugs).
 */
function airtableDateToUtcIso(input) {
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`Invalid Airtable datetime: ${input}`);
  }
  return d.toISOString().replace(".000Z", "Z");
}

/* ---------------- Airtable ---------------- */

async function airtableGetRecord({ baseId, tableName, recordId, token }) {
  const url = `${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}/${recordId}`;
  return httpJson(url, { headers: { Authorization: `Bearer ${token}` } });
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
    const url = new URL(
      `${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}`
    );
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
    maxRecordsRaw === ""
      ? null
      : Number.isFinite(Number(maxRecordsRaw))
      ? Number(maxRecordsRaw)
      : null;

  const dispatchRecordId = String(getEnv("DISPATCH_RECORD_ID", "")).trim();

  console.log("Starting Resolve Class ID job");
  console.log("Datetime handling: TRUST Airtable API value as true UTC (no extra conversion).");
  console.log(`Airtable base=${baseId} table="${tableName}" view="${viewName}"`);
  if (dispatchRecordId) console.log(`Dispatch scope: record_id=${dispatchRecordId}`);
  if (!dispatchRecordId && maxRecords) console.log(`MAX_RECORDS safety cap (full runs): ${maxRecords}`);

  let records = [];

  if (dispatchRecordId) {
    // Single record run: fetch by ID directly
    const one = await airtableGetRecord({
      baseId,
      tableName,
      recordId: dispatchRecordId,
      token: airtableToken,
    });
    records = [one];
  } else {
    // Full run: read from view (paginated), optional safety cap
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
      // Airtable already provides a UTC instant for Date/Time fields via API
      const utcIso = airtableDateToUtcIso(dateValue);

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
        `UPDATED ${r.id}: room="${room}" airtable_date="${dateValue}" => utc="${utcIso}" location=${locationId} class_session_id=${classIdNum}`
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
