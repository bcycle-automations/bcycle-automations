/* eslint-disable no-console */
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

function toIsoUtc(input) {
  // Airtable date fields typically come back as ISO already
  // (e.g., "2026-02-01T15:00:00.000Z" or "2026-02-01T15:00:00Z")
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`Invalid UTC datetime in Airtable: ${input}`);
  }
  return d.toISOString().replace(".000Z", "Z");
}

async function airtableListRecords({ baseId, tableName, viewName, apiKey, maxRecords }) {
  const records = [];
  let offset = undefined;

  while (records.length < maxRecords) {
    const url = new URL(`${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}`);
    url.searchParams.set("view", viewName);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);

    const data = await httpJson(url.toString(), {
      headers: { Authorization: `Bearer ${apiKey}` },
    });

    if (Array.isArray(data.records)) records.push(...data.records);

    offset = data.offset;
    if (!offset) break;

    // tiny backoff (Airtable is usually fine, but keep it polite)
    await sleep(120);
  }

  return records.slice(0, maxRecords);
}

async function airtableUpdateRecord({ baseId, tableName, recordId, apiKey, fields }) {
  const url = `${AIRTABLE_API_BASE}/${baseId}/${encodeURIComponent(tableName)}/${recordId}`;
  return httpJson(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}` },
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

  // Mariana Tek responses are typically JSON:API-ish: { data: [ { id, ... } ] }
  const first = Array.isArray(data?.data) ? data.data[0] : null;
  if (!first?.id) return null;
  return first.id;
}

async function mtekFindClassSessionId({
  mtekBaseUrl,
  token,
  locationId,
  utcIso,
}) {
  const url = new URL(`${mtekBaseUrl}/api/class_sessions`);
  url.searchParams.set("page_size", "1");
  url.searchParams.set("location", String(locationId));
  url.searchParams.set("min_datetime", utcIso);
  url.searchParams.set("max_datetime", utcIso);
  url.searchParams.set("ordering", "min_datetime");

  const data = await httpJson(url.toString(), {
    headers: { Authorization: `Bearer ${token}` },
  });

  const first = Array.isArray(data?.data) ? data.data[0] : null;
  if (!first?.id) return null;
  return first.id;
}

async function main() {
  const airtableApiKey = requireEnv("AIRTABLE_API_KEY");
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
  console.log(`Airtable base=${baseId} table="${tableName}" view="${viewName}"`);
  if (dispatchRecordId) console.log(`Dispatch scope: record_id=${dispatchRecordId}`);

  let records = await airtableListRecords({
    baseId,
    tableName,
    viewName,
    apiKey: airtableApiKey,
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
    const dateUtc = fields[fieldDate];
    const existingClassId = fields[fieldClassId];

    if (!room || !dateUtc) {
      skipped++;
      console.log(`SKIP ${r.id}: missing room/date (room="${room}" date="${dateUtc}")`);
      continue;
    }

    if (existingClassId) {
      skipped++;
      console.log(`SKIP ${r.id}: already has Class ID = ${existingClassId}`);
      continue;
    }

    try {
      const utcIso = toIsoUtc(dateUtc);

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
          `NOT FOUND ${r.id}: class_session not found (location=${locationId} datetime=${utcIso})`
        );
        continue;
      }

      await airtableUpdateRecord({
        baseId,
        tableName,
        recordId: r.id,
        apiKey: airtableApiKey,
        fields: {
          [fieldClassId]: String(classSessionId),
        },
      });

      updated++;
      console.log(
        `UPDATED ${r.id}: room="${room}" utc="${utcIso}" location=${locationId} class_session_id=${classSessionId}`
      );

      // small backoff to avoid bursts
      await sleep(120);
    } catch (e) {
      errors++;
      console.error(`ERROR ${r.id}:`, e?.message || e);
      // keep going
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
