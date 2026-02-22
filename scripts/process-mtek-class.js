// scripts/process-mtek-class.js
/* eslint-disable no-console */
import fs from "node:fs";
import process from "node:process";

// ------------------------------------------------------------
// CONFIG
// ------------------------------------------------------------

// Airtable
const AIRTABLE_BASE_ID = "appofCRTxHoIe6dXI";
const AIRTABLE_BASE_URL = "https://api.airtable.com/v0";

// MTEK
const MTEK_BASE = "https://bcycle.marianatek.com/api";

// Tables
const CLASSES_TABLE_NAME = "CTT SYNC DO NOT TOUCH";
const CLASS_RESERVATIONS_TABLE = "Class Reservations";
const CUSTOMERS_TABLE = "Customers";

// Airtable fields (match EXACT names in your base)
const CLASS_FIELDS = {
  LAST_UPDATE_TIME: "Last update time",
};

const RES_FIELDS = {
  RESERVATION_ID: "MTEK Reservation ID", // text
  STATUS: "Status", // text or single select
  SPOT_NAME: "Spot number", // text
  CLASSES_LINK: "Classes", // linked record (editable)
  CUSTOMERS_LINK: "Customer", // linked record (editable)
  IS_NEW: "NEW?", // checkbox/single select

  // ✅ NEW: This is the field in "Class Reservations" that stores the CLASS Airtable Record ID (recXXXX)
  // Must match the field name EXACTLY in Airtable
  CLASS_RECORD_ID: "Record ID",
};

// Make webhook
const SECOND_CLASS_WEBHOOK_URL =
  "https://hook.us2.make.com/njbqpqqh6i6lxr34ycro62pzh6ip5h33";

// Tokens
const { AIRTABLE_TOKEN, MTEK_API_TOKEN } = process.env;
if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!MTEK_API_TOKEN) throw new Error("Missing env: MTEK_API_TOKEN");

// Pagination / limits
const MTEK_PAGE_SIZE = 200;
const AIRTABLE_PAGE_SIZE = 100;
const AIRTABLE_BATCH_SIZE = 10;

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------
function sanitizeId(id) {
  if (!id) return id;
  return String(id).replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * Airtable filterByFormula string escaping:
 * {Field}="value"
 */
function airtableFormulaString(value) {
  return `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function getPayloadFromEvent() {
  const eventPath = process.env.GITHUB_EVENT_PATH;
  if (!eventPath) throw new Error("GITHUB_EVENT_PATH not set");

  const raw = fs.readFileSync(eventPath, "utf8");
  const event = JSON.parse(raw);

  console.log("Repository dispatch payload:", JSON.stringify(event, null, 2));

  const rawClassRecordId =
    event.client_payload?.airtable_record_id ||
    event.client_payload?.recordId ||
    null;
  const rawMtekClassId = event.client_payload?.mtek_class_id ?? null;

  if (!rawClassRecordId) throw new Error("No airtable_record_id in client_payload");
  if (rawMtekClassId == null) throw new Error("No mtek_class_id in client_payload");

  const classRecordId = sanitizeId(rawClassRecordId);
  const mtekClassId = String(rawMtekClassId).trim();

  console.log("> Raw class record id:", JSON.stringify(rawClassRecordId));
  console.log("> Cleaned class record id:", JSON.stringify(classRecordId));
  console.log("> MTEK class id from payload:", JSON.stringify(mtekClassId));

  // Basic sanity check (won't block, but helps logs)
  if (!String(classRecordId).startsWith("rec")) {
    console.log(
      `WARNING: classRecordId does not start with "rec". Value=${JSON.stringify(
        classRecordId
      )}`
    );
  }

  return { classRecordId, mtekClassId };
}

// ------------------------------------------------------------
// HTTP helpers
// ------------------------------------------------------------
async function airtableRequest(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      ...(options.headers || {}),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Airtable ${options.method || "GET"} ${url} failed: ${res.status} ${text}`
    );
  }

  return res.json();
}

async function airtableRequestTable(tableName, options = {}) {
  const url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
  return airtableRequest(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
}

async function airtablePatchClasses(body) {
  const url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    CLASSES_TABLE_NAME
  )}`;
  return airtableRequest(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

async function airtableDeleteRecords(tableName, recordIds) {
  if (!recordIds.length) return;

  const base = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;

  for (const batch of chunk(recordIds, AIRTABLE_BATCH_SIZE)) {
    const qs = batch.map((id) => `records[]=${encodeURIComponent(id)}`).join("&");
    const url = `${base}?${qs}`;

    const json = await airtableRequest(url, { method: "DELETE" });
    console.log(`> Deleted ${json.records?.length || 0} record(s) from ${tableName}`);
  }
}

async function mtekRequest(pathOrUrl, options = {}) {
  const url = pathOrUrl.startsWith("http") ? pathOrUrl : `${MTEK_BASE}${pathOrUrl}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      "Content-Type": "application/vnd.api+json",
      ...(options.headers || {}),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `MTEK ${options.method || "GET"} ${url} failed: ${res.status} ${text}`
    );
  }
  return res.json();
}

// ------------------------------------------------------------
// Airtable upserts / creates
// ------------------------------------------------------------
async function upsertCustomer({ email, name }) {
  if (!email) throw new Error("Cannot upsert customer with empty email");

  const lowerEmail = email.toLowerCase();

  const body = {
    performUpsert: {
      fieldsToMergeOn: ["Email (lower)", "Dupe?"],
    },
    records: [
      {
        fields: {
          "Email (lower)": lowerEmail,
          Email: lowerEmail,
          Name: name || "",
          "Dupe?": "No",
        },
      },
    ],
  };

  const json = await airtableRequestTable(CUSTOMERS_TABLE, {
    method: "PATCH",
    body: JSON.stringify(body),
  });

  const record = json.records?.[0];
  if (!record) throw new Error("No customer record returned from Airtable upsert");

  console.log(`> Upserted customer ${lowerEmail} -> ${record.id}`);
  return record;
}

async function createClassReservationsBatch(fieldsArray) {
  if (!fieldsArray.length) return;

  const body = {
    records: fieldsArray.map((fields) => ({ fields })),
  };

  const json = await airtableRequestTable(CLASS_RESERVATIONS_TABLE, {
    method: "POST",
    body: JSON.stringify(body),
  });

  console.log(`> Created ${json.records?.length || 0} reservation record(s)`);
  return json;
}

async function updateClassLastUpdate(classRecordId) {
  const body = {
    records: [
      {
        id: classRecordId,
        fields: {
          [CLASS_FIELDS.LAST_UPDATE_TIME]: new Date().toISOString(),
        },
      },
    ],
  };

  await airtablePatchClasses(body);
  console.log(`> Updated class last update time for ${classRecordId}`);
}

// ------------------------------------------------------------
// Find + delete existing Airtable reservations for this class
// ✅ NEW APPROACH:
// We use filterByFormula on the "Class Reservations" table where {Record ID} equals the class record id.
// This avoids issues where the linked "Classes" field points to a different table.
// ------------------------------------------------------------
async function listExistingReservationRecordIdsForClass(classRecordId) {
  const recordIds = [];
  let offset = null;

  do {
    const params = new URLSearchParams();
    params.set("pageSize", String(AIRTABLE_PAGE_SIZE));

    // Only fetch records where the stored class record id matches
    params.set(
      "filterByFormula",
      `{${RES_FIELDS.CLASS_RECORD_ID}}=${airtableFormulaString(classRecordId)}`
    );

    // You can keep this, but it's optional since we only need rec.id for deletes
    params.append("fields[]", RES_FIELDS.CLASS_RECORD_ID);

    if (offset) params.set("offset", offset);

    const url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${encodeURIComponent(
      CLASS_RESERVATIONS_TABLE
    )}?${params.toString()}`;

    const json = await airtableRequest(url);

    for (const rec of json.records || []) {
      recordIds.push(rec.id);
    }

    offset = json.offset || null;
  } while (offset);

  return recordIds;
}

async function deleteExistingReservationsForClass(classRecordId) {
  const ids = await listExistingReservationRecordIdsForClass(classRecordId);

  console.log(
    `> Found ${ids.length} existing Airtable reservation(s) to delete for class ${classRecordId} using {${RES_FIELDS.CLASS_RECORD_ID}}`
  );

  await airtableDeleteRecords(CLASS_RESERVATIONS_TABLE, ids);
}

// ------------------------------------------------------------
// MTEK helpers
// ------------------------------------------------------------

// Pull ALL reservations using:
// /reservations?class_session=[ID]&page_size=200
async function getAllReservationsForClassSession(classSessionId) {
  const all = [];
  let page = 1;

  let nextUrl = `${MTEK_BASE}/reservations?class_session=${encodeURIComponent(
    classSessionId
  )}&page_size=${MTEK_PAGE_SIZE}&page=${page}`;

  while (nextUrl) {
    const json = await mtekRequest(nextUrl);

    const data = Array.isArray(json?.data) ? json.data : [];
    all.push(...data);

    console.log(`> Pulled ${data.length} reservation(s) (total ${all.length})`);

    const next = json?.links?.next || null;
    if (next) {
      nextUrl = next.startsWith("http") ? next : `${MTEK_BASE}${next}`;
      continue;
    }

    if (data.length < MTEK_PAGE_SIZE) {
      nextUrl = null;
    } else {
      page += 1;
      nextUrl = `${MTEK_BASE}/reservations?class_session=${encodeURIComponent(
        classSessionId
      )}&page_size=${MTEK_PAGE_SIZE}&page=${page}`;
    }
  }

  // Dedupe by reservation id
  const seen = new Set();
  const deduped = [];
  for (const r of all) {
    const id = r?.id != null ? String(r.id) : null;
    if (!id) continue;
    if (seen.has(id)) continue;
    seen.add(id);
    deduped.push(r);
  }

  return deduped;
}

async function getUser(userId) {
  return mtekRequest(`/users/${userId}`);
}

async function getSpot(spotId) {
  return mtekRequest(`/spots/${spotId}`);
}

function hasNewTag463(reservationRecord) {
  const tags = reservationRecord?.relationships?.tags?.data || [];
  return tags.some((t) => String(t.id) === "463");
}

// ------------------------------------------------------------
// Make webhook
// ------------------------------------------------------------
async function sendMakeWebhook(payloadArray) {
  const res = await fetch(SECOND_CLASS_WEBHOOK_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payloadArray),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Make webhook failed: ${res.status} ${text}`);
  }

  console.log(`> Sent ${payloadArray.length} item(s) to Make webhook`);
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------
async function main() {
  const { classRecordId, mtekClassId } = getPayloadFromEvent();

  console.log(`> Using MTEK Class Session ID from payload: ${mtekClassId}`);

  // 1) Pull ALL reservations for this class session (includes cancelled + waitlist)
  const reservations = await getAllReservationsForClassSession(mtekClassId);
  console.log(`> Found ${reservations.length} reservation(s) for session ${mtekClassId}`);

  // 2) Delete ALL existing Airtable reservation records for this class (by {Record ID})
  await deleteExistingReservationsForClass(classRecordId);

  const makePayload = [];
  const toCreate = [];

  // 3) Build new reservation records + customer upserts
  for (const r of reservations) {
    const reservationId = r?.id != null ? String(r.id) : null;
    const status = r?.attributes?.status ?? "";

    console.log(`>> Reservation ${reservationId} (status=${status})`);

    const userRel = r?.relationships?.user?.data;
    const spotRel = r?.relationships?.spot?.data;

    const userId = userRel?.id ? String(userRel.id) : null;
    const spotId = spotRel?.id ? String(spotRel.id) : null;

    const [userJson, spotJson] = await Promise.all([
      userId ? getUser(userId) : null,
      spotId ? getSpot(spotId) : null,
    ]);

    const userAttrs = userJson?.data?.attributes || {};
    const spotAttrs = spotJson?.data?.attributes || {};

    const email = userAttrs.email || userAttrs.email_address || null;
    const fullName = userAttrs.full_name || userAttrs.name || null;
    const spotName = spotAttrs.name || null;

    const isNew = hasNewTag463(r);

    // Upsert customer ONLY if email exists
    let customerRecordId = null;
    let customerFields = {};

    if (email) {
      const customerRecord = await upsertCustomer({ email, name: fullName });
      customerRecordId = customerRecord.id;
      customerFields = customerRecord.fields || {};
    } else {
      console.log("   - No email found for user; skipping customer upsert/link");
    }

    // Build Airtable fields for CREATE
    const fields = {
      [RES_FIELDS.RESERVATION_ID]: reservationId,
      [RES_FIELDS.STATUS]: status || "",
      [RES_FIELDS.SPOT_NAME]: spotName || "",
      [RES_FIELDS.CLASSES_LINK]: [classRecordId],
      [RES_FIELDS.IS_NEW]: !!isNew,

      // ✅ NEW: store class record id directly in reservation record for reliable deletes
      [RES_FIELDS.CLASS_RECORD_ID]: classRecordId,
    };

    if (customerRecordId) {
      fields[RES_FIELDS.CUSTOMERS_LINK] = [customerRecordId];
    }

    toCreate.push(fields);

    // Make payload only when we have a customer record (unchanged behavior)
    if (customerRecordId) {
      makePayload.push({
        classRecordId,
        mtekClassId,
        reservationId,
        reservationStatus: status,
        customerRecordId,
        measurementNoteId: customerFields["Measurement Note ID"] || null,
        updatedBoardNameSpivi: customerFields["Updated board name in Spivi"] || null,
        oldZfBoardName: customerFields["OLD ZF BOARD NAME"] || null,
      });
    }
  }

  // 4) Create in Airtable in batches of 10
  for (const batch of chunk(toCreate, AIRTABLE_BATCH_SIZE)) {
    await createClassReservationsBatch(batch);
  }

  // 5) Update class record last update time
  await updateClassLastUpdate(classRecordId);

  // 6) Send to Make
  await sendMakeWebhook(makePayload);

  console.log("> Done processing class");
}

main().catch((err) => {
  console.error("ERROR in process-mtek-class.js:", err);
  process.exit(1);
});
