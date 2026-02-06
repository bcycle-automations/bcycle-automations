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
  RESERVATION_ID: "MTEK Reservation ID", // text, used for performUpsert
  STATUS: "Status", // text or single select
  SPOT_NAME: "Spot number", // text
  CLASSES_LINK: "Classes", // linked record (editable)
  CUSTOMERS_LINK: "Customer", // linked record (editable)
  IS_NEW: "NEW?", // checkbox/single select
};

// Make webhook
const SECOND_CLASS_WEBHOOK_URL =
  "https://hook.us2.make.com/njbqpqqh6i6lxr34ycro62pzh6ip5h33";

// Tokens
const { AIRTABLE_TOKEN, MTEK_API_TOKEN } = process.env;
if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!MTEK_API_TOKEN) throw new Error("Missing env: MTEK_API_TOKEN");

// MTEK pagination
const MTEK_PAGE_SIZE = 200;

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------
function sanitizeId(id) {
  if (!id) return id;
  return String(id).replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
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

  return { classRecordId, mtekClassId };
}

// ------------------------------------------------------------
// HTTP helpers
// ------------------------------------------------------------
async function airtableRequestTable(tableName, options = {}) {
  const url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
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

async function airtablePatchClasses(body) {
  const url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    CLASSES_TABLE_NAME
  )}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Airtable PATCH ${url} failed: ${res.status} ${text}`);
  }
  return res.json();
}

async function mtekRequest(path, options = {}) {
  const url = path.startsWith("http") ? path : `${MTEK_BASE}${path}`;
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
// Airtable upserts
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

async function upsertClassReservation({
  reservationId,
  status,
  spotName,
  classRecordId,
  customerRecordId, // nullable
  isNew,
}) {
  if (!reservationId) throw new Error("Missing reservationId for upsertClassReservation");

  const fields = {
    [RES_FIELDS.RESERVATION_ID]: String(reservationId),
    [RES_FIELDS.STATUS]: status || "",
    [RES_FIELDS.SPOT_NAME]: spotName || "",
    [RES_FIELDS.CLASSES_LINK]: [classRecordId],
    [RES_FIELDS.IS_NEW]: !!isNew,
  };

  // Only link customer if we successfully upserted them (email existed)
  if (customerRecordId) {
    fields[RES_FIELDS.CUSTOMERS_LINK] = [customerRecordId];
  }

  const body = {
    performUpsert: {
      fieldsToMergeOn: [RES_FIELDS.RESERVATION_ID],
    },
    records: [{ fields }],
  };

  const json = await airtableRequestTable(CLASS_RESERVATIONS_TABLE, {
    method: "PATCH",
    body: JSON.stringify(body),
  });

  const record = json.records?.[0];
  if (!record) throw new Error("No class reservation record returned from Airtable upsert");

  console.log(
    `> Upserted reservation ${reservationId} -> ${record.id} (status=${status})`
  );
  return record;
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
// MTEK helpers
// ------------------------------------------------------------

// Pull ALL reservations using your endpoint:
// /reservations?class_session=[ID]&page_size=200
async function getAllReservationsForClassSession(classSessionId) {
  const all = [];
  let page = 1;

  // Use links.next if present, otherwise fall back to ?page=N
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

  const makePayload = [];

  // 2) Process each reservation
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

    // 3) Upsert customer ONLY if email exists
    let customerRecordId = null;
    let customerFields = {};

    if (email) {
      const customerRecord = await upsertCustomer({ email, name: fullName });
      customerRecordId = customerRecord.id;
      customerFields = customerRecord.fields || {};
    } else {
      console.log("   - No email found for user; skipping customer upsert/link");
    }

    // 4) Upsert reservation ALWAYS (even if no email)
    await upsertClassReservation({
      reservationId,
      status,
      spotName,
      classRecordId,
      customerRecordId, // may be null
      isNew,
    });

    // 5) Make payload only when we have a customer record (same behavior as before)
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

  // 6) Update class record last update time
  await updateClassLastUpdate(classRecordId);

  // 7) Send to Make
  await sendMakeWebhook(makePayload);

  console.log("> Done processing class");
}

main().catch((err) => {
  console.error("ERROR in process-mtek-class.js:", err);
  process.exit(1);
});
