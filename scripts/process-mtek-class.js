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

// Mariana Tek (MTEK)
const MTEK_BASE = "https://bcycle.marianatek.com/api";

// Tables
const CLASSES_TABLE_NAME = "CTT SYNC DO NOT TOUCH";
const CLASS_RESERVATIONS_TABLE = "Class Reservations";
const CUSTOMERS_TABLE = "Customers";

// Airtable field mappings (EDIT THESE TO MATCH YOUR BASE)
const CLASS_FIELDS = {
  LAST_UPDATE_TIME: "Last update time", // <-- confirm exact field name
};

const RES_FIELDS = {
  // Strongly recommended to have a text field that stores the MTEK reservation id
  RESERVATION_ID: "MTEK Reservation ID", // <-- confirm exact field name (text)
  STATUS: "Status", // text or single select
  SPOT_NAME: "Spot number", // or "Spot" / "Spot Name" (text)
  CLASS_LINK: "Class!", // linked record to class table
  CUSTOMER_LINK: "Customer", // linked record to Customers table
  IS_NEW: "New?", // checkbox or single select
  USER_ID: "MTEK User ID", // optional
  SPOT_ID: "MTEK Spot ID", // optional
  EMAIL: "Email", // optional (store the email we used)
};

// Webhook
const SECOND_CLASS_WEBHOOK_URL =
  "https://hook.us2.make.com/njbqpqqh6i6lxr34ycro62pzh6ip5h33";

// Tokens
const { AIRTABLE_TOKEN, MTEK_API_TOKEN } = process.env;
if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!MTEK_API_TOKEN) throw new Error("Missing env: MTEK_API_TOKEN");

// Pagination
const PAGE_SIZE = 100;

// ------------------------------------------------------------
// Helper to strip zero-width & stray whitespace from IDs
// ------------------------------------------------------------
function sanitizeId(id) {
  if (!id) return id;
  return String(id).replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
}

// ------------------------------------------------------------
// Read payload from repository_dispatch
// ------------------------------------------------------------
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
  const rawMtekClassId = event.client_payload?.mtek_class_id || null;

  if (!rawClassRecordId) throw new Error("No airtable_record_id in client_payload");
  if (!rawMtekClassId) throw new Error("No mtek_class_id in client_payload");

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
  const url = `${MTEK_BASE}${path}`;
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
  reservation,
  spotName,
  classRecordId,
  customerRecordId,
  isNew,
  userId,
  spotId,
  email,
}) {
  const reservationId = reservation?.data?.id ? String(reservation.data.id) : null;
  if (!reservationId) throw new Error("Reservation missing data.id");

  const status = reservation?.data?.attributes?.status ?? "";

  // NOTE: performUpsert requires the merge fields to already exist in the table schema.
  // Make sure RES_FIELDS.RESERVATION_ID points to an existing field.
  const body = {
    performUpsert: {
      fieldsToMergeOn: [RES_FIELDS.RESERVATION_ID],
    },
    records: [
      {
        fields: {
          [RES_FIELDS.RESERVATION_ID]: reservationId,
          [RES_FIELDS.STATUS]: status,
          [RES_FIELDS.SPOT_NAME]: spotName || "",
          [RES_FIELDS.CLASS_LINK]: [classRecordId],
          [RES_FIELDS.CUSTOMER_LINK]: [customerRecordId],
          [RES_FIELDS.IS_NEW]: !!isNew,
          ...(RES_FIELDS.USER_ID ? { [RES_FIELDS.USER_ID]: userId || "" } : {}),
          ...(RES_FIELDS.SPOT_ID ? { [RES_FIELDS.SPOT_ID]: spotId || "" } : {}),
          ...(RES_FIELDS.EMAIL ? { [RES_FIELDS.EMAIL]: email || "" } : {}),
        },
      },
    ],
  };

  const json = await airtableRequestTable(CLASS_RESERVATIONS_TABLE, {
    method: "PATCH",
    body: JSON.stringify(body),
  });

  const record = json.records?.[0];
  if (!record) throw new Error("No class reservation record returned from Airtable upsert");

  console.log(`> Upserted reservation ${reservationId} -> ${record.id} (status=${status})`);
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

// Fetch reservation IDs via relationship endpoint + pagination (so you actually get them all)
async function getAllReservationIdsForClassSession(classId) {
  const ids = [];
  let path = `/class_sessions/${classId}/relationships/reservations?page[size]=${PAGE_SIZE}`;

  while (path) {
    const json = await mtekRequest(path);

    const data = Array.isArray(json?.data) ? json.data : [];
    for (const item of data) {
      if (item?.id != null) ids.push(String(item.id));
    }

    // JSON:API style "links.next" pagination
    const next = json?.links?.next || null;
    if (next) {
      // sometimes it's full URL, sometimes relative
      path = next.startsWith("http") ? next.replace(MTEK_BASE, "") : next;
    } else {
      path = null;
    }
  }

  return [...new Set(ids)];
}

async function getReservation(reservationId) {
  return mtekRequest(`/reservations/${reservationId}?include=tags`);
}

async function getUser(userId) {
  return mtekRequest(`/users/${userId}`);
}

async function getSpot(spotId) {
  return mtekRequest(`/spots/${spotId}`);
}

// IMPORTANT: We now process ALL statuses (including cancel + waitlist)
function shouldProcessReservation(_status) {
  return true;
}

function hasNewTag463(reservation) {
  const tags = reservation?.data?.relationships?.tags?.data || [];
  return tags.some((t) => String(t.id) === "463");
}

// ------------------------------------------------------------
// Make webhook
// ------------------------------------------------------------
async function sendMakeWebhook(reservationsPayload) {
  const res = await fetch(SECOND_CLASS_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(reservationsPayload),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Make webhook failed: ${res.status} ${text}`);
  }

  console.log(`> Sent ${reservationsPayload.length} item(s) to Make webhook`);
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------
async function main() {
  const { classRecordId, mtekClassId } = getPayloadFromEvent();

  console.log(`> Using MTEK Class Session ID from payload: ${mtekClassId}`);

  // 1) Get *all* reservation IDs (paginated)
  const reservationIds = await getAllReservationIdsForClassSession(mtekClassId);

  console.log(
    `> Found ${reservationIds.length} reservation(s) for session ${mtekClassId}`
  );
  console.log("> Reservation IDs (sample):", reservationIds.slice(0, 10));

  const makePayload = [];

  // 2) Loop through each reservation
  for (const reservationId of reservationIds) {
    console.log(`>> Processing reservation ${reservationId}`);

    const reservation = await getReservation(reservationId);
    const status = reservation?.data?.attributes?.status ?? "";

    if (!shouldProcessReservation(status)) {
      console.log(`   - Skipping by rule (status = ${status})`);
      continue;
    }

    const userRel = reservation?.data?.relationships?.user?.data;
    const spotRel = reservation?.data?.relationships?.spot?.data;

    const userId = userRel?.id ? String(userRel.id) : null;
    const spotId = spotRel?.id ? String(spotRel.id) : null;

    const [user, spot] = await Promise.all([
      userId ? getUser(userId) : null,
      spotId ? getSpot(spotId) : null,
    ]);

    const userAttrs = user?.data?.attributes || {};
    const spotAttrs = spot?.data?.attributes || {};

    const email = userAttrs.email || userAttrs.email_address || null;
    const fullName = userAttrs.full_name || userAttrs.name || null;
    const spotName = spotAttrs.name || null;

    const isNew = hasNewTag463(reservation);

    // If you truly need *every* reservation in Airtable, you can remove this skip
    // and upsert a customer-less reservation keyed by userId.
    if (!email) {
      console.log(`   - Skipping reservation (no user email). status=${status}, userId=${userId}`);
      continue;
    }

    // 3) Upsert customer in Customers table
    const customerRecord = await upsertCustomer({
      email,
      name: fullName,
    });

    const customerRecordId = customerRecord.id;
    const customerFields = customerRecord.fields || {};

    // 4) Upsert reservation in Class Reservations table (now includes cancel/waitlist too)
    await upsertClassReservation({
      reservation,
      spotName,
      classRecordId,
      customerRecordId,
      isNew,
      userId,
      spotId,
      email,
    });

    // 5) Add to Make payload (from customer record fields)
    makePayload.push({
      classRecordId,
      mtekClassId,
      reservationId: String(reservation?.data?.id || ""),
      reservationStatus: status,
      customerRecordId,
      measurementNoteId: customerFields["Measurement Note ID"] || null,
      updatedBoardNameSpivi: customerFields["Updated board name in Spivi"] || null,
      oldZfBoardName: customerFields["OLD ZF BOARD NAME"] || null,
    });
  }

  // 6) Update "Last update time" on the class record
  await updateClassLastUpdate(classRecordId);

  // 7) Send webhook to Make with array of reservations
  await sendMakeWebhook(makePayload);

  console.log("> Done processing class");
}

main().catch((err) => {
  console.error("ERROR in process-mtek-class.js:", err);
  process.exit(1);
});
