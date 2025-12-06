// scripts/process-mtek-class.js
import fs from "node:fs";
import process from "node:process";

// Base + API URLs (from your working Postman URL)
const AIRTABLE_BASE_ID = "appofCRTxHoIe6dXI";
const AIRTABLE_BASE_URL = "https://api.airtable.com/v0";

const { AIRTABLE_TOKEN, MTEK_API_TOKEN } = process.env;

// Hard-coded Make webhook URL (you provided this)
const SECOND_CLASS_WEBHOOK_URL =
  "https://hook.us2.make.com/njbqpqqh6i6lxr34ycro62pzh6ip5h33";

if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!MTEK_API_TOKEN) throw new Error("Missing env: MTEK_API_TOKEN");

const MTEK_BASE = "https://bcycle.marianatek.com/api";

// Airtable table names in base appofCRTxHoIe6dXI
// Classes table is "CTT SYNC DO NOT TOUCH"
const CLASSES_TABLE_NAME = "CTT SYNC DO NOT TOUCH";
const CLASSES_TABLE_SEGMENT = encodeURIComponent(CLASSES_TABLE_NAME);
const CLASS_RESERVATIONS_TABLE = "Class Reservations";
const CUSTOMERS_TABLE = "Customers";

// ------------------------------------------------------------
// Helper to strip zero-width & stray whitespace from IDs
// ------------------------------------------------------------
function sanitizeId(id) {
  if (!id) return id;
  return String(id)
    // remove zero-width chars, BOM, etc.
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .trim();
}

// ------------------------------------------------------------
// Generic helpers
// ------------------------------------------------------------

function getClassRecordIdFromEvent() {
  const eventPath = process.env.GITHUB_EVENT_PATH;
  if (!eventPath) {
    throw new Error("GITHUB_EVENT_PATH not set");
  }

  const raw = fs.readFileSync(eventPath, "utf8");
  const event = JSON.parse(raw);

  console.log("Repository dispatch payload:", JSON.stringify(event, null, 2));

  const rawId =
    event.client_payload?.airtable_record_id ||
    event.client_payload?.recordId ||
    null;

  if (!rawId) {
    throw new Error(
      "No airtable_record_id / recordId found in repository_dispatch payload"
    );
  }

  const cleanId = sanitizeId(rawId);

  console.log("> Raw Airtable class record id from dispatch:", JSON.stringify(rawId));
  console.log("> Cleaned Airtable class record id:", JSON.stringify(cleanId));

  return cleanId;
}

async function airtableGetClassRecord(recordId) {
  const cleanId = sanitizeId(recordId);

  // EXACTLY matches your working pattern, with safe encoding
  const url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${CLASSES_TABLE_SEGMENT}/${cleanId}`;
  console.log("Airtable GET class URL:", url);

  const res = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Airtable GET ${url} failed: ${res.status} ${text}`);
  }

  return res.json();
}

async function airtableRequestTable(tableName, options = {}) {
  const url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    tableName
  )}`;
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
      `Airtable ${options.method || "GET"} ${url} failed: ${
        res.status
      } ${text}`
    );
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
// Airtable helpers
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
  if (!record) {
    throw new Error("No customer record returned from Airtable upsert");
  }

  console.log(`> Upserted customer ${lowerEmail} -> ${record.id}`);
  return record; // { id, fields: {...} }
}

async function upsertClassReservation({
  reservation,
  spotName,
  classRecordId,
  customerRecordId,
  isNew,
}) {
  const cleanClassId = sanitizeId(classRecordId);

  const body = {
    performUpsert: {
      fieldsToMergeOn: ["Reservation ID"],
    },
    records: [
      {
        fields: {
          "Reservation ID": reservation.data.id,
          Status: reservation.data.attributes.status,
          "Reservation Date": reservation.data.attributes.creation_date,
          "Spot number": spotName || null,
          Classes: [cleanClassId],
          Customer: [customerRecordId],
          "NEW?": isNew,
        },
      },
    ],
  };

  await airtableRequestTable(CLASS_RESERVATIONS_TABLE, {
    method: "PATCH",
    body: JSON.stringify(body),
  });

  console.log(
    `> Upserted class reservation ${reservation.data.id} for customer ${customerRecordId}`
  );
}

async function updateClassLastUpdate(classRecordId) {
  const cleanId = sanitizeId(classRecordId);
  const now = new Date().toISOString();

  const body = {
    records: [
      {
        id: cleanId,
        fields: {
          "Last update time": now,
        },
      },
    ],
  };

  const url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${CLASSES_TABLE_SEGMENT}`;
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

  console.log(`> Updated Last update time on class ${cleanId} -> ${now}`);
}

// ------------------------------------------------------------
// MTEK helpers
// ------------------------------------------------------------

// Assumes MTEK endpoint: /class_sessions/{id}?include=reservations
async function getClassSessionWithReservations(classId) {
  return mtekRequest(`/class_sessions/${classId}?include=reservations`);
}

function extractReservationIdsFromSession(sessionJson) {
  const included = sessionJson.included || [];
  const reservations = included.filter((i) => i.type === "reservations");
  return reservations.map((r) => r.id);
}

async function getReservation(reservationId) {
  return mtekRequest(`/reservations/${reservationId}`);
}

async function getUser(userId) {
  return mtekRequest(`/users/${userId}`);
}

async function getSpot(spotId) {
  return mtekRequest(`/spots/${spotId}`);
}

function isActiveReservationStatus(status) {
  const s = (status || "").toLowerCase();
  return !s.includes("cancel") && !s.includes("waitlist");
}

function hasNewTag463(reservation) {
  const tags = reservation.data.relationships?.tags?.data || [];
  return tags.some((t) => String(t.id) === "463");
}

// ------------------------------------------------------------
// Make webhook
// ------------------------------------------------------------

async function sendMakeWebhook(reservationsPayload) {
  if (!reservationsPayload.length) {
    console.log("> No reservations to send to Make webhook");
    return;
  }

  const res = await fetch(SECOND_CLASS_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ reservations: reservationsPayload }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Make webhook POST failed: ${res.status} ${text}`);
  }

  console.log(
    `> Sent ${reservationsPayload.length} reservations to Make webhook`
  );
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------

async function main() {
  const classRecordId = getClassRecordIdFromEvent();

  // 1) Get class record from Airtable and read the MTEK Class ID
  const classRecord = await airtableGetClassRecord(classRecordId);
  const classFields = classRecord.fields || {};

  const classIdFieldNames = ["Class ID", "MTEK Class ID", "Class Session ID"];
  const mtekClassId = classIdFieldNames
    .map((f) => classFields[f])
    .find((v) => v && String(v).trim() !== "");

  if (!mtekClassId) {
    throw new Error(
      `No MTEK class id found on Classes record ${classRecordId} (checked fields: ${classIdFieldNames.join(
        ", "
      )})`
    );
  }

  console.log(`> MTEK Class Session ID: ${mtekClassId}`);

  // 2) Get class session + reservations from MTEK
  const classSession = await getClassSessionWithReservations(mtekClassId);
  const reservationIds = extractReservationIdsFromSession(classSession);

  console.log(
    `> Found ${reservationIds.length} reservation(s) for session ${mtekClassId}`
  );

  const makePayload = [];

  // 3) Loop through each reservation
  for (const reservationId of reservationIds) {
    console.log(`>> Processing reservation ${reservationId}`);

    const reservation = await getReservation(reservationId);
    const status = reservation.data.attributes.status;

    if (!isActiveReservationStatus(status)) {
      console.log(`   - Skipping (status = ${status})`);
      continue;
    }

    const userRel = reservation.data.relationships?.user?.data;
    const spotRel = reservation.data.relationships?.spot?.data;

    const userId = userRel?.id;
    const spotId = spotRel?.id;

    const [user, spot] = await Promise.all([
      userId ? getUser(userId) : null,
      spotId ? getSpot(spotId) : null,
    ]);

    const userAttrs = user?.data?.attributes || {};
    const spotAttrs = spot?.data?.attributes || {};

    const email = userAttrs.email || userAttrs.email_address || null;

    if (!email) {
      console.log("   - Skipping reservation (no user email)");
      continue;
    }

    const fullName = userAttrs.full_name || userAttrs.name || null;
    const spotName = spotAttrs.name || null;
    const isNew = hasNewTag463(reservation);

    // 4) Upsert customer in Customers table
    const customerRecord = await upsertCustomer({
      email,
      name: fullName,
    });

    const customerRecordId = customerRecord.id;
    const customerFields = customerRecord.fields || {};

    // 5) Upsert reservation in Class Reservations table
    await upsertClassReservation({
      reservation,
      spotName,
      classRecordId,
      customerRecordId,
      isNew,
    });

    // 6) Add to Make payload (from customer record fields)
    makePayload.push({
      customerRecordId,
      measurementNoteId: customerFields["Measurement Note ID"] || null,
      updatedBoardNameSpivi:
        customerFields["Updated board name in Spivi"] || null,
      oldZfBoardName: customerFields["OLD ZF BOARD NAME"] || null,
    });
  }

  // 7) Update "Last update time" on the class record
  await updateClassLastUpdate(classRecordId);

  // 8) Send webhook to Make with array of reservations
  await sendMakeWebhook(makePayload);

  console.log("> Done processing class");
}

main().catch((err) => {
  console.error("ERROR in process-mtek-class.js:", err);
  process.exit(1);
});
