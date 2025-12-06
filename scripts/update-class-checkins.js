// scripts/update-class-checkins.js
import process from "node:process";

const {
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  MTEK_API_TOKEN,
  AIRTABLE_ALL_CLASSES_TABLE,
  AIRTABLE_VIEW_TO_UPDATE,
  AIRTABLE_FIELD_CLASS_SESSION_ID,
  AIRTABLE_FIELD_COUNT,
} = process.env;

if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing env: AIRTABLE_BASE_ID");
if (!MTEK_API_TOKEN) throw new Error("Missing env: MTEK_API_TOKEN");

const AIRTABLE_API_BASE = "https://api.airtable.com/v0";
const AIRTABLE_TABLE_NAME = AIRTABLE_ALL_CLASSES_TABLE || "All Classes";
const AIRTABLE_VIEW_NAME =
  AIRTABLE_VIEW_TO_UPDATE || "TO UPDATE DO NOT TOUCH";

const FIELD_CLASS_SESSION_ID =
  AIRTABLE_FIELD_CLASS_SESSION_ID || "Class Session ID";
const FIELD_COUNT = AIRTABLE_FIELD_COUNT || "Count";

const MTEK_BASE = "https://bcycle.marianatek.com/api";

/**
 * Fetch all records from Airtable in the "TO UPDATE DO NOT TOUCH" view.
 * You control which rows get updated by putting/removing them from that view.
 */
async function fetchAirtableRecordsFromView() {
  let records = [];
  let offset;

  do {
    const params = new URLSearchParams({ view: AIRTABLE_VIEW_NAME });
    if (offset) params.set("offset", offset);

    const url = `${AIRTABLE_API_BASE}/${AIRTABLE_BASE_ID}/${encodeURIComponent(
      AIRTABLE_TABLE_NAME
    )}?${params.toString()}`;

    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      },
    });

    if (!res.ok) {
      const bodyText = await res.text();
      throw new Error(
        `Airtable list error (${res.status}): ${bodyText || res.statusText}`
      );
    }

    const body = await res.json();
    records = records.concat(body.records || []);
    offset = body.offset;
  } while (offset);

  return records;
}

/**
 * Fetch check-in reservations for a given MTEK class_session id.
 * Endpoint: /reservations?class_session=&status=check_in&page_size=1000
 */
async function fetchCheckInCountForClassSession(classSessionId) {
  let total = 0;
  let nextUrl = `${MTEK_BASE}/reservations?class_session=${encodeURIComponent(
    classSessionId
  )}&status=check_in&page_size=1000`;

  while (nextUrl) {
    const res = await fetch(nextUrl, {
      headers: {
        Authorization: `Bearer ${MTEK_API_TOKEN}`,
        Accept: "application/vnd.api+json",
      },
    });

    if (!res.ok) {
      const bodyText = await res.text();
      throw new Error(
        `MTEK reservations error for class_session=${classSessionId} (${res.status}): ${
          bodyText || res.statusText
        }`
      );
    }

    const body = await res.json();

    // Flexible in case the shape changes a bit
    if (Array.isArray(body.data)) {
      total += body.data.length;
    } else if (Array.isArray(body.reservations)) {
      total += body.reservations.length;
    } else {
      console.warn(
        `Unexpected reservations response structure for class_session=${classSessionId}`
      );
    }

    // Basic pagination handling if present
    if (body.links && body.links.next) {
      nextUrl = body.links.next;
    } else if (body.meta && body.meta.next) {
      nextUrl = body.meta.next;
    } else {
      nextUrl = null;
    }
  }

  return total;
}

/**
 * Update Airtable Count field for a batch of records.
 * Uses PATCH with up to 10 records at a time (Airtable limit).
 */
async function updateAirtableCounts(records) {
  const BATCH_SIZE = 10;

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);

    const updates = [];

    for (const rec of batch) {
      const classSessionId = rec.fields[FIELD_CLASS_SESSION_ID];

      if (!classSessionId) {
        console.warn(
          `Record ${rec.id} has no "${FIELD_CLASS_SESSION_ID}" – skipping`
        );
        continue;
      }

      const count = await fetchCheckInCountForClassSession(classSessionId);

      console.log(
        `Record ${rec.id} | class_session=${classSessionId} | check-ins=${count}`
      );

      updates.push({
        id: rec.id,
        fields: {
          [FIELD_COUNT]: count,
        },
      });
    }

    if (!updates.length) continue;

    const url = `${AIRTABLE_API_BASE}/${AIRTABLE_BASE_ID}/${encodeURIComponent(
      AIRTABLE_TABLE_NAME
    )}`;

    const res = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ records: updates }),
    });

    if (!res.ok) {
      const bodyText = await res.text();
      throw new Error(
        `Airtable update error (${res.status}): ${bodyText || res.statusText}`
      );
    }
  }
}

async function main() {
  console.log(
    `Fetching records from Airtable base=${AIRTABLE_BASE_ID}, table="${AIRTABLE_TABLE_NAME}", view="${AIRTABLE_VIEW_NAME}"...`
  );
  const records = await fetchAirtableRecordsFromView();

  if (!records.length) {
    console.log("No records in view – nothing to update.");
    return;
  }

  console.log(`Found ${records.length} record(s) to process.`);
  await updateAirtableCounts(records);
  console.log("Done updating Count field.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
