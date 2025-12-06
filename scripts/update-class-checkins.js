// scripts/update-class-checkins.js

import process from "node:process";

const {
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  MTEK_API_TOKEN,
  AIRTABLE_ALL_CLASSES_TABLE,
  AIRTABLE_VIEW_TO_UPDATE,
  AIRTABLE_FIELD_CLASS_ID,
  AIRTABLE_FIELD_COUNT,
} = process.env;

if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing env: AIRTABLE_BASE_ID");
if (!MTEK_API_TOKEN) throw new Error("Missing env: MTEK_API_TOKEN");

const AIRTABLE_API_BASE = "https://api.airtable.com/v0";
const AIRTABLE_TABLE_NAME = AIRTABLE_ALL_CLASSES_TABLE || "All Classes";
const AIRTABLE_VIEW_NAME =
  AIRTABLE_VIEW_TO_UPDATE || "TO UPDATE DO NOT TOUCH";

const FIELD_CLASS_ID = AIRTABLE_FIELD_CLASS_ID || "Class ID";
const FIELD_COUNT = AIRTABLE_FIELD_COUNT || "Count";

const MTEK_BASE = "https://bcycle.marianatek.com/api";

/**
 * Fetch all records from Airtable in the "TO UPDATE DO NOT TOUCH" view.
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
      const body = await res.text();
      throw new Error(
        `Airtable list error (${res.status}): ${body || res.statusText}`
      );
    }

    const data = await res.json();
    records = records.concat(data.records || []);
    offset = data.offset;
  } while (offset);

  return records;
}

/**
 * Fetch check-in reservation count from Mariana Tek for a class session.
 * Uses: /reservations?class_session=&status=check_in&page_size=1000
 */
async function fetchCheckInCountForClassId(classId) {
  let total = 0;

  let nextUrl = `${MTEK_BASE}/reservations/?class_session=${encodeURIComponent(
    classId
  )}&status=check in&page_size=1000`;

  while (nextUrl) {
    const res = await fetch(nextUrl, {
      headers: {
        Authorization: `Bearer ${MTEK_API_TOKEN}`,
        Accept: "application/vnd.api+json",
      },
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(
        `MTEK reservations error for class_session=${classId} (${res.status}): ${
          body || res.statusText
        }`
      );
    }

    const data = await res.json();

    if (Array.isArray(data.data)) {
      total += data.data.length;
    } else if (Array.isArray(data.reservations)) {
      total += data.reservations.length;
    } else {
      console.warn(
        `Unexpected reservations structure for class_session=${classId}`
      );
    }

    if (data.links && data.links.next) {
      nextUrl = data.links.next;
    } else if (data.meta && data.meta.next) {
      nextUrl = data.meta.next;
    } else {
      nextUrl = null;
    }
  }

  return total;
}

/**
 * Update Airtable Count field in batches of 10.
 */
async function updateAirtableCounts(records) {
  const BATCH_SIZE = 10;

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const updates = [];

    for (const rec of batch) {
      const classId = rec.fields[FIELD_CLASS_ID];

      if (!classId) {
        console.warn(`Record ${rec.id} missing "${FIELD_CLASS_ID}" — skipped`);
        continue;
      }

      const checkIns = await fetchCheckInCountForClassId(classId);

      console.log(
        `Record ${rec.id} | Class ID=${classId} | check-ins=${checkIns}`
      );

      updates.push({
        id: rec.id,
        fields: { [FIELD_COUNT]: checkIns },
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
      const body = await res.text();
      throw new Error(
        `Airtable update error (${res.status}): ${body || res.statusText}`
      );
    }
  }
}

async function main() {
  console.log(
    `Fetching Airtable records from base=${AIRTABLE_BASE_ID}, table="${AIRTABLE_TABLE_NAME}", view="${AIRTABLE_VIEW_NAME}"…`
  );

  const records = await fetchAirtableRecordsFromView();

  if (!records.length) {
    console.log("No records in view — nothing to update.");
    return;
  }

  console.log(`Found ${records.length} record(s) to update.`);
  await updateAirtableCounts(records);
  console.log("All Count values updated.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
