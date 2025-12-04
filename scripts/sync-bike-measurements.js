// scripts/sync-bike-measurements.js
import process from 'node:process';

const {
  MTEK_API_TOKEN,
  AIRTABLE_TOKEN,
  CUSTOMER_BASE_ID,
  AIRTABLE_MEASUREMENTS_TABLE,
  MTEK_BASE_URL,
  MTEK_NOTE_AUTHOR,
} = process.env;

if (!MTEK_API_TOKEN) throw new Error('Missing env: MTEK_API_TOKEN');
if (!AIRTABLE_TOKEN) throw new Error('Missing env: AIRTABLE_TOKEN');
if (!CUSTOMER_BASE_ID) throw new Error('Missing env: CUSTOMER_BASE_ID');
if (!AIRTABLE_MEASUREMENTS_TABLE)
  throw new Error('Missing env: AIRTABLE_MEASUREMENTS_TABLE');

const MTEK_BASE = MTEK_BASE_URL || 'https://bcycle.marianatek.com/api';
const NOTE_AUTHOR = MTEK_NOTE_AUTHOR || 'Bike Measurement Automation';

const AIRTABLE_BASE_URL = `https://api.airtable.com/v0/${CUSTOMER_BASE_ID}/${encodeURIComponent(
  AIRTABLE_MEASUREMENTS_TABLE
)}`;

const AIRTABLE_HEADERS = {
  Authorization: `Bearer ${AIRTABLE_TOKEN}`,
  'Content-Type': 'application/json',
};

const MTEK_HEADERS = {
  Authorization: `Bearer ${MTEK_API_TOKEN}`,
  'Content-Type': 'application/vnd.api+json',
  Accept: 'application/vnd.api+json',
};

/**
 * Pull records where MEASUREMENT LAST MODIFIED is within the last 24 hours.
 * This approximates “since last update” without needing separate state storage.
 */
async function fetchRecentlyModifiedRecords() {
  const filterFormula =
    "IS_AFTER({MEASUREMENT LAST MODIFIED}, DATEADD(NOW(), -1, 'day'))";

  let records = [];
  let offset;

  do {
    const params = new URLSearchParams();
    params.append('filterByFormula', filterFormula);
    params.append('pageSize', '100');
    if (offset) params.append('offset', offset);

    const res = await fetch(`${AIRTABLE_BASE_URL}?${params.toString()}`, {
      headers: AIRTABLE_HEADERS,
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(
        `Failed to fetch Airtable records: ${res.status} ${res.statusText} - ${text}`
      );
    }

    const data = await res.json();
    records = records.concat(data.records || []);
    offset = data.offset;
  } while (offset);

  return records;
}

/**
 * Build the measurement note text based on Airtable fields.
 *
 * Bike Measurements:
 * Seat Height: {Seat Height}
 * Seat Position: {Seat Position}
 * Handlebar Height: {Handlebar Height}
 * Handlebar Position: {Handlebar Position}
 * Shoe Size: {Shoe Size}
 */
function buildMeasurementText(fields) {
  const seatHeight = fields['Seat Height'] ?? '';
  const seatPosition = fields['Seat Position'] ?? '';
  const handlebarHeight = fields['Handlebar Height'] ?? '';
  const handlebarPosition = fields['Handlebar Position'] ?? '';
  const shoeSize = fields['Shoe Size'] ?? '';

  return [
    'Bike Measurements:',
    `Seat Height: ${seatHeight}`,
    `Seat Position: ${seatPosition}`,
    `Handlebar Height: ${handlebarHeight}`,
    `Handlebar Position: ${handlebarPosition}`,
    `Shoe Size: ${shoeSize}`,
  ].join('\n');
}

/**
 * Look up a user by email in Mariana Tek: GET /users?email=...
 */
async function getUserByEmail(email) {
  if (!email) return null;

  const url = `${MTEK_BASE}/users?email=${encodeURIComponent(email)}`;
  const res = await fetch(url, {
    method: 'GET',
    headers: MTEK_HEADERS,
  });

  if (!res.ok) {
    const text = await res.text();
    console.error(
      `Failed to fetch user by email ${email}: ${res.status} ${res.statusText} - ${text}`
    );
    return null;
  }

  const data = await res.json();
  const userData = Array.isArray(data.data) ? data.data[0] : null;

  if (!userData) {
    console.log(`No Mariana Tek user found for email ${email}`);
    return null;
  }

  return userData;
}

/**
 * PUT /user_notes/{id}
 * Update an existing user note with new text (and keep it pinned).
 */
async function updateUserNote(noteId, userId, text) {
  const payload = {
    data: {
      type: 'user_notes',
      id: String(noteId),
      attributes: {
        author: NOTE_AUTHOR,
        text,
        is_pinned: true,
      },
      relationships: {
        user: {
          data: {
            type: 'users',
            id: String(userId),
          },
        },
      },
    },
  };

  const url = `${MTEK_BASE}/user_notes/${encodeURIComponent(noteId)}`;
  const res = await fetch(url, {
    method: 'PUT',
    headers: MTEK_HEADERS,
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const textBody = await res.text();
    throw new Error(
      `Failed to update user note ${noteId}: ${res.status} ${res.statusText} - ${textBody}`
    );
  }
}

/**
 * POST /user_notes
 * Create a new pinned note for a user; returns note id.
 */
async function createUserNote(userId, text) {
  const payload = {
    data: {
      type: 'user_notes',
      attributes: {
        author: NOTE_AUTHOR,
        text,
        is_pinned: true,
      },
      relationships: {
        user: {
          data: {
            type: 'users',
            id: String(userId),
          },
        },
      },
    },
  };

  const url = `${MTEK_BASE}/user_notes`;
  const res = await fetch(url, {
    method: 'POST',
    headers: MTEK_HEADERS,
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const textBody = await res.text();
    throw new Error(
      `Failed to create user note for user ${userId}: ${res.status} ${res.statusText} - ${textBody}`
    );
  }

  const data = await res.json();
  return data.data?.id;
}

/**
 * PATCH a single Airtable record with the given fields.
 */
async function updateAirtableRecord(recordId, fields) {
  if (!recordId) return;
  if (!fields || Object.keys(fields).length === 0) return;

  const payload = {
    records: [
      {
        id: recordId,
        fields,
      },
    ],
  };

  const res = await fetch(AIRTABLE_BASE_URL, {
    method: 'PATCH',
    headers: AIRTABLE_HEADERS,
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Failed to update Airtable record ${recordId}: ${res.status} ${res.statusText} - ${text}`
    );
  }
}

/**
 * Process a single Airtable record according to your logic.
 *
 * If Measurement Note ID exists:
 *   - Use USER ID MTEK from Airtable
 *   - PUT /user_notes/{id} with updated text
 *
 * If Measurement Note ID is empty:
 *   - GET /users?email=Email
 *   - If not found, stop
 *   - POST /user_notes with text
 *   - Update Airtable USER ID MTEK + Measurement Note ID
 */
async function handleRecord(record) {
  const { id: recordId, fields } = record;

  const measurementNoteId = fields['Measurement Note ID'];
  let userId = fields['USER ID MTEK'];
  const email = fields['Email'];

  const text = buildMeasurementText(fields);

  // PATH 1: Measurement Note ID exists → PUT update
  if (measurementNoteId) {
    console.log(
      `Updating existing user note ${measurementNoteId} for Airtable record ${recordId}`
    );

    if (!userId) {
      console.warn(
        `USER ID MTEK missing for record ${recordId} with Measurement Note ID ${measurementNoteId}; cannot update note.`
      );
      return;
    }

    await updateUserNote(measurementNoteId, userId, text);
    return;
  }

  // PATH 2: Measurement Note ID does NOT exist → GET user by email, POST note, update Airtable
  console.log(
    `No Measurement Note ID for record ${recordId}; will try creating a new note`
  );

  if (!email) {
    console.warn(`No Email on record ${recordId}; skipping`);
    return;
  }

  const user = await getUserByEmail(email);
  if (!user) {
    console.warn(
      `No Mariana Tek user found for email ${email}; stopping for record ${recordId}`
    );
    return;
  }

  userId = user.id;

  const noteId = await createUserNote(userId, text);

  const updateFields = {
    'USER ID MTEK': userId,
  };

  if (noteId) {
    updateFields['Measurement Note ID'] = noteId;
  }

  await updateAirtableRecord(recordId, updateFields);
}

/**
 * Main entry point.
 */
async function main() {
  console.log('Fetching Airtable records modified in the last 24 hours...');
  const records = await fetchRecentlyModifiedRecords();
  console.log(`Found ${records.length} record(s) to process.`);

  for (const record of records) {
    try {
      await handleRecord(record);
    } catch (err) {
      console.error(
        `Error processing Airtable record ${record.id}:`,
        err?.message || err
      );
    }
  }

  console.log('Bike measurement sync complete.');
}

main().catch((err) => {
  console.error('Fatal error in sync-bike-measurements:', err);
  process.exit(1);
});
