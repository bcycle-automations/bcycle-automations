// scripts/get-new-people-today.js

'use strict';

// --------- Config ---------

const MTEK_BASE_URL = 'https://bcycle.marianatek.com/api';

// Airtable base + table names
const AIRTABLE_BASE_ID = 'appofCRTxHoIe6dXI';
const AIRTABLE_BASE_URL = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

const AIRTABLE_TABLE_CTT = 'CTT SYNC DO NOT TOUCH';
const AIRTABLE_TABLE_CUSTOMERS = 'Customers';

// Airtable field names (NOT IDs) – change if yours differ
const FIELD_PHONE_NUMBER = 'Phone number';
const FIELD_FIRST_CLASS_DATE = 'First Class Date (Imported)';
const FIELD_PROFILE_CREATED = 'Profile Created';
const FIELD_FIRST_CLASS_LINK = 'First Class';

// New-people tag ID in MTEK
const NEW_PEOPLE_TAG_ID = '463';

// --------- Env ---------

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const MTEK_API_TOKEN = process.env.MTEK_API_TOKEN;

if (!AIRTABLE_TOKEN || !MTEK_API_TOKEN) {
  console.error('Missing required env vars: AIRTABLE_TOKEN or MTEK_API_TOKEN');
  process.exit(1);
}

// --------- Date helpers (Eastern Time) ---------

function formatYMD(dateObj) {
  const y = dateObj.getFullYear();
  const m = String(dateObj.getMonth() + 1).padStart(2, '0');
  const d = String(dateObj.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/**
 * Get today & tomorrow in America/New_York as YYYY-MM-DD strings.
 */
function getTodayAndTomorrowEastern() {
  const nowUtc = new Date();

  const nowET = new Date(
    nowUtc.toLocaleString('en-US', { timeZone: 'America/New_York' })
  );

  const todayStr = formatYMD(nowET);

  const tomorrowET = new Date(nowET);
  tomorrowET.setDate(nowET.getDate() + 1);
  const tomorrowStr = formatYMD(tomorrowET);

  return { todayStr, tomorrowStr };
}

// --------- HTTP helper ---------

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  const text = await res.text();

  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch (e) {
    console.error('Failed to parse JSON from:', url);
    console.error('Raw response:', text);
    throw e;
  }

  if (!res.ok) {
    console.error('Request failed:', url);
    console.error('Status:', res.status, res.statusText);
    console.error('Response:', JSON.stringify(json, null, 2));
    throw new Error(`HTTP ${res.status} for ${url}`);
  }

  return json;
}

// --------- MTEK helpers ---------

async function fetchReservationsForToday() {
  const { todayStr, tomorrowStr } = getTodayAndTomorrowEastern();

  const params = new URLSearchParams({
    class_session_min_date: todayStr,
    class_session_max_date: tomorrowStr,
    status: 'pending',
    page_size: '2000'
  });

  const url = `${MTEK_BASE_URL}/reservations?${params.toString()}`;

  console.log(`Fetching reservations from MTEK (ET dates): ${url}`);

  const json = await fetchJson(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: 'application/vnd.api+json'
    }
  });

  const data = json.data || [];
  console.log(`Fetched ${data.length} reservations for today (ET).`);
  return data;
}

async function fetchUserById(userId) {
  if (!userId) return null;

  const url = `${MTEK_BASE_URL}/users/${encodeURIComponent(userId)}/`;
  const json = await fetchJson(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: 'application/vnd.api+json'
    }
  });

  if (Array.isArray(json.data)) {
    return json.data[0] || null;
  }

  return json.data || null;
}

async function fetchUserByEmail(email) {
  if (!email) return null;

  const params = new URLSearchParams({ email });
  const url = `${MTEK_BASE_URL}/users?${params.toString()}`;

  const json = await fetchJson(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: 'application/vnd.api+json'
    }
  });

  const users = json.data || [];
  if (!users.length) {
    console.log(`No MTEK user found by email: ${email}`);
    return null;
  }

  return users[0];
}

async function fetchClassSession(classSessionId) {
  if (!classSessionId) return null;

  const url = `${MTEK_BASE_URL}/class_sessions/${encodeURIComponent(
    classSessionId
  )}`;
  const json = await fetchJson(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: 'application/vnd.api+json'
    }
  });

  if (Array.isArray(json.data)) {
    return json.data[0] || null;
  }

  return json.data || null;
}

// --------- Airtable helpers ---------

async function findCTTRecordByClassId(classSessionId) {
  const formula = `{Class ID} = "${String(classSessionId).replace(/"/g, '\\"')}"`;

  const params = new URLSearchParams({
    filterByFormula: formula,
    maxRecords: '1',
    pageSize: '1'
  });

  const url = `${AIRTABLE_BASE_URL}/${encodeURIComponent(
    AIRTABLE_TABLE_CTT
  )}?${params.toString()}`;

  const json = await fetchJson(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      Accept: 'application/json'
    }
  });

  const records = json.records || [];
  if (!records.length) {
    console.log(`No CTT record found for Class ID=${classSessionId}`);
    return null;
  }

  return records[0];
}

async function findCustomerByEmail(email) {
  if (!email) return null;

  const formula = `{Email} = "${String(email).replace(/"/g, '\\"')}"`;

  const params = new URLSearchParams({
    filterByFormula: formula,
    maxRecords: '1',
    pageSize: '1'
  });

  const url = `${AIRTABLE_BASE_URL}/${encodeURIComponent(
    AIRTABLE_TABLE_CUSTOMERS
  )}?${params.toString()}`;

  const json = await fetchJson(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      Accept: 'application/json'
    }
  });

  const records = json.records || [];
  if (!records.length) {
    console.log(`No Customer found in Airtable for email=${email}`);
    return null;
  }

  return records[0];
}

async function createCustomerRecord(initialFields) {
  const url = `${AIRTABLE_BASE_URL}/${encodeURIComponent(
    AIRTABLE_TABLE_CUSTOMERS
  )}`;

  const body = {
    records: [
      {
        fields: initialFields
      }
    ]
  };

  const json = await fetchJson(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    body: JSON.stringify(body)
  });

  const records = json.records || [];
  if (!records.length) {
    console.error('Airtable createCustomerRecord: no records returned');
    return null;
  }

  return records[0];
}

async function updateCustomerRecord(customerRecordId, fields) {
  const url = `${AIRTABLE_BASE_URL}/${encodeURIComponent(
    AIRTABLE_TABLE_CUSTOMERS
  )}`;

  const body = {
    records: [
      {
        id: customerRecordId,
        fields
      }
    ]
  };

  const json = await fetchJson(url, {
    method: 'PATCH',
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    body: JSON.stringify(body)
  });

  return json;
}

// --------- Core reservation processing ---------

function reservationHasNewPeopleTag(reservation) {
  const tags = reservation?.relationships?.tags?.data || [];
  return tags.some((t) => String(t.id) === NEW_PEOPLE_TAG_ID);
}

async function processReservation(reservation) {
  const resId = reservation.id;
  const attrs = reservation.attributes || {};

  if (!reservationHasNewPeopleTag(reservation)) {
    return { skipped: true, reason: 'no-new-people-tag' };
  }

  const classSessionId =
    reservation?.relationships?.class_session?.data?.id || null;
  if (!classSessionId) {
    console.log(`Reservation ${resId} has no class_session.id – skipping`);
    return { skipped: true, reason: 'no-class-session-id' };
  }

  const guestEmail = attrs.guest_email || null;

  let userData = null;
  let emailForCustomerSearch = null;

  if (guestEmail) {
    // Guest flow: use guest_email for user lookup + Airtable lookup
    console.log(`Reservation ${resId}: guest_email=${guestEmail}`);
    userData = await fetchUserByEmail(guestEmail);
    if (!userData) {
      console.log(`Reservation ${resId}: no user found by guest_email – skipping`);
      return { skipped: true, reason: 'no-user-guest-email' };
    }
    emailForCustomerSearch = guestEmail;
  } else {
    // Normal flow: fetch user by ID, then use that email
    const userId = reservation?.relationships?.user?.data?.id || null;
    if (!userId) {
      console.log(`Reservation ${resId}: no user.id – skipping`);
      return { skipped: true, reason: 'no-user-id' };
    }

    userData = await fetchUserById(userId);
    if (!userData) {
      console.log(`Reservation ${resId}: user not found in MTEK – skipping`);
      return { skipped: true, reason: 'user-not-found' };
    }

    const userEmail = userData?.attributes?.email || null;
    if (!userEmail) {
      console.log(`Reservation ${resId}: user has no email – skipping`);
      return { skipped: true, reason: 'user-has-no-email' };
    }

    emailForCustomerSearch = userEmail;
  }

  // Find CTT class record
  const cttRecord = await findCTTRecordByClassId(classSessionId);
  if (!cttRecord) {
    return { skipped: true, reason: 'no-ctt-record' };
  }

  const cttRecordId = cttRecord.id;

  // Find or create Customer by email (guest or normal)
  let customerRecord = await findCustomerByEmail(emailForCustomerSearch);
  if (!customerRecord) {
    console.log(
      `No Customer found for email=${emailForCustomerSearch}, creating new record.`
    );
    const newCustomer = await createCustomerRecord({
      Email: emailForCustomerSearch
    });

    if (!newCustomer) {
      console.log(
        `Reservation ${resId}: failed to create Customer record – skipping`
      );
      return { skipped: true, reason: 'customer-create-failed' };
    }

    customerRecord = newCustomer;
  }

  const customerRecordId = customerRecord.id;

  // Get class session info for start_datetime
  const classSession = await fetchClassSession(classSessionId);
  const classAttrs = classSession?.attributes || {};
  const startDatetime = classAttrs.start_datetime || null;

  const userAttrs = userData.attributes || {};
  const phoneNumber = userAttrs.phone_number || null;
  const dateJoined = userAttrs.date_joined || null;

  const fieldsToUpdate = {};

  if (phoneNumber !== null) {
    fieldsToUpdate[FIELD_PHONE_NUMBER] = phoneNumber;
  }

  if (startDatetime !== null) {
    fieldsToUpdate[FIELD_FIRST_CLASS_DATE] = startDatetime;
  }

  if (dateJoined !== null) {
    fieldsToUpdate[FIELD_PROFILE_CREATED] = dateJoined;
  }

  // Single linked record to the CTT class record
  fieldsToUpdate[FIELD_FIRST_CLASS_LINK] = [cttRecordId];

  console.log(
    `Updating Customer ${customerRecordId} for reservation ${resId}, email=${emailForCustomerSearch}`
  );

  await updateCustomerRecord(customerRecordId, fieldsToUpdate);

  return { skipped: false };
}

// --------- Main ---------

(async () => {
  try {
    const reservations = await fetchReservationsForToday();

    let processed = 0;
    let skipped = 0;

    for (const reservation of reservations) {
      try {
        const result = await processReservation(reservation);
        if (result.skipped) {
          skipped++;
        } else {
          processed++;
        }
      } catch (err) {
        skipped++;
        console.error(`Error processing reservation ${reservation.id}:`, err);
      }
    }

    console.log(
      `Done. Processed=${processed}, Skipped=${skipped}, Total=${reservations.length}`
    );
  } catch (err) {
    console.error('Fatal error in Get New People Today job:', err);
    process.exit(1);
  }
})();
