// scripts/send-mtek-reminders.js
import process from 'node:process';

const {
  MTEK_API_TOKEN,
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_STUDIOS_TABLE,
  MAKE_WEBHOOK_URL,
  TIMEZONE = 'America/Toronto',
} = process.env;

if (!MTEK_API_TOKEN) throw new Error('Missing env: MTEK_API_TOKEN');
if (!AIRTABLE_TOKEN) throw new Error('Missing env: AIRTABLE_TOKEN');
if (!AIRTABLE_BASE_ID) throw new Error('Missing env: AIRTABLE_BASE_ID');
if (!AIRTABLE_STUDIOS_TABLE) throw new Error('Missing env: AIRTABLE_STUDIOS_TABLE');
if (!MAKE_WEBHOOK_URL) throw new Error('Missing env: MAKE_WEBHOOK_URL');

const MTEK_BASE = 'https://bcycle.marianatek.com/api';
const AIRTABLE_BASE_URL = 'https://api.airtable.com/v0';

const MTEK_HEADERS = {
  Authorization: `Bearer ${MTEK_API_TOKEN}`,
  Accept: 'application/vnd.api+json',
};

const AIRTABLE_HEADERS = {
  Authorization: `Bearer ${AIRTABLE_TOKEN}`,
  Accept: 'application/json',
};

/**************************************************
 * Helpers
 **************************************************/
function getDateStringInTimeZone(timeZone, offsetDays = 0) {
  const now = new Date();
  now.setDate(now.getDate() + offsetDays);

  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });

  return formatter.format(now);
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed ${res.status} ${res.statusText} for ${url}: ${text}`);
  }
  return res.json();
}

/**************************************************
 * Load Studios lookup from Airtable
 **************************************************/
async function loadStudiosMap() {
  const table = encodeURIComponent(AIRTABLE_STUDIOS_TABLE);
  let url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${table}` +
            `?fields[]=MTEK%20Location%20ID&fields[]=Studio%20name&fields[]=Studio%20email`;
  const studiosByLocationId = new Map();

  while (url) {
    const data = await fetchJson(url, { headers: AIRTABLE_HEADERS });
    for (const rec of data.records || []) {
      const fields = rec.fields || {};
      const mtekId = fields['MTEK Location ID'];
      if (mtekId != null && mtekId !== '') {
        studiosByLocationId.set(String(mtekId), {
          name: fields['Studio name'] || '',
          email: fields['Studio email'] || '',
        });
      }
    }
    if (data.offset) {
      url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${table}` +
            `?fields[]=MTEK%20Location%20ID&fields[]=Studio%20name&fields[]=Studio%20email&offset=${data.offset}`;
    } else {
      url = null;
    }
  }

  return studiosByLocationId;
}

/**************************************************
 * Main
 **************************************************/
async function main() {
  const dateStr = getDateStringInTimeZone(TIMEZONE, 1); // tomorrow
  console.log(`Querying reservations for date: ${dateStr}`);

  const reservationsUrl =
    `${MTEK_BASE}/reservations` +
    `?class_session_min_date=${encodeURIComponent(dateStr)}` +
    `&class_session_max_date=${encodeURIComponent(dateStr)}` +
    `&status=pending`;

  console.log(`Reservations URL: ${reservationsUrl}`);

  const reservationsPayload = await fetchJson(reservationsUrl, { headers: MTEK_HEADERS });
  const reservations = reservationsPayload.data || [];
  console.log(`Found ${reservations.length} reservations`);

  if (!reservations.length) {
    console.log('Nothing to do, exiting.');
    return;
  }

  console.log('Loading studios from Airtable…');
  const studiosByLocationId = await loadStudiosMap();
  console.log(`Loaded ${studiosByLocationId.size} studios`);

  let processed = 0;

  for (const reservation of reservations) {
    const attrs = reservation.attributes || {};
    const rels = reservation.relationships || {};

    const guestEmail = attrs.guest_email || null;
    const userRel = rels.user?.data;
    const classSessionRel = rels.class_session?.data;

    const userId = userRel?.id || null;
    const classSessionId = classSessionRel?.id;

    if (!classSessionId) {
      console.warn(`Skipping reservation ${reservation.id}: no class_session`);
      continue;
    }

    try {
      const classSessionPromise = fetchJson(
        `${MTEK_BASE}/class_sessions/${classSessionId}`,
        { headers: MTEK_HEADERS }
      );

      let userJson = null;
      if (userId) {
        userJson = await fetchJson(`${MTEK_BASE}/users/${userId}`, { headers: MTEK_HEADERS });
      }

      const classSessionJson = await classSessionPromise;
      const classSessionData = classSessionJson.data;
      const classAttrs = classSessionData?.attributes || {};

      const userData = userJson ? userJson.data : null;
      const userAttrs = userData?.attributes || {};

      const locationId = classSessionData?.relationships?.location?.data?.id;
      let studioName = '';
      let studioEmail = '';

      if (locationId && studiosByLocationId.has(String(locationId))) {
        const studio = studiosByLocationId.get(String(locationId));
        studioName = studio.name;
        studioEmail = studio.email;
      }

      const userEmail = userAttrs.email || '';
      const firstName = userAttrs.first_name || '';

      const publicNote = (classAttrs.public_note || '').trim();
      const classTypeDisplay = classAttrs.class_type_display || '';
      const classLabel = publicNote === '' ? classTypeDisplay : publicNote;

      let instructorNames = '';
      if (Array.isArray(classAttrs.instructor_names)) {
        instructorNames = classAttrs.instructor_names.join(', ');
      } else if (typeof classAttrs.instructor_names === 'string') {
        instructorNames = classAttrs.instructor_names;
      }

      const emailTo = guestEmail || userEmail || '';

      const payload = {
        email: emailTo,
        first_name: firstName,
        guest_email: guestEmail,
        class_label: classLabel,
        instructor_names: instructorNames,
        reservation_id: reservation.id,
        class_session_id: classSessionId,
        studio_name: studioName,
        studio_email: studioEmail,
      };

      const webhookRes = await fetch(MAKE_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!webhookRes.ok) {
        const text = await webhookRes.text();
        console.warn(
          `Webhook failed for reservation ${reservation.id}: ` +
          `${webhookRes.status} ${webhookRes.statusText} – ${text}`
        );
      } else {
        processed++;
      }
    } catch (err) {
      console.error(`Error processing reservation ${reservation.id}:`, err.message);
    }
  }

  console.log(`Done. Successfully sent ${processed} reservations to the webhook.`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
