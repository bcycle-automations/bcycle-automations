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

// Hours (local, 24h) when studios have classes: 7–12 & 16–20
const ACTIVE_HOURS = new Set([6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]);

/**************************************************
 * Helpers
 **************************************************/
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Get local date + hour in given timezone, offset by offsetDays
function getLocalDateHour(timeZone, offsetDays = 0) {
  const now = new Date();
  now.setDate(now.getDate() + offsetDays);

  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    hour12: false,
  });

  const parts = formatter.formatToParts(now);
  const get = type => parts.find(p => p.type === type)?.value;

  const year = get('year');
  const month = get('month');
  const day = get('day');
  const hourStr = get('hour');
  const hour = Number(hourStr);

  return { year, month, day, hour };
}

// Simple JSON fetch (no special rate-limit logic) – used for Airtable
async function fetchJsonSimple(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed ${res.status} ${res.statusText} for ${url}: ${text}`);
  }
  return res.json();
}

// Rate-limit-aware JSON fetch – used for MTEK API
async function fetchJsonWithRateLimit(url, options = {}, maxRetries = 5) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const res = await fetch(url, options);

    if (res.status !== 429) {
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Request failed ${res.status} ${res.statusText} for ${url}: ${text}`);
      }
      return res.json();
    }

    // 429 Too Many Requests – inspect Retry-After
    const retryAfterHeader = res.headers.get('Retry-After');
    let delayMs = 0;

    if (retryAfterHeader) {
      const secs = Number(retryAfterHeader);
      if (!Number.isNaN(secs)) {
        delayMs = secs * 1000;
      } else {
        const retryDate = new Date(retryAfterHeader);
        if (!Number.isNaN(retryDate.getTime())) {
          delayMs = retryDate.getTime() - Date.now();
        }
      }
    }

    if (!delayMs || delayMs < 0) {
      delayMs = 2000; // fallback: 2 seconds
    }

    console.warn(
      `Got 429 from MTEK for ${url}. Retry-After=${retryAfterHeader || 'n/a'}; ` +
      `waiting ${Math.round(delayMs / 1000)}s before retry (attempt ${attempt}/${maxRetries})`
    );

    await sleep(delayMs);
  }

  throw new Error(`Exceeded max retries (${maxRetries}) for ${url} after repeated 429s.`);
}

/**************************************************
 * Load Studios lookup from Airtable (once per run)
 **************************************************/
async function loadStudiosMap() {
  const table = encodeURIComponent(AIRTABLE_STUDIOS_TABLE);
  let url = `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${table}` +
            `?fields[]=MTEK%20Location%20ID&fields[]=Studio%20name&fields[]=Studio%20email`;

  const studiosByLocationId = new Map();

  while (url) {
    const data = await fetchJsonSimple(url, { headers: AIRTABLE_HEADERS });

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
  // Local time "now" in TIMEZONE, but using tomorrow's date
  const { year, month, day, hour } = getLocalDateHour(TIMEZONE, 1); // +1 day = tomorrow

  console.log(`Local hour in ${TIMEZONE}: ${hour}:00 (tomorrow's date ${year}-${month}-${day})`);

  if (!ACTIVE_HOURS.has(hour)) {
    console.log('Current hour is outside active class hours, nothing to do. Exiting.');
    return;
  }

  const hourStr = String(hour).padStart(2, '0');
  const nextHour = (hour + 1) % 24;
  const nextHourStr = String(nextHour).padStart(2, '0');

  const startDateTime = `${year}-${month}-${day}T${hourStr}:00:00`;
  const endDateTime   = `${year}-${month}-${day}T${nextHourStr}:00:00`;

  console.log(`Querying reservations for window: ${startDateTime} → ${endDateTime}`);

  const reservationsUrl =
    `${MTEK_BASE}/reservations` +
    `?class_session_min_datetime=${encodeURIComponent(startDateTime)}` +
    `&class_session_max_datetime=${encodeURIComponent(endDateTime)}` +
    `&status=pending`;

  console.log(`Reservations URL: ${reservationsUrl}`);

  const reservationsPayload = await fetchJsonWithRateLimit(
    reservationsUrl,
    { headers: MTEK_HEADERS }
  );
  const reservations = reservationsPayload.data || [];
  console.log(`Found ${reservations.length} reservations in this hour window`);

  if (!reservations.length) {
    console.log('Nothing to do for this window, exiting.');
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
      // Fetch class session + user (user may be null)
      const classSessionPromise = fetchJsonWithRateLimit(
        `${MTEK_BASE}/class_sessions/${classSessionId}`,
        { headers: MTEK_HEADERS }
      );

      let userJson = null;
      if (userId) {
        userJson = await fetchJsonWithRateLimit(
          `${MTEK_BASE}/users/${userId}`,
          { headers: MTEK_HEADERS }
        );
      }

      const classSessionJson = await classSessionPromise;
      const classSessionData = classSessionJson.data;
      const classAttrs = classSessionData?.attributes || {};

      const userData = userJson ? userJson.data : null;
      const userAttrs = userData?.attributes || {};

      // Studio lookup
      const locationId = classSessionData?.relationships?.location?.data?.id;
      let studioName = '';
      let studioEmail = '';

      if (locationId && studiosByLocationId.has(String(locationId))) {
        const studio = studiosByLocationId.get(String(locationId));
        studioName = studio.name;
        studioEmail = studio.email;
      }

      // Build payload fields
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

  console.log(`Done. Successfully sent ${processed} reservations to the webhook for this window.`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
