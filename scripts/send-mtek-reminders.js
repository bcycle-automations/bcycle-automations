// scripts/send-mtek-reminders.js
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import process from 'node:process';

const {
  MTEK_API_TOKEN,
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_STUDIOS_TABLE,
  MAKE_WEBHOOK_URL,
  TIMEZONE: ENV_TIMEZONE,
} = process.env;

if (!MTEK_API_TOKEN) throw new Error('Missing env: MTEK_API_TOKEN');
if (!AIRTABLE_TOKEN) throw new Error('Missing env: AIRTABLE_TOKEN');
if (!AIRTABLE_BASE_ID) throw new Error('Missing env: AIRTABLE_BASE_ID');
if (!AIRTABLE_STUDIOS_TABLE) throw new Error('Missing env: AIRTABLE_STUDIOS_TABLE');
if (!MAKE_WEBHOOK_URL) throw new Error('Missing env: MAKE_WEBHOOK_URL');

const TIMEZONE = ENV_TIMEZONE || 'America/Toronto';

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

// Resolve __dirname for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// State file to remember which UTC "tomorrow date" and which hour we last processed
const STATE_FILE = path.join(__dirname, '..', 'state', 'mtek-reminder-state.json');

/**************************************************
 * Helpers – state + timing
 **************************************************/
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load/save state: which "tomorrow" UTC date & lastProcessedHour we already handled
function loadState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    // If file missing or invalid, default
    return { targetDate: '', lastProcessedHour: -1 };
  }
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
}

// Get UTC date + hour, offset by offsetDays (in UTC)
function getUtcDateHour(offsetDays = 0) {
  const now = new Date();
  now.setUTCDate(now.getUTCDate() + offsetDays);

  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, '0');
  const day = String(now.getUTCDate()).padStart(2, '0');
  const hour = now.getUTCHours(); // 0–23

  return { year, month, day, hour };
}

/**************************************************
 * HTTP helpers
 **************************************************/
// Simple JSON fetch (no rate-limit handling) – for Airtable
async function fetchJsonSimple(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed ${res.status} ${res.statusText} for ${url}: ${text}`);
  }
  return res.json();
}

// Rate-limit-aware JSON fetch – for MTEK API
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
      delayMs = 2000; // fallback 2s
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
  let url =
    `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${table}` +
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
      url =
        `${AIRTABLE_BASE_URL}/${AIRTABLE_BASE_ID}/${table}` +
        `?fields[]=MTEK%20Location%20ID&fields[]=Studio%20name&fields[]=Studio%20email&offset=${data.offset}`;
    } else {
      url = null;
    }
  }

  return studiosByLocationId;
}

/**************************************************
 * Process a single UTC hour window for "tomorrow UTC"
 **************************************************/
async function processHourWindow({ year, month, day, hour, studiosByLocationId }) {
  const hourStr = String(hour).padStart(2, '0');
  const nextHour = (hour + 1) % 24;
  const nextHourStr = String(nextHour).padStart(2, '0');

  // Build full UTC datetimes with 'Z' suffix
  const startDateTime = `${year}-${month}-${day}T${hourStr}:00:00Z`;
  const endDateTime = `${year}-${month}-${day}T${nextHourStr}:00:00Z`;

  console.log(`\n=== Processing UTC window: ${startDateTime} → ${endDateTime} ===`);

  const reservationsUrl =
    `${MTEK_BASE}/reservations` +
    `?class_session_min_datetime=${encodeURIComponent(startDateTime)}` +
    `&class_session_max_datetime=${encodeURIComponent(endDateTime)}` +
    `&status=pending&page_size=1000`;

  console.log(`Reservations URL: ${reservationsUrl}`);

  const reservationsPayload = await fetchJsonWithRateLimit(reservationsUrl, {
    headers: MTEK_HEADERS,
  });
  const reservations = reservationsPayload.data || [];
  console.log(`Found ${reservations.length} reservations in this UTC hour window`);

  if (!reservations.length) {
    console.log('Nothing to do for this window.');
    return 0;
  }

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
      // Fetch class session & user
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

      // Email + names
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

      // Class date & time based on start_date/start_time attributes
      const startDateRaw = classAttrs.start_date || attrs.start_date || null; // e.g. "2025-11-27"
      const startTimeRaw = classAttrs.start_time || attrs.start_time || null; // e.g. "07:00:00"

      let classDateFormatted = '';
      let classTimeFormatted = '';

      if (startDateRaw && /^\d{4}-\d{2}-\d{2}$/.test(startDateRaw)) {
        const [y, m, d] = startDateRaw.split('-');
        classDateFormatted = `${d}-${m}-${y}`; // DD-MM-YYYY
      }

      if (startTimeRaw && /^\d{2}:\d{2}/.test(startTimeRaw)) {
        // take HH:MM from HH:MM:SS
        classTimeFormatted = startTimeRaw.slice(0, 5); // HH:MM
      }

      // Fallback: if those didn't work, try ISO datetime conversion
      if (
        (!classDateFormatted || !classTimeFormatted) &&
        (attrs.class_session_min_datetime || classAttrs.class_session_min_datetime)
      ) {
        const classStartIso =
          attrs.class_session_min_datetime || classAttrs.class_session_min_datetime;

        const dt = new Date(classStartIso);

        const formatter = new Intl.DateTimeFormat('en-CA', {
          timeZone: TIMEZONE,
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false,
        });

        const parts = formatter.formatToParts(dt);
        const get = type => parts.find(p => p.type === type)?.value || '';

        const yearLocal = get('year');
        const monthLocal = get('month');
        const dayLocal = get('day');
        const hourLocal = get('hour');
        const minuteLocal = get('minute');

        if (!classDateFormatted) {
          classDateFormatted = `${dayLocal}-${monthLocal}-${yearLocal}`;
        }
        if (!classTimeFormatted) {
          classTimeFormatted = `${hourLocal}:${minuteLocal}`;
        }
      }

      const emailTo = guestEmail || userEmail || '';

      const payload = {
        email: emailTo,
        first_name: firstName,
        guest_email: guestEmail, // null/empty for non-guests
        class_label: classLabel,
        instructor_names: instructorNames,
        reservation_id: reservation.id,
        class_session_id: classSessionId,
        studio_name: studioName,
        studio_email: studioEmail,
        class_date: classDateFormatted, // DD-MM-YYYY
        class_time: classTimeFormatted, // HH:MM
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

  console.log(
    `Finished UTC window ${startDateTime} → ${endDateTime}. ` +
    `Successfully sent ${processed} reservations to the webhook.`
  );
  return processed;
}

/**************************************************
 * Main with catch-up logic
 **************************************************/
async function main() {
  // "Tomorrow" in UTC and the current UTC hour
  const { year, month, day, hour: currentHour } = getUtcDateHour(1); // +1 day = tomorrow (UTC)
  const targetDate = `${year}-${month}-${day}`;

  let state = loadState();

  if (state.targetDate !== targetDate) {
    console.log(
      `New target UTC date detected (was ${state.targetDate || 'none'}, now ${targetDate}). ` +
      `Resetting lastProcessedHour to -1.`
    );
    state = { targetDate, lastProcessedHour: -1 };
  }

  const startHour = state.lastProcessedHour + 1;
  const endHour = currentHour;

  if (startHour > endHour) {
    console.log(
      `No catch-up needed. Already processed up to hour ${state.lastProcessedHour} ` +
      `for UTC date ${targetDate}.`
    );
    return;
  }

  console.log(
    `Processing UTC hours ${startHour}–${endHour} for target date ${targetDate}.`
  );

  console.log('Loading studios from Airtable…');
  const studiosByLocationId = await loadStudiosMap();
  console.log(`Loaded ${studiosByLocationId.size} studios`);

  let totalProcessed = 0;

  for (let h = startHour; h <= endHour; h++) {
    const processed = await processHourWindow({
      year,
      month,
      day,
      hour: h,
      studiosByLocationId,
    });
    totalProcessed += processed;
  }

  state.lastProcessedHour = endHour;
  saveState(state);

  console.log(
    `Done. For UTC date ${targetDate}, processed hours ${startHour}–${endHour}. ` +
    `Total reservations sent: ${totalProcessed}.`
  );
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
