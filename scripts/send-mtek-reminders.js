// scripts/send-mtek-reminders.js
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import process from 'node:process';

const {
  MTEK_API_TOKEN,
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_STUDIOS_TABLE,
  MAKE_WEBHOOK_URL,
  TIMEZONE,
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

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Where we store per-day/hour progress
const STATE_FILE = path.join(__dirname, '..', 'state', 'mtek-reminder-state.json');

/**************************************************
 * Helpers
 **************************************************/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Return "tomorrow" in UTC (YYYY-MM-DD) and current UTC hour.
 */
function getUtcTomorrowAndCurrentHour() {
  const now = new Date();

  // current UTC hour
  const currentHour = now.getUTCHours();

  // tomorrow in UTC
  now.setUTCDate(now.getUTCDate() + 1);
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, '0');
  const day = String(now.getUTCDate()).padStart(2, '0');

  return {
    targetDate: `${year}-${month}-${day}`,
    currentHour,
  };
}

/**
 * Simple JSON fetch – used for Airtable where rate limits are generous.
 */
async function fetchJsonSimple(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed ${res.status} ${res.statusText} for ${url}: ${text}`);
  }
  return res.json();
}

/**
 * Rate-limit-aware JSON fetch – used for MTEK API.
 */
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
 * State handling
 **************************************************/

async function loadState() {
  try {
    const raw = await fs.readFile(STATE_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return {
      targetDate: parsed.targetDate || '',
      lastProcessedHour:
        Number.isInteger(parsed.lastProcessedHour) ? parsed.lastProcessedHour : -1,
    };
  } catch (err) {
    console.warn(
      `Could not read state file at ${STATE_FILE}. Starting fresh. (${err.message})`
    );
    return {
      targetDate: '',
      lastProcessedHour: -1,
    };
  }
}

async function saveState(state) {
  const dir = path.dirname(STATE_FILE);
  await fs.mkdir(dir, { recursive: true });
  const contents = JSON.stringify(
    {
      targetDate: state.targetDate,
      lastProcessedHour: state.lastProcessedHour,
    },
    null,
    2
  );
  await fs.writeFile(STATE_FILE, contents, 'utf8');
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
 * Per-hour processing
 **************************************************/

/**
 * Process a single UTC hour window (e.g., 2025-12-07 10:00:00Z → 11:00:00Z)
 * for the given targetDate (YYYY-MM-DD).
 */
async function processHourWindow(targetDate, hour, studiosByLocationId) {
  const hourStr = String(hour).padStart(2, '0');
  const nextHour = (hour + 1) % 24;
  const nextHourStr = String(nextHour).padStart(2, '0');

  const startDateTime = `${targetDate}T${hourStr}:00:00Z`;
  const endDateTime = `${targetDate}T${nextHourStr}:00:00Z`;

  console.log(
    `\n=== Processing UTC window ${startDateTime} → ${endDateTime} for ${targetDate} ===`
  );

  const reservationsUrl =
    `${MTEK_BASE}/reservations` +
    `?class_session_min_datetime=${encodeURIComponent(startDateTime)}` +
    `&class_session_max_datetime=${encodeURIComponent(endDateTime)}` +
    `&status=pending&reservation_type=standard&page_size=1000`;

  console.log(`Reservations URL: ${reservationsUrl}`);

  const reservationsPayload = await fetchJsonWithRateLimit(reservationsUrl, {
    headers: MTEK_HEADERS,
  });

  const reservations = reservationsPayload.data || [];
  console.log(`Found ${reservations.length} reservations in this window.`);

  if (!reservations.length) {
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
      console.warn(`Skipping reservation ${reservation.id}: no class_session relationship.`);
      continue;
    }

    try {
      // Fetch class session & user in parallel where possible
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

      // Class date & time from start_date/start_time if present
      const startDateRaw = classAttrs.start_date || attrs.start_date || null; // "YYYY-MM-DD"
      const startTimeRaw = classAttrs.start_time || attrs.start_time || null; // "HH:MM:SS"

      let classDateFormatted = '';
      let classTimeFormatted = '';

      if (startDateRaw && /^\d{4}-\d{2}-\d{2}$/.test(startDateRaw)) {
        const [y, m, d] = startDateRaw.split('-');
        classDateFormatted = `${d}-${m}-${y}`; // DD-MM-YYYY
      }

      if (startTimeRaw && /^\d{2}:\d{2}/.test(startTimeRaw)) {
        classTimeFormatted = startTimeRaw.slice(0, 5); // HH:MM
      }

      // Fallback: derive from class_session_min_datetime in local TIMEZONE
      const classStartIso =
        attrs.class_session_min_datetime || classAttrs.class_session_min_datetime;

      if ((!classDateFormatted || !classTimeFormatted) && classStartIso) {
        const tz = TIMEZONE || 'America/Toronto';
        const dt = new Date(classStartIso);

        const formatter = new Intl.DateTimeFormat('en-CA', {
          timeZone: tz,
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

      if (!emailTo) {
        console.warn(`Skipping reservation ${reservation.id}: no email (user or guest).`);
        continue;
      }

      const payload = {
        email: emailTo,
        first_name: firstName,
        guest_email: guestEmail || null,
        class_label: classLabel,
        instructor_names: instructorNames,
        reservation_id: reservation.id,
        class_session_id: classSessionId,
        studio_name: studioName,
        studio_email: studioEmail,
        class_date: classDateFormatted,
        class_time: classTimeFormatted,
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

  console.log(`Finished window ${startDateTime} → ${endDateTime}. Sent ${processed}.`);
  return processed;
}

/**************************************************
 * Main
 **************************************************/

async function main() {
  const state = await loadState();
  const { targetDate, currentHour } = getUtcTomorrowAndCurrentHour();

  console.log(
    `\n=== MTEK Reminder run ===\n` +
      `Target UTC date (tomorrow): ${targetDate}\n` +
      `Current UTC hour: ${currentHour}\n` +
      `State file: targetDate=${state.targetDate || '(none)'}, ` +
      `lastProcessedHour=${state.lastProcessedHour}`
  );

  if (state.targetDate !== targetDate) {
    console.log(
      `Target date changed (old=${state.targetDate}, new=${targetDate}). ` +
        `Resetting lastProcessedHour to -1.`
    );
    state.targetDate = targetDate;
    state.lastProcessedHour = -1;
  }

  const startHour = state.lastProcessedHour + 1;

  if (startHour > currentHour) {
    console.log(
      `Nothing to process. startHour=${startHour} > currentHour=${currentHour}. Exiting.`
    );
    return;
  }

  console.log(`Will process hours from ${startHour} to ${currentHour} (inclusive).`);

  console.log('Loading studios from Airtable…');
  const studiosByLocationId = await loadStudiosMap();
  console.log(`Loaded ${studiosByLocationId.size} studios from Airtable.`);

  let totalProcessed = 0;

  for (let hour = startHour; hour <= currentHour; hour++) {
    const count = await processHourWindow(targetDate, hour, studiosByLocationId);
    totalProcessed += count;
  }

  state.lastProcessedHour = currentHour;
  await saveState(state);

  console.log(
    `\nDone. For UTC date ${targetDate}, processed hours ${startHour}–${currentHour}. ` +
      `Total reservations sent to webhook: ${totalProcessed}.`
  );
}

main().catch(err => {
  console.error('Fatal error in reminders script:', err);
  process.exit(1);
});
