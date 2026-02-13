// scripts/send-mtek-reminders.js
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import process from "node:process";

const {
  // MTEK
  MTEK_API_TOKEN,

  // Airtable
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_STUDIOS_TABLE,

  // Timezone for class display fallback
  TIMEZONE,

  // Microsoft Graph (Client Credentials)
  M365_TENANT_ID,
  M365_CLIENT_ID,
  M365_CLIENT_SECRET,
  M365_SENDER_UPN, // noreply@bcyclespin.com

  // Optional controls
  FROM_NAME, // "b.cycle" (optional; default below)
  MAX_EMAILS_PER_MINUTE, // optional; default 20
} = process.env;

// ------------------- Required env checks -------------------
if (!MTEK_API_TOKEN) throw new Error("Missing env: MTEK_API_TOKEN");

if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing env: AIRTABLE_BASE_ID");
if (!AIRTABLE_STUDIOS_TABLE) throw new Error("Missing env: AIRTABLE_STUDIOS_TABLE");

if (!M365_TENANT_ID) throw new Error("Missing env: M365_TENANT_ID");
if (!M365_CLIENT_ID) throw new Error("Missing env: M365_CLIENT_ID");
if (!M365_CLIENT_SECRET) throw new Error("Missing env: M365_CLIENT_SECRET");
if (!M365_SENDER_UPN) throw new Error("Missing env: M365_SENDER_UPN");

// ------------------- Constants -------------------
const MTEK_BASE = "https://bcycle.marianatek.com/api";
const AIRTABLE_BASE_URL = "https://api.airtable.com/v0";
const GRAPH_BASE = "https://graph.microsoft.com/v1.0";

const EMAIL_SUBJECT = "Heads up / Rappel";
const EMAIL_LOG_TYPE = "Reservation in 24 hours";

const EMAIL_LOG_BASE_ID = "appofCRTxHoIe6dXI";
const EMAIL_LOG_TABLE_ID = "tbloAdBJHSygcndbA";

const DISPLAY_FROM_NAME = (FROM_NAME || "b.cycle").trim();

// Conservative default rate cap to reduce chance of hitting mailbox/tenant limits
const RATE_CAP_PER_MIN = Number(MAX_EMAILS_PER_MINUTE || 20);

const MTEK_HEADERS = {
  Authorization: `Bearer ${MTEK_API_TOKEN}`,
  Accept: "application/vnd.api+json",
};

const AIRTABLE_HEADERS = {
  Authorization: `Bearer ${AIRTABLE_TOKEN}`,
  Accept: "application/json",
  "Content-Type": "application/json",
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Where we store per-day/hour progress
const STATE_FILE = path.join(__dirname, "..", "state", "mtek-reminder-state.json");

/**************************************************
 * Helpers
 **************************************************/
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Return "tomorrow" in UTC (YYYY-MM-DD) and current UTC hour.
 */
function getUtcTomorrowAndCurrentHour() {
  const now = new Date();
  const currentHour = now.getUTCHours();

  now.setUTCDate(now.getUTCDate() + 1);
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  const day = String(now.getUTCDate()).padStart(2, "0");

  return { targetDate: `${year}-${month}-${day}`, currentHour };
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

    const retryAfterHeader = res.headers.get("Retry-After");
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

    if (!delayMs || delayMs < 0) delayMs = 2000;

    console.warn(
      `Got 429 from MTEK for ${url}. Retry-After=${retryAfterHeader || "n/a"}; ` +
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
    const raw = await fs.readFile(STATE_FILE, "utf8");
    const parsed = JSON.parse(raw);
    return {
      targetDate: parsed.targetDate || "",
      lastProcessedHour: Number.isInteger(parsed.lastProcessedHour) ? parsed.lastProcessedHour : -1,
    };
  } catch (err) {
    console.warn(`Could not read state file at ${STATE_FILE}. Starting fresh. (${err.message})`);
    return { targetDate: "", lastProcessedHour: -1 };
  }
}

async function saveState(state) {
  const dir = path.dirname(STATE_FILE);
  await fs.mkdir(dir, { recursive: true });
  const contents = JSON.stringify(
    { targetDate: state.targetDate, lastProcessedHour: state.lastProcessedHour },
    null,
    2
  );
  await fs.writeFile(STATE_FILE, contents, "utf8");
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
      const mtekId = fields["MTEK Location ID"];
      if (mtekId != null && mtekId !== "") {
        studiosByLocationId.set(String(mtekId), {
          name: fields["Studio name"] || "",
          email: fields["Studio email"] || "",
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
 * Airtable email log
 * - only called after successful send
 **************************************************/
async function createEmailLog(email) {
  const url = `${AIRTABLE_BASE_URL}/${EMAIL_LOG_BASE_ID}/${EMAIL_LOG_TABLE_ID}`;

  const res = await fetch(url, {
    method: "POST",
    headers: AIRTABLE_HEADERS,
    body: JSON.stringify({
      fields: {
        Email: email,
        Type: EMAIL_LOG_TYPE,
      },
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Airtable email log create failed (${res.status}): ${text}`);
  }
}

/**************************************************
 * Microsoft Graph token + send
 **************************************************/
let cachedGraphToken = null; // { accessToken, expiresAtMs }

async function getGraphAccessToken() {
  const now = Date.now();
  if (cachedGraphToken?.accessToken && cachedGraphToken.expiresAtMs - now > 60_000) {
    return cachedGraphToken.accessToken;
  }

  const tokenUrl = `https://login.microsoftonline.com/${encodeURIComponent(
    M365_TENANT_ID
  )}/oauth2/v2.0/token`;

  const body = new URLSearchParams({
    client_id: M365_CLIENT_ID,
    client_secret: M365_CLIENT_SECRET,
    grant_type: "client_credentials",
    scope: "https://graph.microsoft.com/.default",
  });

  const res = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Graph token request failed (${res.status}): ${text}`);
  }

  const json = await res.json();
  const expiresInSec = Number(json.expires_in || 3599);

  cachedGraphToken = {
    accessToken: json.access_token,
    expiresAtMs: Date.now() + expiresInSec * 1000,
  };

  return cachedGraphToken.accessToken;
}

/**
 * Graph-aware send with throttling/backoff.
 * Returns on success; throws on final failure.
 */
async function sendMailGraph({ toEmail, replyToEmail, subject, html }) {
  const maxAttempts = 5;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const token = await getGraphAccessToken();

    const url = `${GRAPH_BASE}/users/${encodeURIComponent(M365_SENDER_UPN)}/sendMail`;

    const message = {
      subject,
      body: { contentType: "HTML", content: html },
      toRecipients: [{ emailAddress: { address: toEmail } }],
      from: { emailAddress: { name: DISPLAY_FROM_NAME, address: M365_SENDER_UPN } },
      sender: { emailAddress: { name: DISPLAY_FROM_NAME, address: M365_SENDER_UPN } },
    };

    if (replyToEmail) {
      message.replyTo = [{ emailAddress: { address: replyToEmail } }];
    }

    const payload = {
      message,
      saveToSentItems: false,
    };

    const res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (res.ok) return;

    const status = res.status;
    const retryAfter = res.headers.get("Retry-After");
    const text = await res.text().catch(() => "");

    const isRetryable = status === 429 || status === 503 || status === 504;

    if (!isRetryable || attempt === maxAttempts) {
      throw new Error(
        `Graph sendMail failed (attempt ${attempt}/${maxAttempts}) status=${status} ${res.statusText}: ${text}`
      );
    }

    let delayMs = 0;

    if (retryAfter) {
      const secs = Number(retryAfter);
      if (!Number.isNaN(secs)) delayMs = secs * 1000;
    }

    if (!delayMs) {
      // Backoff ladder (caps at 5 minutes)
      const ladder = [2000, 10_000, 30_000, 120_000, 300_000];
      delayMs = ladder[Math.min(attempt - 1, ladder.length - 1)];
    }

    console.warn(
      `Graph throttled/transient (status=${status}). Retry-After=${retryAfter || "n/a"}; ` +
        `waiting ${Math.round(delayMs / 1000)}s then retrying (attempt ${attempt}/${maxAttempts})`
    );

    await sleep(delayMs);
  }
}

/**************************************************
 * Optional rate cap (soft limiter)
 **************************************************/
let sentInWindow = 0;
let windowStartMs = Date.now();

async function enforceRateCap() {
  if (!RATE_CAP_PER_MIN || RATE_CAP_PER_MIN <= 0) return;

  const now = Date.now();
  const elapsed = now - windowStartMs;

  if (elapsed >= 60_000) {
    windowStartMs = now;
    sentInWindow = 0;
    return;
  }

  if (sentInWindow >= RATE_CAP_PER_MIN) {
    const waitMs = 60_000 - elapsed;
    console.log(`Rate cap reached (${RATE_CAP_PER_MIN}/min). Sleeping ${Math.round(waitMs / 1000)}s…`);
    await sleep(waitMs);
    windowStartMs = Date.now();
    sentInWindow = 0;
  }
}

/**************************************************
 * Email HTML template (Make version)
 **************************************************/
function escapeHtml(s) {
  if (s == null) return "";
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function buildEmailHtml({
  first_name,
  class_label,
  instructor_names,
  class_date,
  class_time,
  studio_name,
}) {
  return `<p>Hey / Bonjour ${escapeHtml(first_name)}!</p>

<p>You are enrolled in &nbsp;/&nbsp;Vous êtes inscrit au&nbsp;</p>

<p><span style="font-size:18px;"><strong>Class(e) :&nbsp;</strong></span>${escapeHtml(
    class_label
  )}&nbsp;<span style="font-size: 18px;"><strong>&nbsp;-&nbsp;</strong></span> ${escapeHtml(
    instructor_names
  )}</p>

<p><span style="font-size:18px;"><strong>Date :&nbsp;</strong></span> ${escapeHtml(
    class_date
  )}  <strong><span style="font-size:18px;">-</span></strong>${escapeHtml(class_time)}</p>

<p><span style="font-size:18px;"><strong>Studio:&nbsp;</strong></span>${escapeHtml(studio_name)}</p>

<p>&nbsp;</p>

<p><strong><span style="font-family: Lato, sans-serif; font-size: 14px; color: rgb(255, 0, 0);">Nous vous prions de bien vouloir arriver 10-15 minutes avant le début de votre classe&nbsp;afin d&rsquo;éviter les retards.&nbsp;Une fois la porte des classes de cycle et body fermée, aucune entrée tardive ne sera autorisée.</span>&nbsp;/&nbsp;<span style="font-family: Lato, sans-serif; font-size: 14px; color: rgb(255, 0, 0);">Please arrive 10-15 minutes before class&nbsp;as late entries are not permitted once the Cycle / Body door has shut.</span></strong></p>

<p><span style="color:#ff6600;"><strong><span style="font-family: Lato, sans-serif; font-size: 14px;">Si vous n&rsquo;êtes pas enregistré(e) 5 minutes avant le cours, votre place sera attribuée à un client qui attend au studio d&#39;avoir la chance d&rsquo;entrer dans une classe complète. / If you are not checked in 5 minutes before class, your spot will be given away to a client who is waiting at the studio for the chance to get in to a sold out class.</span></strong></span></p>

<p><strong><span style="font-family: Lato, sans-serif; font-size: 14px; color: rgb(255, 0, 0);">Toute annulation doit être effectuée avant 18h00 le jour précédent votre cours. Pour plus d&rsquo;information, svp consultez notre section&nbsp;</span><a href="https://bcyclespin.com/fr/terms-and-conditions/">Termes&nbsp;&amp; Conditions</a>.&nbsp;/&nbsp;<span style="font-family: Lato, sans-serif; font-size: 14px; color: rgb(255, 0, 0);">Cancelations must be done before 6PM the day prior to your class. For more information, please read our</span><a href="https://bcyclespin.com/en/terms-and-conditions/"><span style="color: rgb(255, 0, 0);">&nbsp;</span>Terms &amp; Conditions.</a>&nbsp;</strong></p>

<p>&nbsp;</p>

<hr />
<p>&nbsp;</p>

<h3 class="null" style="font-family: Arial, Verdana, sans-serif; line-height: 20.8px;"><strong>Première visite / First time?</strong></h3>

<p><a href="https://bcyclespin.com/fr/about/#first-time">Cliquez ici </a>/ <a href="https://bcyclespin.com/en/about/#first-time">Click here</a></p>

<h3><strong>Vous devez annuler / Can&rsquo;t make it?</strong></h3>

<p><a href="https://bcyclespin.com/fr/my-account/?_mt=/account/reservations">Cliquez ici </a>/ <a href="https://bcyclespin.com/en/my-account/?_mt=/account/reservations">Click here</a></p>

<p>L&rsquo;équipe b.</p>`;
}

/**************************************************
 * Per-hour processing
 **************************************************/
async function processHourWindow(targetDate, hour, studiosByLocationId) {
  const hourStr = String(hour).padStart(2, "0");
  const nextHour = (hour + 1) % 24;
  const nextHourStr = String(nextHour).padStart(2, "0");

  const startDateTime = `${targetDate}T${hourStr}:00:00Z`;
  const endDateTime = `${targetDate}T${nextHourStr}:00:00Z`;

  console.log(`\n=== Processing UTC window ${startDateTime} → ${endDateTime} for ${targetDate} ===`);

  const reservationsUrl =
    `${MTEK_BASE}/reservations` +
    `?class_session_min_datetime=${encodeURIComponent(startDateTime)}` +
    `&class_session_max_datetime=${encodeURIComponent(endDateTime)}` +
    `&status=pending&reservation_type=standard&page_size=1000`;

  console.log(`Reservations URL: ${reservationsUrl}`);

  const reservationsPayload = await fetchJsonWithRateLimit(reservationsUrl, { headers: MTEK_HEADERS });
  const reservations = reservationsPayload.data || [];

  console.log(`Found ${reservations.length} reservations in this window.`);
  if (!reservations.length) return 0;

  let processed = 0;

  for (const reservation of reservations) {
    const attrs = reservation.attributes || {};
    const rels = reservation.relationships || {};

    const guestEmail = attrs.guest_email || null;
    const userRel = rels.user?.data;
    const classSessionRel = rels.class_session?.data;

    const userId = userRel?.id || null;
    const classSessionId = classSessionRel?.id || null;

    if (!classSessionId) {
      console.warn(`Skipping reservation ${reservation.id}: no class_session relationship.`);
      continue;
    }

    try {
      // Fetch class session & user
      const classSessionPromise = fetchJsonWithRateLimit(`${MTEK_BASE}/class_sessions/${classSessionId}`, {
        headers: MTEK_HEADERS,
      });

      let userJson = null;
      if (userId) {
        userJson = await fetchJsonWithRateLimit(`${MTEK_BASE}/users/${userId}`, { headers: MTEK_HEADERS });
      }

      const classSessionJson = await classSessionPromise;
      const classSessionData = classSessionJson.data;
      const classAttrs = classSessionData?.attributes || {};

      const userData = userJson ? userJson.data : null;
      const userAttrs = userData?.attributes || {};

      // Studio lookup
      const locationId = classSessionData?.relationships?.location?.data?.id || null;
      let studioName = "";
      let studioEmail = "";

      if (locationId && studiosByLocationId.has(String(locationId))) {
        const studio = studiosByLocationId.get(String(locationId));
        studioName = studio.name;
        studioEmail = studio.email;
      }

      // Email + names
      const userEmail = userAttrs.email || "";
      const firstName = userAttrs.first_name || "";

      const publicNote = (classAttrs.public_note || "").trim();
      const classTypeDisplay = classAttrs.class_type_display || "";
      const classLabel = publicNote === "" ? classTypeDisplay : publicNote;

      let instructorNames = "";
      if (Array.isArray(classAttrs.instructor_names)) {
        instructorNames = classAttrs.instructor_names.join(", ");
      } else if (typeof classAttrs.instructor_names === "string") {
        instructorNames = classAttrs.instructor_names;
      }

      // Class date & time from start_date/start_time if present
      const startDateRaw = classAttrs.start_date || attrs.start_date || null; // "YYYY-MM-DD"
      const startTimeRaw = classAttrs.start_time || attrs.start_time || null; // "HH:MM:SS"

      let classDateFormatted = "";
      let classTimeFormatted = "";

      if (startDateRaw && /^\d{4}-\d{2}-\d{2}$/.test(startDateRaw)) {
        const [y, m, d] = startDateRaw.split("-");
        classDateFormatted = `${d}-${m}-${y}`; // DD-MM-YYYY
      }

      if (startTimeRaw && /^\d{2}:\d{2}/.test(startTimeRaw)) {
        classTimeFormatted = startTimeRaw.slice(0, 5); // HH:MM
      }

      // Fallback: derive from class_session_min_datetime in local TIMEZONE
      const classStartIso = attrs.class_session_min_datetime || classAttrs.class_session_min_datetime;

      if ((!classDateFormatted || !classTimeFormatted) && classStartIso) {
        const tz = TIMEZONE || "America/Toronto";
        const dt = new Date(classStartIso);

        const formatter = new Intl.DateTimeFormat("en-CA", {
          timeZone: tz,
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
        });

        const parts = formatter.formatToParts(dt);
        const get = (type) => parts.find((p) => p.type === type)?.value || "";

        const yearLocal = get("year");
        const monthLocal = get("month");
        const dayLocal = get("day");
        const hourLocal = get("hour");
        const minuteLocal = get("minute");

        if (!classDateFormatted) classDateFormatted = `${dayLocal}-${monthLocal}-${yearLocal}`;
        if (!classTimeFormatted) classTimeFormatted = `${hourLocal}:${minuteLocal}`;
      }

      const emailTo = guestEmail || userEmail || "";
      if (!emailTo) {
        console.warn(`Skipping reservation ${reservation.id}: no email (user or guest).`);
        continue;
      }

      const replyTo = studioEmail && studioEmail.includes("@") ? studioEmail : null;

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

      const html = buildEmailHtml(payload);

      // Rate cap + send + log (log only on success)
      await enforceRateCap();
      await sendMailGraph({
        toEmail: payload.email,
        replyToEmail: replyTo,
        subject: EMAIL_SUBJECT,
        html,
      });
      sentInWindow++;

      await createEmailLog(payload.email);

      processed++;
      console.log(`Sent + logged: reservation=${reservation.id} to=${payload.email}`);
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
      `State file: targetDate=${state.targetDate || "(none)"}, ` +
      `lastProcessedHour=${state.lastProcessedHour}\n` +
      `Graph sender: ${M365_SENDER_UPN} (From name="${DISPLAY_FROM_NAME}")\n` +
      `Rate cap: ${RATE_CAP_PER_MIN}/min\n` +
      `Email log: base=${EMAIL_LOG_BASE_ID} table=${EMAIL_LOG_TABLE_ID} type="${EMAIL_LOG_TYPE}"`
  );

  if (state.targetDate !== targetDate) {
    console.log(
      `Target date changed (old=${state.targetDate}, new=${targetDate}). Resetting lastProcessedHour to -1.`
    );
    state.targetDate = targetDate;
    state.lastProcessedHour = -1;
  }

  const startHour = state.lastProcessedHour + 1;

  if (startHour > currentHour) {
    console.log(`Nothing to process. startHour=${startHour} > currentHour=${currentHour}. Exiting.`);
    return;
  }

  console.log(`Will process hours from ${startHour} to ${currentHour} (inclusive).`);

  console.log("Loading studios from Airtable…");
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
      `Total emails sent: ${totalProcessed}.`
  );
}

main().catch((err) => {
  console.error("Fatal error in reminders script:", err);
  process.exit(1);
});
