/* eslint-disable no-console */
import process from "node:process";

/* ============================================================
   ENV / CONFIG
============================================================ */

const {
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  FEEDBACKS_TABLE_ID,
  LOGS_TABLE_ID,
  MTEK_API_TOKEN,
} = process.env;

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");

function requireEnv(name) {
  if (!process.env[name]) throw new Error(`Missing env var: ${name}`);
}

requireEnv("AIRTABLE_TOKEN");
requireEnv("AIRTABLE_BASE_ID");
requireEnv("FEEDBACKS_TABLE_ID");
requireEnv("LOGS_TABLE_ID");

/* ============================================================
   CONSTANTS
============================================================ */

const AIRTABLE_API = "https://api.airtable.com/v0";

// Process ONLY this view:
const TARGET_VIEW_NAME = "FIND EMAIL - DO NOT TOUCH";

/**
 * FEEDBACKS TABLE FIELDS (names)
 * - CONTACT is what you already have (email or name)
 * - EMAIL is the destination email-type field you want to fill
 */
const FEEDBACK_FIELDS = {
  CONTACT: "Contact",
  EMAIL: "Email", // <-- CHANGE THIS to your exact email field name in the Feedbacks table
};

/* ---- Logs table fields (names) ---- */
const LOG_FIELDS = {
  STATUS: "Status",
  TYPE: "Type",
  IMPORTED: "Ratings Imported",
  IGNORED: "Ratings Ignored",
  ISSUE_LOG: "Issue log",
};

const LOG_STATUS = {
  STARTED: "Started",
  COMPLETED: "Completed",
  ISSUE: "ISSUE",
};

const LOG_TYPE_VALUE = "Find Email Instructor Rating";

/* ============================================================
   HELPERS
============================================================ */

async function fetchAirtableJson(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${t}`);
  }
  return res.json();
}

async function fetchMtekJson(url) {
  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: "application/vnd.api+json",
    },
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`MTEK ${res.status} ${res.statusText}: ${t}`);
  }
  return res.json();
}

function looksLikeEmail(v) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(v ?? "").trim());
}

/* ============================================================
   MTEK – NAME → EMAIL (same logic as before)
============================================================ */

async function resolveEmailViaMtek(name) {
  const q = String(name || "").trim();
  if (!q) return null;
  if (!MTEK_API_TOKEN) return null;

  try {
    const url = new URL("/api/users", MTEK_BASE_URL);
    url.searchParams.set("name_query", q);
    url.searchParams.set("page_size", "5");

    const json = await fetchMtekJson(url.toString());
    const users = Array.isArray(json.data) ? json.data : [];

    for (const u of users) {
      const email = u?.attributes?.email;
      if (looksLikeEmail(email)) return String(email).trim();
    }
    return null;
  } catch (err) {
    console.warn(`⚠️ MTEK lookup failed for "${q}": ${err?.message || err}`);
    return null;
  }
}

/* ============================================================
   READ VIEW RECORDS (pagination)
============================================================ */

async function listFeedbacksFromView(viewName) {
  const records = [];
  let offset = null;

  do {
    const url = new URL(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FEEDBACKS_TABLE_ID}`);
    url.searchParams.set("view", viewName);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);

    const out = await fetchAirtableJson(url.toString());
    records.push(...(out.records || []));
    offset = out.offset || null;
  } while (offset);

  return records;
}

/* ============================================================
   LOGGING
============================================================ */

async function createLog() {
  const out = await fetchAirtableJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${LOGS_TABLE_ID}`, {
    method: "POST",
    body: JSON.stringify({
      records: [
        {
          fields: {
            [LOG_FIELDS.STATUS]: LOG_STATUS.STARTED,
            [LOG_FIELDS.TYPE]: LOG_TYPE_VALUE,
          },
        },
      ],
    }),
  });

  const id = out?.records?.[0]?.id;
  if (!id) throw new Error("Failed to create log record");
  return id;
}

async function updateLog(logId, fields) {
  await fetchAirtableJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${LOGS_TABLE_ID}/${logId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields }),
  });
}

/* ============================================================
   UPDATE FEEDBACK RECORD (set Email field)
============================================================ */

async function setEmailOnFeedbackRecord(feedbackRecordId, email) {
  await fetchAirtableJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FEEDBACKS_TABLE_ID}/${feedbackRecordId}`, {
    method: "PATCH",
    body: JSON.stringify({
      fields: {
        [FEEDBACK_FIELDS.EMAIL]: email,
      },
    }),
  });
}

/* ============================================================
   MAIN
============================================================ */

async function main() {
  let logId = null;

  let scanned = 0;
  let updated = 0;    // email set
  let skipped = 0;    // already has email or missing contact
  let notFound = 0;   // could not resolve email (NOT an ISSUE)
  const notes = [];

  try {
    logId = await createLog();

    const records = await listFeedbacksFromView(TARGET_VIEW_NAME);
    scanned = records.length;

    console.log(`Found ${records.length} record(s) in view "${TARGET_VIEW_NAME}"`);

    for (const r of records) {
      const id = r.id;
      const fields = r.fields || {};

      const existingEmail = String(fields[FEEDBACK_FIELDS.EMAIL] || "").trim();
      if (looksLikeEmail(existingEmail)) {
        skipped++;
        continue;
      }

      const contact = String(fields[FEEDBACK_FIELDS.CONTACT] || "").trim();
      if (!contact) {
        skipped++;
        notes.push(`Record ${id}: missing Contact value`);
        continue;
      }

      const email = looksLikeEmail(contact) ? contact : await resolveEmailViaMtek(contact);
      if (!email) {
        notFound++;
        notes.push(`Record ${id}: could not resolve email from Contact "${contact}"`);
        continue;
      }

      await setEmailOnFeedbackRecord(id, email);
      updated++;
    }

    // Completed even if some emails weren’t found
    await updateLog(logId, {
      [LOG_FIELDS.STATUS]: LOG_STATUS.COMPLETED,
      [LOG_FIELDS.IMPORTED]: updated,              // reuse as "updated"
      [LOG_FIELDS.IGNORED]: skipped + notFound,    // reuse as "skipped + notFound"
      ...(notes.length ? { [LOG_FIELDS.ISSUE_LOG]: notes.join("\n") } : {}),
    });

    console.log(`✅ Done. Scanned=${scanned}, Updated=${updated}, Skipped=${skipped}, NotFound=${notFound}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("❌ Fatal:", msg);

    if (logId) {
      try {
        await updateLog(logId, {
          [LOG_FIELDS.STATUS]: LOG_STATUS.ISSUE,
          [LOG_FIELDS.ISSUE_LOG]: msg,
        });
      } catch (e) {
        console.error("Also failed to update log record:", e?.message || e);
      }
    }

    process.exit(1);
  }
}

main();
