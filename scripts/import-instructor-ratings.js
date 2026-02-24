/* eslint-disable no-console */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";

/* ============================================================
   ENV / CONFIG
============================================================ */

const {
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  FORM_TABLE_ID,
  FEEDBACKS_TABLE_ID,
  LOGS_TABLE_ID,
  FORM_RECORD_ID,
} = process.env;

function requireEnv(name) {
  if (!process.env[name]) throw new Error(`Missing env var: ${name}`);
}

requireEnv("AIRTABLE_TOKEN");
requireEnv("AIRTABLE_BASE_ID");
requireEnv("FORM_TABLE_ID");
requireEnv("FEEDBACKS_TABLE_ID");
requireEnv("LOGS_TABLE_ID");
requireEnv("FORM_RECORD_ID");

/* ============================================================
   CONSTANTS
============================================================ */

const AIRTABLE_API = "https://api.airtable.com/v0";

/**
 * FORM TABLE FIELD IDS (from your screenshot)
 * These live on the Form table record and can be prefilled by the form URL.
 * People might edit them; we always read whatever is in the submitted record.
 */
const FORM_FIELD_IDS = {
  CSV_UPLOAD: "fld9aPVBfGiKnxqNu",        // "CSV Upload" (Attachment)
  STUDIO: "fld37o0IEnMH4Qz1z",            // "Studio" (Link to record)

  // Mapping fields (Single line text) containing the CSV header to use:
  CONTACT_HEADER: "fldcLopFy64blIgFl",    // "Contact"
  RATING_HEADER: "fld1CnnyKmmAQuMg1",     // "Rating"
  COMMENT_HEADER: "fldXYsNP1QjLNPVBy",    // "COMMENT"
  CLASSTYPE_HEADER: "fldZ1CSNQY6naOeAJ",  // "CLASSTYPE"
  DATE_HEADER: "fldBoXrIXNCD8ZTPR",       // "DATE OF RATING"
  INSTRUCTOR_HEADER: "fldkeOL4mnfCKq0up", // "Instructor Name"
};

/**
 * FEEDBACKS TABLE FIELD NAMES (destination)
 * (These are Airtable field *names* on the Feedbacks table)
 */
const FEEDBACK_FIELDS = {
  CONTACT: "Contact",                // TEXT
  STUDIO: "Studio",                  // LINKED RECORD
  DATE: "DATE OF RATING",            // DATE (or date-like string if your field is text)
  RATING: "Rating",                  // NUMBER (or text if your field is text)
  COMMENT: "COMMENT",                // LONG TEXT / TEXT
  CLASSTYPE: "CLASSTYPE",            // TEXT / SINGLE SELECT
  INSTRUCTOR_NAME: "Instructor Name",// TEXT
  TYPE: "Type",                      // SINGLE SELECT
};

const TYPE_VALUE = "Instructor Feedback";

/* ---- Logs table ---- */
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

const LOG_TYPE_VALUE = "Instructor Ratings Import";

/* ============================================================
   HELPERS
============================================================ */

async function fetchJson(url, options = {}) {
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

function escapeFormula(v) {
  return String(v ?? "").replace(/"/g, '\\"');
}

/* ============================================================
   CSV PARSER (ORDER-INDEPENDENT)
============================================================ */

function normalizeHeader(h) {
  return String(h || "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[_-]/g, "");
}

function parseCSV(text) {
  const rows = [];
  let row = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    const n = text[i + 1];

    if (inQuotes) {
      if (c === '"' && n === '"') {
        cur += '"';
        i++;
      } else if (c === '"') {
        inQuotes = false;
      } else {
        cur += c;
      }
    } else {
      if (c === '"') inQuotes = true;
      else if (c === ",") {
        row.push(cur);
        cur = "";
      } else if (c === "\n") {
        row.push(cur);
        rows.push(row);
        row = [];
        cur = "";
      } else if (c !== "\r") {
        cur += c;
      }
    }
  }

  row.push(cur);
  rows.push(row);

  return rows.filter((r) => r.some((c) => String(c).trim() !== ""));
}

function makeHeaderIndex(headers) {
  const map = new Map();
  headers.forEach((h, i) => map.set(normalizeHeader(h), i));
  return map;
}

function getCell(row, headerIndex, ...headerCandidates) {
  for (const h of headerCandidates) {
    const key = normalizeHeader(h);
    const idx = headerIndex.get(key);
    if (idx !== undefined && idx !== -1) return row[idx];
  }
  return "";
}

/* ============================================================
   DATE NORMALIZATION (DATE-ONLY)
============================================================ */

function parseDateOnlyToISO(v) {
  if (!v) return null;

  const raw = String(v).trim();
  if (!raw) return null;

  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return null;

  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString();
}

/* ============================================================
   DEDUPE CHECK
   Key: Contact(text) + Studio(link) + DATE OF RATING (date-only)
============================================================ */

async function feedbackExists({ contact, studioId, dateISO }) {
  const formula = `AND(
    {${FEEDBACK_FIELDS.CONTACT}} = "${escapeFormula(contact)}",
    FIND("${studioId}", ARRAYJOIN({${FEEDBACK_FIELDS.STUDIO}})) > 0,
    IS_SAME({${FEEDBACK_FIELDS.DATE}}, DATETIME_PARSE("${dateISO}"), 'day')
  )`;

  const url =
    `${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FEEDBACKS_TABLE_ID}` +
    `?pageSize=1&filterByFormula=${encodeURIComponent(formula)}`;

  const out = await fetchJson(url);
  return (out.records?.length || 0) > 0;
}

/* ============================================================
   LOGGING
============================================================ */

async function createLog() {
  const out = await fetchJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${LOGS_TABLE_ID}`, {
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
  await fetchJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${LOGS_TABLE_ID}/${logId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields }),
  });
}

/* ============================================================
   FORM LOAD + CSV DOWNLOAD
============================================================ */

/**
 * IMPORTANT: returnFieldsByFieldId=true
 * This ensures form.fields is keyed by field IDs, so renaming fields won’t break the script.
 */
async function loadFormRecord() {
  const url =
    `${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FORM_TABLE_ID}/${FORM_RECORD_ID}` +
    `?returnFieldsByFieldId=true`;
  return fetchJson(url);
}

async function downloadCsvToTemp(csvUrl) {
  const csvPath = path.join(os.tmpdir(), `ratings_${Date.now()}.csv`);
  const res = await fetch(csvUrl);
  if (!res.ok) throw new Error(`CSV download failed ${res.status} ${res.statusText}`);
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(csvPath, buf);
  return csvPath;
}

function getFormTextField(form, fieldId, fallback) {
  const v = form?.fields?.[fieldId];
  if (v === undefined || v === null) return fallback;
  const s = String(v).trim();
  return s || fallback;
}

/* ============================================================
   MAIN
============================================================ */

async function main() {
  let logId = null;
  let imported = 0;
  let ignored = 0;
  const issues = [];

  try {
    logId = await createLog();

    const form = await loadFormRecord();

    // Required from the form submission (by field ID)
    const studioId = form?.fields?.[FORM_FIELD_IDS.STUDIO]?.[0];
    const csvUrl = form?.fields?.[FORM_FIELD_IDS.CSV_UPLOAD]?.[0]?.url;

    if (!studioId) throw new Error(`Missing Studio (fieldId=${FORM_FIELD_IDS.STUDIO}) on form record`);
    if (!csvUrl) throw new Error(`Missing CSV Upload (fieldId=${FORM_FIELD_IDS.CSV_UPLOAD}) attachment on form record`);

    // CSV header mappings come from editable/prefilled form text fields (by field ID)
    // If someone changes them, we automatically use the new values.
    const CSV_HEADERS = {
      CONTACT: getFormTextField(form, FORM_FIELD_IDS.CONTACT_HEADER, "Contact"),
      DATE: getFormTextField(form, FORM_FIELD_IDS.DATE_HEADER, "Response Date"),
      RATING: getFormTextField(form, FORM_FIELD_IDS.RATING_HEADER, "Rating"),
      COMMENT: getFormTextField(form, FORM_FIELD_IDS.COMMENT_HEADER, "Comment"),
      CLASSTYPE: getFormTextField(form, FORM_FIELD_IDS.CLASSTYPE_HEADER, "Class"),
      INSTRUCTOR: getFormTextField(form, FORM_FIELD_IDS.INSTRUCTOR_HEADER, "Instructor"),
    };

    const csvPath = await downloadCsvToTemp(csvUrl);
    const csvText = fs.readFileSync(csvPath, "utf8");

    const rows = parseCSV(csvText);
    if (rows.length < 2) throw new Error("CSV has no data rows");

    const headerIndex = makeHeaderIndex(rows[0]);

    console.log("CSV headers:", rows[0]);
    console.log("Using header mapping from form record:", CSV_HEADERS);

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const line = i + 1;

      const contact = String(getCell(row, headerIndex, CSV_HEADERS.CONTACT)).trim();

      const dateRaw = getCell(row, headerIndex, CSV_HEADERS.DATE);
      const dateISO = parseDateOnlyToISO(dateRaw);

      const ratingRaw = String(getCell(row, headerIndex, CSV_HEADERS.RATING)).trim();
      const comment = String(getCell(row, headerIndex, CSV_HEADERS.COMMENT)).trim();
      const classType = String(getCell(row, headerIndex, CSV_HEADERS.CLASSTYPE)).trim();
      const instructor = String(getCell(row, headerIndex, CSV_HEADERS.INSTRUCTOR)).trim();

      if (!contact || !dateISO) {
        ignored++;
        issues.push(`Line ${line}: Missing contact or date (contact="${contact}", date="${dateRaw}")`);
        continue;
      }

      // Dedup (Contact + Studio + same day)
      if (await feedbackExists({ contact, studioId, dateISO })) {
        ignored++;
        continue;
      }

      // Build Airtable fields (NO customer/email resolution anymore)
      const fields = {
        [FEEDBACK_FIELDS.CONTACT]: contact,
        [FEEDBACK_FIELDS.STUDIO]: [studioId],
        [FEEDBACK_FIELDS.DATE]: dateISO,
        [FEEDBACK_FIELDS.TYPE]: TYPE_VALUE,
      };

      if (instructor) fields[FEEDBACK_FIELDS.INSTRUCTOR_NAME] = instructor;
      if (ratingRaw && !Number.isNaN(Number(ratingRaw))) fields[FEEDBACK_FIELDS.RATING] = Number(ratingRaw);
      if (comment) fields[FEEDBACK_FIELDS.COMMENT] = comment;
      if (classType) fields[FEEDBACK_FIELDS.CLASSTYPE] = classType;

      await fetchJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FEEDBACKS_TABLE_ID}`, {
        method: "POST",
        body: JSON.stringify({ records: [{ fields }] }),
      });

      imported++;
    }

    await updateLog(logId, {
      [LOG_FIELDS.STATUS]: issues.length ? LOG_STATUS.ISSUE : LOG_STATUS.COMPLETED,
      [LOG_FIELDS.IMPORTED]: imported,
      [LOG_FIELDS.IGNORED]: ignored,
      ...(issues.length ? { [LOG_FIELDS.ISSUE_LOG]: issues.join("\n") } : {}),
    });

    console.log(`✅ Import completed. Imported=${imported}, Ignored=${ignored}, Issues=${issues.length}`);
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
