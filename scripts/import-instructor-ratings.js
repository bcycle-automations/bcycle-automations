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
 * FORM TABLE FIELD IDS (by field ID, stable)
 */
const FORM_FIELD_IDS = {
  CSV_UPLOAD: "fld9aPVBfGiKnxqNu",        // "CSV Upload" (Attachment)
  STUDIO: "fld37o0IErMH4Qz1z",            // "Studio" (Link to record)

  // Mapping fields (Single line text) containing the CSV header to use:
  CONTACT_HEADER: "fldcLopFy64blIgFl",    // "Contact"
  RATING_HEADER: "fld1CnnyKmmAQuMg1",     // "Rating"
  COMMENT_HEADER: "fldXYsNP1QjLNPVBy",    // "COMMENT"
  CLASSTYPE_HEADER: "fldZ1CSNQY6naOeAJ",  // "CLASSTYPE"
  DATE_HEADER: "fldBoXrlXNCD8ZTPR",       // "DATE OF RATING"
  INSTRUCTOR_HEADER: "fldkeOL4mnfCKq0up", // "Instructor Name"
};

/**
 * FEEDBACKS TABLE FIELD NAMES (destination)
 */
const FEEDBACK_FIELDS = {
  CONTACT: "Contact",                 // TEXT
  STUDIO: "Studio",                   // LINKED RECORD
  DATE: "DATE OF RATING",             // DATE
  RATING: "Rating",                  // NUMBER
  COMMENT: "COMMENT",                 // TEXT / LONG TEXT
  CLASSTYPE: "CLASSTYPE",             // TEXT / SINGLE SELECT
  INSTRUCTOR_NAME: "Instructor Name", // TEXT
  TYPE: "Type",                       // SINGLE SELECT
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
   GENERAL HELPERS
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Linked-record field shapes
 */
function asFirstLinkedRecordId(v) {
  // Common: ["recXXXX"]
  if (Array.isArray(v) && typeof v[0] === "string" && v[0]) return v[0];

  // Sometimes: [{ id: "recXXXX" }, ...]
  if (Array.isArray(v) && v[0] && typeof v[0].id === "string") return v[0].id;

  // Rare: single string
  if (typeof v === "string" && v) return v;

  return null;
}

/**
 * Attachment fields: [{url, ...}]
 */
function asFirstAttachmentUrl(v) {
  if (Array.isArray(v) && v[0] && typeof v[0].url === "string") {
    return v[0].url;
  }
  return null;
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
      if (c === '"') {
        inQuotes = true;
      } else if (c === ",") {
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

  // Drop completely empty rows
  return rows.filter((r) => r.some((c) => String(c).trim() !== ""));
}

function makeHeaderIndex(headers) {
  const map = new Map();
  headers.forEach((h, i) => map.set(normalizeHeader(h), i));
  return map;
}

function hasHeader(headerIndex, headerName) {
  if (!headerName) return false;
  return headerIndex.has(normalizeHeader(headerName));
}

function getCell(row, headerIndex, headerName) {
  const idx = headerIndex.get(normalizeHeader(headerName));
  if (idx === undefined || idx === -1) return "";
  return row[idx];
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

  // Force to midnight UTC
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString();
}

function dateKeyFromISO(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/* ============================================================
   DEDUPE: STUDIO-AWARE WITH ANON SPECIAL CASE
   - We only dedupe against feedbacks for the same Studio.
   - Named contact: Contact + date (day-only) + class type
   - Anonymous User: date + instructor + rating + class type
============================================================ */

const ANON_CONTACT = "anonymous user";

function isAnonymousContact(contact) {
  return String(contact || "").trim().toLowerCase() === ANON_CONTACT;
}

function makeDedupeKey({ contact, dateKey, ratingKey, instructor, classTypeKey }) {
  if (!contact || !dateKey) return null;

  if (isAnonymousContact(contact)) {
    const instr = String(instructor || "").trim().toLowerCase();
    const ratingPart = ratingKey ?? "";
    const classPart = String(classTypeKey || "").trim().toLowerCase();
    return `anon|${dateKey}|${instr}|${ratingPart}|${classPart}`;
  }

  const normalizedContact = String(contact).trim().toLowerCase();
  const classPart = String(classTypeKey || "").trim().toLowerCase();
  return `named|${normalizedContact}|${dateKey}|${classPart}`;
}

/**
 * Load all existing feedbacks and build a dedupe set for this Studio.
 * We fetch all records and then filter by Studio ID in JS.
 */
async function loadExistingDedupeKeysForStudio(studioId) {
  const keys = new Set();
  let offset;

  do {
    const url =
      `${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FEEDBACKS_TABLE_ID}?pageSize=100` +
      (offset ? `&offset=${offset}` : "");

    const out = await fetchJson(url);
    const records = out.records || [];

    for (const rec of records) {
      const fields = rec.fields || {};
      const studioField = fields[FEEDBACK_FIELDS.STUDIO];
      const recStudioId = asFirstLinkedRecordId(studioField);

      if (recStudioId !== studioId) continue; // only this Studio

      const contact = String(fields[FEEDBACK_FIELDS.CONTACT] || "").trim();
      const dateISO = fields[FEEDBACK_FIELDS.DATE];
      if (!contact || !dateISO) continue;

      const dateKey = dateKeyFromISO(dateISO);
      if (!dateKey) continue;

      const ratingVal = fields[FEEDBACK_FIELDS.RATING];
      const ratingKey =
        ratingVal === undefined || ratingVal === null || ratingVal === ""
          ? ""
          : String(ratingVal).trim();

      const instructor = fields[FEEDBACK_FIELDS.INSTRUCTOR_NAME] || "";
      const classTypeVal = fields[FEEDBACK_FIELDS.CLASSTYPE] || "";
      const classTypeKey = String(classTypeVal || "").trim().toLowerCase();

      const key = makeDedupeKey({ contact, dateKey, ratingKey, instructor, classTypeKey });
      if (key) keys.add(key);
    }

    offset = out.offset;
  } while (offset);

  return keys;
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

   HOW WE GET STUDIO ID:
   - Table:  FORM_TABLE_ID  (env) — the "form" table in your base
   - Record: FORM_RECORD_ID (env) — one specific row (the form submission/config row)
   - We GET that single record with ?returnFieldsByFieldId=true so fields come back by field ID.
   - Field:  FORM_FIELD_IDS.STUDIO ("fld37o0IErMH4Qz1z") — the "Studio" linked-record field on that row
   - Value: Airtable returns the link as an array of record IDs, e.g. ["rec7AQvxpcu0h41Wy"]
   - We pass that raw value to asFirstLinkedRecordId() to get the first ID string → studioId
   - So: studioId is the Airtable record ID of the linked Studio row chosen on that form record.
============================================================ */

async function loadFormRecord() {
  const url =
    `${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FORM_TABLE_ID}/${FORM_RECORD_ID}` +
    `?returnFieldsByFieldId=true`;
  return fetchJson(url);
}

/**
 * Robust loader to allow for UI/API lag (esp. attachments).
 */
async function loadFormRecordWithRetry({ attempts = 8, delayMs = 3000, logFn = console.log } = {}) {
  let last = null;

  for (let i = 1; i <= attempts; i++) {
    last = await loadFormRecord();

    const studioRaw = last?.fields?.[FORM_FIELD_IDS.STUDIO];
    const csvRaw = last?.fields?.[FORM_FIELD_IDS.CSV_UPLOAD];

    const studioId = asFirstLinkedRecordId(studioRaw);
    const csvUrl = asFirstAttachmentUrl(csvRaw);

    if (studioId && csvUrl) return last;

    logFn(
      `⏳ Waiting for Studio/CSV in API... attempt ${i}/${attempts} ` +
        `(studio=${studioId ? "OK" : "missing"}, csv=${csvUrl ? "OK" : "missing"})`,
    );

    if (i < attempts) await sleep(delayMs);
  }

  return last;
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
   BATCH WRITE HELPERS
============================================================ */

async function flushPendingFeedbackBatch(pendingRecords, importedRef) {
  if (!pendingRecords.length) return;

  await fetchJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FEEDBACKS_TABLE_ID}`, {
    method: "POST",
    body: JSON.stringify({ records: pendingRecords }),
  });

  importedRef.count += pendingRecords.length;
  pendingRecords.length = 0;
}

/* ============================================================
   HEADER VALIDATION
============================================================ */

function validateMappedHeaders(headerIndex, mapping) {
  const missing = [];
  for (const [key, headerName] of Object.entries(mapping)) {
    if (!headerName || !String(headerName).trim()) {
      missing.push(`${key}: (blank mapping)`);
      continue;
    }
    if (!hasHeader(headerIndex, headerName)) {
      missing.push(`${key}: "${headerName}"`);
    }
  }
  return missing;
}

/* ============================================================
   MAIN
============================================================ */

async function main() {
  let logId = null;
  const importedRef = { count: 0 }; // mutated by batch helper
  let ignored = 0;
  const issues = [];
  /** @type {string[]} - Ignored rows with reasons, for GitHub Actions run log */
  const ignoredReasons = [];

  try {
    logId = await createLog();

    const form = await loadFormRecordWithRetry({
      attempts: 8,
      delayMs: 3000,
      logFn: (m) => console.log(m),
    });

    const studioRaw = form?.fields?.[FORM_FIELD_IDS.STUDIO];
    const csvRaw = form?.fields?.[FORM_FIELD_IDS.CSV_UPLOAD];

    const studioId = asFirstLinkedRecordId(studioRaw);
    const csvUrl = asFirstAttachmentUrl(csvRaw);

    const presentFieldIds = Object.keys(form?.fields || {});
    const debugDump =
      `FORM_RECORD_ID=${FORM_RECORD_ID}\n` +
      `LoadedRecordId=${form?.id || "(unknown)"}\n` +
      `Present field IDs (${presentFieldIds.length}): ${presentFieldIds.join(", ")}\n\n` +
      `Studio fieldId=${FORM_FIELD_IDS.STUDIO}\n` +
      `Studio raw=${JSON.stringify(studioRaw)}\n` +
      `Studio id=${studioId || "(null)"}\n\n` +
      `CSV fieldId=${FORM_FIELD_IDS.CSV_UPLOAD}\n` +
      `CSV raw=${JSON.stringify(csvRaw)}\n` +
      `CSV parsedUrl=${csvUrl || "(null)"}\n`;

    if (!studioId || !csvUrl) {
      const msg =
        `Missing Studio (id) and/or CSV Upload according to Airtable API response.\n\n` +
        debugDump;
      issues.push(msg);
      throw new Error(msg);
    }

    // Preload existing feedback dedupe keys for THIS studio
    console.log("Loading existing feedbacks for dedupe...");
    const existingKeys = await loadExistingDedupeKeysForStudio(studioId);
    console.log(`Loaded ${existingKeys.size} existing dedupe keys for this studio.`);

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

    const missingHeaders = validateMappedHeaders(headerIndex, CSV_HEADERS);
    if (missingHeaders.length) {
      const msg =
        `Mapped CSV headers not found in uploaded CSV:\n` +
        missingHeaders.map((m) => `- ${m}`).join("\n") +
        `\n\nCSV headers seen:\n` +
        rows[0].map((h) => `- ${h}`).join("\n");
      issues.push(msg);
      throw new Error(msg);
    }

    const totalDataRows = rows.length - 1;
    const pendingRecords = [];

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const line = i + 1; // CSV line number (1-based)
      const processedCount = i; // number of data rows processed so far

      const contact = String(getCell(row, headerIndex, CSV_HEADERS.CONTACT)).trim();
      const dateRaw = getCell(row, headerIndex, CSV_HEADERS.DATE);
      const dateISO = parseDateOnlyToISO(dateRaw);

      const ratingRaw = String(getCell(row, headerIndex, CSV_HEADERS.RATING)).trim();
      const ratingKey =
        ratingRaw && !Number.isNaN(Number(ratingRaw)) ? String(Number(ratingRaw)) : "";
      const comment = String(getCell(row, headerIndex, CSV_HEADERS.COMMENT)).trim();
      const classType = String(getCell(row, headerIndex, CSV_HEADERS.CLASSTYPE)).trim();
      const classTypeKey = String(classType || "").trim().toLowerCase();
      const instructor = String(getCell(row, headerIndex, CSV_HEADERS.INSTRUCTOR)).trim();

      if (!contact || !dateISO) {
        ignored++;
        const reason = `Line ${line}: Missing contact or date (contact="${contact}", date="${dateRaw}")`;
        issues.push(reason);
        ignoredReasons.push(reason);
      } else {
        const dateKey = dateKeyFromISO(dateISO);
        const key = makeDedupeKey({ contact, dateKey, ratingKey, instructor, classTypeKey });

        if (key && existingKeys.has(key)) {
          ignored++;
          if (isAnonymousContact(contact)) {
            ignoredReasons.push(
              `Line ${line}: Duplicate Anonymous User (studio=${studioId}, date=${dateKey}, instructor="${instructor}", rating=${
                ratingKey || "N/A"
              }, classType="${classType || ""}")`,
            );
          } else {
            ignoredReasons.push(
              `Line ${line}: Duplicate (studio=${studioId}, contact="${contact}", date=${dateKey}, classType="${classType || ""}")`,
            );
          }
        } else {
          if (key) existingKeys.add(key);

          const fields = {
            [FEEDBACK_FIELDS.CONTACT]: contact,
            [FEEDBACK_FIELDS.STUDIO]: [studioId],
            [FEEDBACK_FIELDS.DATE]: dateISO,
            [FEEDBACK_FIELDS.TYPE]: TYPE_VALUE,
          };

          if (instructor) fields[FEEDBACK_FIELDS.INSTRUCTOR_NAME] = instructor;
          if (ratingKey) {
            fields[FEEDBACK_FIELDS.RATING] = Number(ratingKey);
          }
          if (comment) fields[FEEDBACK_FIELDS.COMMENT] = comment;
          if (classType) fields[FEEDBACK_FIELDS.CLASSTYPE] = classType;

          pendingRecords.push({ fields });

          if (pendingRecords.length === 10) {
            await flushPendingFeedbackBatch(pendingRecords, importedRef);
          }
        }
      }

      // Progress log + partial log record update every 50 rows (and at the very end)
      if (processedCount % 50 === 0 || processedCount === totalDataRows) {
        // Flush any remaining records before reporting
        if (pendingRecords.length) {
          await flushPendingFeedbackBatch(pendingRecords, importedRef);
        }

        console.log(
          `Progress: processed ${processedCount}/${totalDataRows} data rows. ` +
            `Imported=${importedRef.count}, Ignored=${ignored}`,
        );

        if (logId) {
          try {
            await updateLog(logId, {
              [LOG_FIELDS.IMPORTED]: importedRef.count,
              [LOG_FIELDS.IGNORED]: ignored,
            });
          } catch (e) {
            console.error("Failed to update log progress:", e?.message || e);
          }
        }
      }
    }

    // Flush any remaining records
    if (pendingRecords.length) {
      await flushPendingFeedbackBatch(pendingRecords, importedRef);
    }

    // Log ignored records with reasons to stdout so they appear in GitHub Actions run
    if (ignoredReasons.length > 0) {
      console.log("\n--- Ignored records (with reasons) ---");
      ignoredReasons.forEach((r) => console.log(r));
      console.log("--- End ignored records ---\n");
    }

    await updateLog(logId, {
      [LOG_FIELDS.STATUS]: issues.length ? LOG_STATUS.ISSUE : LOG_STATUS.COMPLETED,
      [LOG_FIELDS.IMPORTED]: importedRef.count,
      [LOG_FIELDS.IGNORED]: ignored,
      ...(issues.length ? { [LOG_FIELDS.ISSUE_LOG]: issues.join("\n\n") } : {}),
    });

    console.log(
      `✅ Import completed. Imported=${importedRef.count}, Ignored=${ignored}, Issues=${issues.length}`,
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("❌ Fatal:", msg);

    if (logId) {
      try {
        await updateLog(logId, {
          [LOG_FIELDS.STATUS]: LOG_STATUS.ISSUE,
          [LOG_FIELDS.IMPORTED]: importedRef.count,
          [LOG_FIELDS.IGNORED]: ignored,
          [LOG_FIELDS.ISSUE_LOG]: issues.length ? issues.join("\n\n") : msg,
        });
      } catch (e) {
        console.error("Also failed to update log record:", e?.message || e);
      }
    }

    process.exit(1);
  }
}

main();
