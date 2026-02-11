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
  MTEK_API_TOKEN,
} = process.env;

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");

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
const AIRTABLE_META_API = "https://api.airtable.com/v0/meta";

/* ---- Form table ---- */
const FORM_FIELDS = {
  CSV_UPLOAD: "CSV Upload",
  STUDIO: "Studio",
};

/* ---- Feedbacks table ---- */
const FEEDBACK_FIELDS = {
  CONTACT: "Contact",            // TEXT
  CUSTOMER: "Customer",          // LINKED RECORD
  STUDIO: "Studio",              // LINKED RECORD
  DATE: "DATE OF RATING",        // DATE (no time)
  RATING: "Rating",              // NUMBER
  COMMENT: "COMMENT",            // LONG TEXT
  CLASSTYPE: "CLASSTYPE",        // TEXT / SINGLE SELECT (your choice)
  INSTRUCTOR_NAME: "Instructor Name", // TEXT
  TYPE_PUBLIC: "Type - Public",  // OPTIONAL (if exists)
  TYPE: "Type",                  // SINGLE SELECT -> set to "Instructor Feedback"
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

async function fetchMtek(url) {
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

function escapeFormula(v) {
  return String(v ?? "").replace(/"/g, '\\"');
}

function looksLikeEmail(v) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(v ?? "").trim());
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

  // Remove empty trailing rows
  return rows.filter((r) => r.some((c) => String(c).trim() !== ""));
}

function makeHeaderIndex(headers) {
  const map = new Map();
  headers.forEach((h, i) => map.set(normalizeHeader(h), i));
  return map;
}

function getCell(row, headerIndex, ...headerCandidates) {
  for (const h of headerCandidates) {
    const idx = headerIndex.get(normalizeHeader(h));
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

  // Let JS parse common formats (incl. M/D/YYYY)
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return null;

  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString();
}

/* ============================================================
   AIRTABLE META – FIND CUSTOMER TABLE + PRIMARY FIELD
============================================================ */

async function getCustomersLinkInfo() {
  const meta = await fetchJson(`${AIRTABLE_META_API}/bases/${AIRTABLE_BASE_ID}/tables`);
  const feedbacks = meta.tables.find((t) => t.id === FEEDBACKS_TABLE_ID);
  if (!feedbacks) throw new Error(`Meta: feedbacks table not found: ${FEEDBACKS_TABLE_ID}`);

  const customerField = feedbacks.fields.find((f) => f.name === FEEDBACK_FIELDS.CUSTOMER);
  if (!customerField?.options?.linkedTableId) {
    throw new Error(`Meta: could not resolve linked table for field "${FEEDBACK_FIELDS.CUSTOMER}"`);
  }

  const customersTable = meta.tables.find((t) => t.id === customerField.options.linkedTableId);
  if (!customersTable) throw new Error(`Meta: customers linked table not found`);

  const primaryField = customersTable.fields.find((f) => f.id === customersTable.primaryFieldId);
  if (!primaryField) throw new Error(`Meta: customers primary field not found`);

  return {
    tableId: customersTable.id,
    primaryField: primaryField.name,
  };
}

async function findCustomerByEmail(linkInfo, email) {
  const e = String(email || "").trim();
  if (!e) return null;

  const formula = `{${linkInfo.primaryField}} = "${escapeFormula(e)}"`;
  const url =
    `${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${linkInfo.tableId}` +
    `?pageSize=1&filterByFormula=${encodeURIComponent(formula)}`;

  const out = await fetchJson(url);
  return out.records?.[0]?.id || null;
}

/* ============================================================
   MTEK – NAME → EMAIL (USING name_query)
============================================================ */

async function resolveEmailViaMtek(name) {
  const q = String(name || "").trim();
  if (!q) return null;
  if (!MTEK_API_TOKEN) return null;

  try {
    const url = new URL("/api/users", MTEK_BASE_URL);
    url.searchParams.set("name_query", q);
    url.searchParams.set("page_size", "5");

    const json = await fetchMtek(url.toString());
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

async function loadFormRecord() {
  return fetchJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FORM_TABLE_ID}/${FORM_RECORD_ID}`);
}

async function downloadCsvToTemp(csvUrl) {
  const csvPath = path.join(os.tmpdir(), `ratings_${Date.now()}.csv`);
  const res = await fetch(csvUrl);
  if (!res.ok) throw new Error(`CSV download failed ${res.status} ${res.statusText}`);
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(csvPath, buf);
  return csvPath;
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
    const studioId = form?.fields?.[FORM_FIELDS.STUDIO]?.[0];
    const csvUrl = form?.fields?.[FORM_FIELDS.CSV_UPLOAD]?.[0]?.url;

    if (!studioId) throw new Error(`Missing "${FORM_FIELDS.STUDIO}" on form record`);
    if (!csvUrl) throw new Error(`Missing "${FORM_FIELDS.CSV_UPLOAD}" attachment on form record`);

    const csvPath = await downloadCsvToTemp(csvUrl);
    const csvText = fs.readFileSync(csvPath, "utf8");

    const rows = parseCSV(csvText);
    if (rows.length < 2) throw new Error("CSV has no data rows");

    const headerIndex = makeHeaderIndex(rows[0]);

    // Debug: see what the importer thinks the headers are
    console.log("CSV headers:", rows[0]);
    console.log("CSV headers (normalized):", rows[0].map(normalizeHeader));

    const customerLinkInfo = await getCustomersLinkInfo();

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const line = i + 1;

      // Pull from CSV (supports shifting order + slight naming variations)
      const contact = String(getCell(row, headerIndex, "Contact")).trim();

      const dateRaw = getCell(row, headerIndex, "DATE OF RATING", "Date");
      const dateISO = parseDateOnlyToISO(dateRaw);

      const ratingRaw = String(getCell(row, headerIndex, "Rating", "RATING")).trim();
      const comment = String(getCell(row, headerIndex, "Comment", "COMMENT")).trim();
      const classType = String(getCell(row, headerIndex, "Class", "CLASSTYPE")).trim();
      const instructor = String(getCell(row, headerIndex, "Instructor", "Instructor Name", "CAL_NAME")).trim();
      const typePublic = String(getCell(row, headerIndex, "Type", "Type - Public")).trim(); // optional

      if (!contact || !dateISO) {
        ignored++;
        issues.push(`Line ${line}: Missing contact or date (contact="${contact}", date="${dateRaw}")`);
        continue;
      }

      // Dedup
      if (await feedbackExists({ contact, studioId, dateISO })) {
        ignored++;
        continue;
      }

      // Resolve Customer linked record
      let customerId = null;
      const email = looksLikeEmail(contact) ? contact : await resolveEmailViaMtek(contact);

      if (email) {
        customerId = await findCustomerByEmail(customerLinkInfo, email);
        if (!customerId) issues.push(`Line ${line}: Customer not found in Airtable for ${email}`);
      } else {
        issues.push(`Line ${line}: Could not resolve email from Contact "${contact}"`);
      }

      // Build Airtable fields
      const fields = {
        [FEEDBACK_FIELDS.CONTACT]: contact,
        [FEEDBACK_FIELDS.STUDIO]: [studioId],
        [FEEDBACK_FIELDS.DATE]: dateISO,
        [FEEDBACK_FIELDS.TYPE]: TYPE_VALUE, // sets "Type" single select
      };

      if (customerId) fields[FEEDBACK_FIELDS.CUSTOMER] = [customerId];
      if (instructor) fields[FEEDBACK_FIELDS.INSTRUCTOR_NAME] = instructor; // ✅ FIX
      if (ratingRaw && !Number.isNaN(Number(ratingRaw))) fields[FEEDBACK_FIELDS.RATING] = Number(ratingRaw);
      if (comment) fields[FEEDBACK_FIELDS.COMMENT] = comment;
      if (classType) fields[FEEDBACK_FIELDS.CLASSTYPE] = classType;

      // Optional: if you actually want to store the CSV "Type - Public"/"Type" value somewhere
      // Only set if the field exists and you want it.
      if (typePublic) {
        // If your Airtable field is a checkbox/text and exists:
        // fields[FEEDBACK_FIELDS.TYPE_PUBLIC] = typePublic;
        // Leaving commented to avoid Airtable "unknown field" failures.
      }

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
