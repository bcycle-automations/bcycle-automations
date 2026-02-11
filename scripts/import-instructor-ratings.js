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
  CONTACT: "Contact",              // TEXT
  CUSTOMER: "Customer",            // LINKED RECORD
  STUDIO: "Studio",                // LINKED RECORD
  DATE: "DATE OF RATING",           // DATE (no time)
  RATING: "Rating",
  COMMENT: "COMMENT",
  CLASSTYPE: "CLASSTYPE",
  CAL_NAME: "Instructor Name",
  TYPE: "Type - Public",
  DIRECTED_TO: "Type",
};

const DIRECTED_TO_VALUE = "Instructor Feedback";

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
    const t = await res.text();
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
    const t = await res.text();
    throw new Error(`MTEK ${res.status}: ${t}`);
  }
  return res.json();
}

function escapeFormula(v) {
  return String(v || "").replace(/"/g, '\\"');
}

function looksLikeEmail(v) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(v || "").trim());
}

/* ============================================================
   CSV PARSER (ORDER-INDEPENDENT)
============================================================ */

function normalizeHeader(h) {
  return String(h || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[_-]/g, "")
    .trim();
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
      } else cur += c;
    } else {
      if (c === '"') inQuotes = true;
      else if (c === ",") {
        row.push(cur); cur = "";
      } else if (c === "\n") {
        row.push(cur);
        rows.push(row);
        row = []; cur = "";
      } else if (c !== "\r") cur += c;
    }
  }
  row.push(cur);
  rows.push(row);
  return rows.filter(r => r.some(c => String(c).trim()));
}

/* ============================================================
   DATE NORMALIZATION (DATE-ONLY)
============================================================ */

function parseDateOnlyToISO(v) {
  if (!v) return null;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return null;
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString();
}

/* ============================================================
   AIRTABLE META – FIND CUSTOMER TABLE
============================================================ */

async function getCustomersLinkInfo() {
  const meta = await fetchJson(`${AIRTABLE_META_API}/bases/${AIRTABLE_BASE_ID}/tables`);
  const feedbacks = meta.tables.find(t => t.id === FEEDBACKS_TABLE_ID);
  const customerField = feedbacks.fields.find(f => f.name === FEEDBACK_FIELDS.CUSTOMER);

  const table = meta.tables.find(t => t.id === customerField.options.linkedTableId);
  const primary = table.fields.find(f => f.id === table.primaryFieldId);

  return {
    tableId: table.id,
    primaryField: primary.name,
  };
}

async function findCustomerByEmail(linkInfo, email) {
  const formula = `{${linkInfo.primaryField}} = "${escapeFormula(email)}"`;
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
  if (!MTEK_API_TOKEN) return null;

  try {
    const url = new URL("/api/users", MTEK_BASE_URL);
    url.searchParams.set("name_query", name);
    url.searchParams.set("page_size", "5");

    const json = await fetchMtek(url.toString());
    const users = Array.isArray(json.data) ? json.data : [];

    for (const u of users) {
      const email = u.attributes?.email;
      if (looksLikeEmail(email)) return email;
    }
    return null;
  } catch {
    return null;
  }
}

/* ============================================================
   DEDUPE CHECK
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
  return out.records?.length > 0;
}

/* ============================================================
   MAIN
============================================================ */

async function main() {
  let logId;
  let imported = 0;
  let ignored = 0;
  const issues = [];

  try {
    /* ---- Create log ---- */
    const log = await fetchJson(
      `${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${LOGS_TABLE_ID}`,
      {
        method: "POST",
        body: JSON.stringify({
          records: [{
            fields: {
              [LOG_FIELDS.STATUS]: LOG_STATUS.STARTED,
              [LOG_FIELDS.TYPE]: LOG_TYPE_VALUE,
            },
          }],
        }),
      }
    );
    logId = log.records[0].id;

    /* ---- Load form record ---- */
    const form = await fetchJson(
      `${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FORM_TABLE_ID}/${FORM_RECORD_ID}`
    );

    const studioId = form.fields[FORM_FIELDS.STUDIO]?.[0];
    const csvUrl = form.fields[FORM_FIELDS.CSV_UPLOAD]?.[0]?.url;

    if (!studioId || !csvUrl) throw new Error("Missing Studio or CSV Upload on form record");

    /* ---- Download CSV ---- */
    const csvPath = path.join(os.tmpdir(), `ratings_${Date.now()}.csv`);
    const buf = await fetch(csvUrl).then(r => r.arrayBuffer());
    fs.writeFileSync(csvPath, Buffer.from(buf));
    const csvText = fs.readFileSync(csvPath, "utf8");

    const rows = parseCSV(csvText);
    const headers = rows[0].map(normalizeHeader);
    const data = rows.slice(1);

    const idx = name => headers.indexOf(normalizeHeader(name));

    const customerLinkInfo = await getCustomersLinkInfo();

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const line = i + 2;

      const contact = row[idx("contact")]?.trim();
      const dateISO = parseDateOnlyToISO(row[idx("date")] || row[idx("date of rating")]);

      if (!contact || !dateISO) {
        ignored++;
        issues.push(`Line ${line}: Missing contact or date`);
        continue;
      }

      if (await feedbackExists({ contact, studioId, dateISO })) {
        ignored++;
        continue;
      }

      let customerId = null;
      let email = looksLikeEmail(contact) ? contact : await resolveEmailViaMtek(contact);

      if (email) {
        customerId = await findCustomerByEmail(customerLinkInfo, email);
        if (!customerId) {
          issues.push(`Line ${line}: Customer not found in Airtable for ${email}`);
        }
      }

      const fields = {
        [FEEDBACK_FIELDS.CONTACT]: contact,
        [FEEDBACK_FIELDS.STUDIO]: [studioId],
        [FEEDBACK_FIELDS.DATE]: dateISO,
        [FEEDBACK_FIELDS.DIRECTED_TO]: DIRECTED_TO_VALUE,
      };

      if (customerId) fields[FEEDBACK_FIELDS.CUSTOMER] = [customerId];
      if (row[idx("rating")]) fields[FEEDBACK_FIELDS.RATING] = Number(row[idx("rating")]);
      if (row[idx("comment")]) fields[FEEDBACK_FIELDS.COMMENT] = row[idx("comment")];
      if (row[idx("class")]) fields[FEEDBACK_FIELDS.CLASSTYPE] = row[idx("class")];

      await fetchJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${FEEDBACKS_TABLE_ID}`, {
        method: "POST",
        body: JSON.stringify({ records: [{ fields }] }),
      });

      imported++;
    }

    await fetchJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${LOGS_TABLE_ID}/${logId}`, {
      method: "PATCH",
      body: JSON.stringify({
        fields: {
          [LOG_FIELDS.STATUS]: issues.length ? LOG_STATUS.ISSUE : LOG_STATUS.COMPLETED,
          [LOG_FIELDS.IMPORTED]: imported,
          [LOG_FIELDS.IGNORED]: ignored,
          ...(issues.length ? { [LOG_FIELDS.ISSUE_LOG]: issues.join("\n") } : {}),
        },
      }),
    });

    console.log("✅ Import completed");

  } catch (err) {
    if (logId) {
      await fetchJson(`${AIRTABLE_API}/${AIRTABLE_BASE_ID}/${LOGS_TABLE_ID}/${logId}`, {
        method: "PATCH",
        body: JSON.stringify({
          fields: {
            [LOG_FIELDS.STATUS]: LOG_STATUS.ISSUE,
            [LOG_FIELDS.ISSUE_LOG]: err.message,
          },
        }),
      });
    }
    throw err;
  }
}

main().catch(e => {
  console.error("❌ Fatal:", e.message);
  process.exit(1);
});
