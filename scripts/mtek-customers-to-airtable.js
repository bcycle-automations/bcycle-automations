// scripts/mtek-customers-to-airtable.js

// ---------- Config ----------
const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const RAW_MTEK_API_TOKEN = process.env.MTEK_API_TOKEN || "";
const MTEK_API_TOKEN = RAW_MTEK_API_TOKEN.trim(); // clean token
const REPORT_ID = process.env.MTEK_CUSTOMERS_REPORT_ID || "336";
const REPORT_SLUG = process.env.MTEK_CUSTOMERS_REPORT_SLUG || "customers-details";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");

// Airtable
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;
const CUSTOMERS_TABLE = process.env.AIRTABLE_CUSTOMERS_TABLE || "Customers";
const STUDIOS_TABLE = process.env.AIRTABLE_STUDIOS_TABLE || "Studio";

// Default date = yesterday (UTC) if not provided
function getDefaultDate() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10);
}
const TARGET_DATE = process.env.TARGET_DATE || getDefaultDate();

// ---- Column indices from the MTEK report rows ----
// 🔴 YOU MUST ADJUST THESE TO MATCH YOUR REPORT
const COL_EMAIL        = 2;
const COL_FIRST_NAME   = 3;
const COL_LAST_NAME    = 4;
const COL_FULL_NAME    = 5;
const COL_DATE_JOINED  = 6;
const COL_HOME_STUDIO  = 25;

// ---- Fields in Airtable ----
const FIELD_EMAIL_LOWER     = "Email (lower)";
const FIELD_EMAIL           = "Email";
const FIELD_NAME            = "Name";
const FIELD_PROFILE_CREATED = "Profile Created";
const FIELD_PREFERRED_STUDIO = "Preferred Studio";
const FIELD_FIRST_CHECKIN   = "First Check-In";

// ---------- Sanity checks ----------
console.log("MTEK_BASE_URL:", MTEK_BASE_URL);
console.log(
  "MTEK_API_TOKEN present?",
  MTEK_API_TOKEN
    ? `yes (starts with ${MTEK_API_TOKEN.slice(0,4)}..., len=${MTEK_API_TOKEN.length})`
    : "NO"
);
console.log("Airtable base:", AIRTABLE_BASE_ID);

if (!MTEK_API_TOKEN) {
  console.error("Missing or empty MTEK_API_TOKEN");
  process.exit(1);
}
if (!AIRTABLE_TOKEN) {
  console.error("Missing AIRTABLE_TOKEN");
  process.exit(1);
}
if (!AIRTABLE_BASE_ID) {
  console.error("Missing AIRTABLE_BASE_ID");
  process.exit(1);
}

// ---------- Helpers ----------
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed ${res.status} ${res.statusText} for ${url}: ${text}`);
  }
  return res.json();
}

function normalize(str) {
  return (str || "").toString().trim().toLowerCase();
}

// ---------- MTEK: table report ----------
async function fetchMtekReportPage(page) {
  const url = new URL("/api/table_report_data", MTEK_BASE_URL);
  url.searchParams.set("id", REPORT_ID);
  url.searchParams.set("slug", REPORT_SLUG);
  url.searchParams.set("page_size", String(PAGE_SIZE));
  url.searchParams.set("page", String(page));
  url.searchParams.set("min_join_date_day", TARGET_DATE);
  url.searchParams.set("max_join_date_day", TARGET_DATE);

  const finalUrl = url.toString();
  console.log(`➡️  Calling MTEK page ${page}: ${finalUrl}`);
  console.log(
    "Auth header:",
    `Bearer ${MTEK_API_TOKEN.slice(0,4)}...`
  );

  return fetchJson(finalUrl, {
    headers: {
      // ✅ MTEK in your account expects Bearer
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: "application/vnd.api+json",
    },
  });
}

async function fetchAllMtekRows() {
  let page = 1;
  let allRows = [];
  let more = true;

  while (more) {
    const json = await fetchMtekReportPage(page);
    const attrs = json?.data?.attributes || {};
    const rows = attrs.rows || [];
    const maxExceeded = attrs.max_results_exceeded;

    if (allRows.length === 0 && rows.length > 0) {
      console.log("Sample row from report:", rows[0]);
    }

    allRows = allRows.concat(rows);
    console.log(`✅ Page ${page}: ${rows.length} rows (total: ${allRows.length})`);

    if (!maxExceeded || rows.length < PAGE_SIZE) {
      more = false;
    } else {
      page += 1;
      await sleep(200);
    }
  }

  return allRows;
}

// ---------- Airtable helpers ----------
const AIRTABLE_BASE_URL = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/`;

async function airtableGet(table, params = {}) {
  const url = new URL(AIRTABLE_BASE_URL + encodeURIComponent(table));
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, v);
  });

  return fetchJson(url.toString(), {
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      Accept: "application/json",
    },
  });
}

async function airtableCreate(table, fields) {
  const url = AIRTABLE_BASE_URL + encodeURIComponent(table);
  const json = await fetchJson(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ records: [{ fields }] }),
  });
  return json.records[0];
}

async function airtableUpdate(table, recordId, fields) {
  const url = AIRTABLE_BASE_URL + encodeURIComponent(table);
  const json = await fetchJson(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ records: [{ id: recordId, fields }] }),
  });
  return json.records[0];
}

// ---------- Airtable: studios ----------
async function fetchAllStudios() {
  let offset;
  const studios = [];

  do {
    const params = { pageSize: "100" };
    if (offset) params.offset = offset;

    const json = await airtableGet(STUDIOS_TABLE, params);
    (json.records || []).forEach((rec) => {
      studios.push({
        id: rec.id,
        name: rec.fields?.Name || rec.fields?.name || "",
      });
    });
    offset = json.offset;
  } while (offset);

  console.log(`📦 Loaded ${studios.length} studios`);
  return studios;
}

function findBestStudioId(studios, homeStudioName) {
  const target = normalize(homeStudioName);
  if (!target) return null;

  let studio = studios.find((s) => normalize(s.name) === target);
  if (studio) return studio.id;

  studio = studios.find((s) => {
    const n = normalize(s.name);
    return n.includes(target) || target.includes(n);
  });
  if (studio) return studio.id;

  return null;
}

// ---------- Airtable: customer lookup ----------
async function findCustomerByEmailLower(emailLower) {
  const formula = `LOWER({${FIELD_EMAIL_LOWER}}) = '${emailLower.replace(/'/g, "\\'")}'`;
  const json = await airtableGet(CUSTOMERS_TABLE, {
    filterByFormula: formula,
    maxRecords: "1",
  });
  return json.records && json.records.length > 0 ? json.records[0] : null;
}

// ---------- Main ----------
async function main() {
  console.log(`🚀 MTEK → Airtable for date ${TARGET_DATE}`);

  const [rows, studios] = await Promise.all([
    fetchAllMtekRows(),
    fetchAllStudios(),
  ]);

  console.log(`Total rows from MTEK report: ${rows.length}`);

  let created = 0;
  let updated = 0;
  let skippedFirstCheckIn = 0;
  let skippedNoEmail = 0;

  for (const row of rows) {
    const email = row[COL_EMAIL];
    if (!email) {
      skippedNoEmail++;
      continue;
    }

    const emailLower = normalize(email);
    const firstName = row[COL_FIRST_NAME] || "";
    const lastName = row[COL_LAST_NAME] || "";
    const fullNameFromReport = row[COL_FULL_NAME] || "";
    const name =
      fullNameFromReport ||
      `${firstName} ${lastName}`.trim() ||
      email;

    const joinDateRaw = row[COL_DATE_JOINED];
    const profileCreatedDate = joinDateRaw
      ? joinDateRaw.toString().slice(0, 10)
      : TARGET_DATE;

    const homeStudioName = row[COL_HOME_STUDIO] || "";
    const preferredStudioId = findBestStudioId(studios, homeStudioName);

    let customer = await findCustomerByEmailLower(emailLower);

    if (customer) {
      const fields = customer.fields || {};
      const firstCheckIn = fields[FIELD_FIRST_CHECKIN];

      if (firstCheckIn) {
        skippedFirstCheckIn++;
        continue;
      }

      const updateFields = {
        [FIELD_EMAIL_LOWER]: emailLower,
        [FIELD_EMAIL]: email,
        [FIELD_NAME]: name,
        [FIELD_PROFILE_CREATED]: profileCreatedDate,
      };
      if (preferredStudioId) {
        updateFields[FIELD_PREFERRED_STUDIO] = [preferredStudioId];
      }

      await airtableUpdate(CUSTOMERS_TABLE, customer.id, updateFields);
      updated++;
    } else {
      const createFields = {
        [FIELD_EMAIL_LOWER]: emailLower,
        [FIELD_EMAIL]: email,
        [FIELD_NAME]: name,
        [FIELD_PROFILE_CREATED]: profileCreatedDate,
      };
      if (preferredStudioId) {
        createFields[FIELD_PREFERRED_STUDIO] = [preferredStudioId];
      }

      await airtableCreate(CUSTOMERS_TABLE, createFields);
      created++;
    }

    await sleep(200);
  }

  console.log(
    `✅ Done. Created: ${created}, Updated: ${updated}, Skipped (First Check-In): ${skippedFirstCheckIn}, Skipped (no email): ${skippedNoEmail}`
  );
}

main().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
