// scripts/mtek-customers-to-airhook.js
// Or reuse your existing filename (e.g. mtek-customers-to-airtable.js) in the workflow.

// =====================
// CONFIG / ENV
// =====================

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const RAW_MTEK_API_TOKEN = process.env.MTEK_API_TOKEN || "";
const MTEK_API_TOKEN = RAW_MTEK_API_TOKEN.trim(); // clean whitespace

const REPORT_ID = process.env.MTEK_CUSTOMERS_REPORT_ID || "336";
const REPORT_SLUG = process.env.MTEK_CUSTOMERS_REPORT_SLUG || "customers-details";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");

// Hard-coded Make webhook URL (as requested)
const WEBHOOK_URL = "https://hook.us2.make.com/pmp8d9nca57ur9ifaai8vusahpxsi3ip";

// If TARGET_DATE not provided, default = yesterday (UTC)
function getDefaultDate() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}
const TARGET_DATE = process.env.TARGET_DATE || getDefaultDate();

// =====================
// SANITY CHECKS
// =====================

console.log("MTEK_BASE_URL:", MTEK_BASE_URL);
console.log(
  "MTEK_API_TOKEN present?",
  MTEK_API_TOKEN
    ? `yes (starts with ${MTEK_API_TOKEN.slice(0, 4)}..., len=${MTEK_API_TOKEN.length})`
    : "NO"
);
console.log("Target date:", TARGET_DATE);
console.log("Webhook URL:", WEBHOOK_URL);

if (!MTEK_API_TOKEN) {
  console.error("❌ Missing or empty MTEK_API_TOKEN env var");
  process.exit(1);
}

// =====================
// HELPERS
// =====================

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed ${res.status} ${res.statusText} for ${url}: ${text}`);
  }
  return res.json();
}

// =====================
// MTEK: TABLE REPORT
// =====================

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

  return fetchJson(finalUrl, {
    headers: {
      // You confirmed your instance expects Bearer for API auth
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: "application/vnd.api+json",
    },
  });
}

async function fetchAllMtekRowsAndHeaders() {
  let page = 1;
  let allRows = [];
  let headers = null;
  let more = true;

  while (more) {
    const json = await fetchMtekReportPage(page);
    const attrs = json?.data?.attributes || {};
    const rows = attrs.rows || [];
    const maxExceeded = attrs.max_results_exceeded;

    if (!headers) {
      headers = attrs.headers || [];
      console.log("Headers from report:", headers);
    }

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

  return { headers, rows: allRows };
}

// =====================
// SEND TO MAKE WEBHOOK
// =====================

async function postToWebhook(payload) {
  console.log(`➡️  Posting ${payload.rows.length} rows to Make webhook`);

  const res = await fetch(WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Webhook POST failed ${res.status} ${res.statusText}: ${text}`);
  }

  console.log("✅ Webhook POST succeeded");
}

// =====================
// MAIN
// =====================

async function main() {
  console.log(`🚀 MTEK → Make webhook sync for date ${TARGET_DATE}`);

  const { headers, rows } = await fetchAllMtekRowsAndHeaders();
  console.log(`Total rows from MTEK report: ${rows.length}`);

  if (!headers || headers.length === 0) {
    throw new Error("No headers returned from report; cannot determine column positions.");
  }

  // Find the index of "Total Upcoming Reservations" from headers
  const upcomingIndex = headers.indexOf("Total Upcoming Reservations");
  if (upcomingIndex === -1) {
    throw new Error('Could not find "Total Upcoming Reservations" in headers.');
  }
  console.log('"Total Upcoming Reservations" index:', upcomingIndex);

  // Filter: only rows with Total Upcoming Reservations == 0
  const filteredRows = rows.filter((row) => {
    const raw = row[upcomingIndex];
    const totalUpcoming = Number(raw || 0);
    return totalUpcoming === 0;
  });

  console.log(
    `Filtered rows with Total Upcoming Reservations = 0: ${filteredRows.length} / ${rows.length}`
  );

  // Build payload for Make
  const payload = {
    target_date: TARGET_DATE,
    headers,
    rows: filteredRows,
  };

  await postToWebhook(payload);

  console.log("🎉 Done.");
}

main().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
