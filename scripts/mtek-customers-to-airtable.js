// scripts/mtek-customers-to-webhook.js

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const REPORT_ID = "336";
const REPORT_SLUG = "customers-details";
const PAGE_SIZE = 500;

const WEBHOOK_URL = "https://hook.us2.make.com/pmp8d9nca57ur9ifaai8vusahpxsi3ip";

function getDefaultDate() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}
const TARGET_DATE = process.env.TARGET_DATE || getDefaultDate();

// Helper
async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

async function fetchPage(page) {
  const url = new URL("/api/table_report_data", MTEK_BASE_URL);
  url.searchParams.set("id", REPORT_ID);
  url.searchParams.set("slug", REPORT_SLUG);
  url.searchParams.set("page_size", String(PAGE_SIZE));
  url.searchParams.set("page", String(page));
  url.searchParams.set("min_join_date_day", TARGET_DATE);
  url.searchParams.set("max_join_date_day", TARGET_DATE);

  return fetchJson(url.toString(), {
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: "application/vnd.api+json",
    },
  });
}

async function fetchAllRows() {
  let allRows = [];
  let headers = null;
  let page = 1;
  let more = true;

  while (more) {
    const json = await fetchPage(page);
    const attrs = json.data.attributes;
    const rows = attrs.rows || [];

    if (!headers) headers = attrs.headers;
    if (rows.length > 0 && page === 1) console.log("Sample row:", rows[0]);

    allRows = allRows.concat(rows);

    if (!attrs.max_results_exceeded || rows.length < PAGE_SIZE) {
      more = false;
    } else {
      page++;
    }
  }

  return { headers, rows: allRows };
}

async function sendToWebhook(payload) {
  console.log(`➡️ Sending ${payload.rows.length} rows to webhook...`);

  const res = await fetch(WEBHOOK_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) throw new Error(await res.text());
  console.log("✅ Webhook accepted payload");
}

async function main() {
  console.log(`Fetching customers for ${TARGET_DATE}`);
  const { headers, rows } = await fetchAllRows();

  // Get index of Total Upcoming Reservations from headers dynamically:
  const upcomingIndex = headers.indexOf("Total Upcoming Reservations");
  if (upcomingIndex === -1) throw new Error("Column 'Total Upcoming Reservations' not found");

  const filteredRows = rows.filter(r => Number(r[upcomingIndex] || 0) === 0);

  console.log(`Filtered rows: ${filteredRows.length} / ${rows.length}`);

  const payload = {
    target_date: TARGET_DATE,
    headers,
    rows: filteredRows   // ← THIS IS THE SINGLE ARRAY YOU WANT
  };

  await sendToWebhook(payload);
}

main().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});
