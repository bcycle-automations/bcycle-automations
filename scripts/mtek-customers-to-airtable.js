// scripts/mtek-customers-to-airtable.js
// Fetch MTEK Customers - Details report and send one flat webhook call per row

// =====================
// CONFIG / ENV
// =====================

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();

const REPORT_ID = process.env.MTEK_CUSTOMERS_REPORT_ID || "336";
const REPORT_SLUG = process.env.MTEK_CUSTOMERS_REPORT_SLUG || "customers-details";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");

// Hard-coded Make webhook
const WEBHOOK_URL = "https://hook.us2.make.com/pmp8d9nca57ur9ifaai8vusahpxsi3ip";

// Default date = yesterday (UTC) if not provided
function getDefaultDate() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}
const TARGET_DATE = process.env.TARGET_DATE || getDefaultDate();

// =====================
// BASIC HELPERS
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

function normalize(str) {
  return (str || "").toString().trim().toLowerCase();
}

// =====================
// MTEK TABLE REPORT
// =====================

async function fetchMtekPage(page) {
  const url = new URL("/api/table_report_data", MTEK_BASE_URL);
  url.searchParams.set("id", REPORT_ID);
  url.searchParams.set("slug", REPORT_SLUG);
  url.searchParams.set("page_size", String(PAGE_SIZE));
  url.searchParams.set("page", String(page));
  url.searchParams.set("min_join_date_day", TARGET_DATE);
  url.searchParams.set("max_join_date_day", TARGET_DATE);

  console.log(`➡️  MTEK page ${page}: ${url.toString()}`);

  return fetchJson(url.toString(), {
    headers: {
      Authorization: `Bearer ${MTEK_API_TOKEN}`,
      Accept: "application/vnd.api+json",
    },
  });
}

async function fetchAllRowsAndHeaders() {
  let page = 1;
  let allRows = [];
  let headers = null;
  let more = true;

  while (more) {
    const json = await fetchMtekPage(page);
    const attrs = json?.data?.attributes || {};
    const rows = attrs.rows || [];
    const maxExceeded = attrs.max_results_exceeded;

    if (!headers) {
      headers = attrs.headers || [];
      console.log("Headers:", headers);
    }

    if (rows.length && page === 1) {
      console.log("Sample row:", rows[0]);
    }

    allRows = allRows.concat(rows);
    console.log(`✅ Page ${page}: ${rows.length} rows (total: ${allRows.length})`);

    if (!maxExceeded || rows.length < PAGE_SIZE) {
      more = false;
    } else {
      page += 1;
      await sleep(150);
    }
  }

  return { headers, rows: allRows };
}

// =====================
// WEBHOOK
// =====================

async function postToWebhook(bodyObj) {
  const res = await fetch(WEBHOOK_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(bodyObj),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Webhook POST failed ${res.status} ${res.statusText}: ${text}`);
  }
}

// =====================
// MAIN
// =====================

async function main() {
  console.log("MTEK_BASE_URL:", MTEK_BASE_URL);
  console.log(
    "MTEK_API_TOKEN present?",
    MTEK_API_TOKEN ? `yes (starts with ${MTEK_API_TOKEN.slice(0, 4)}..., len=${MTEK_API_TOKEN.length})` : "NO"
  );
  console.log("TARGET_DATE:", TARGET_DATE);

  if (!MTEK_API_TOKEN) {
    throw new Error("Missing MTEK_API_TOKEN");
  }

  const { headers, rows } = await fetchAllRowsAndHeaders();
  console.log(`Total rows from report: ${rows.length}`);

  if (!headers || headers.length === 0) {
    throw new Error("No headers returned from report");
  }

  // Dynamically find positions from headers (no magic numbers)
  const idx = {
    customerId: headers.indexOf("Customer ID"),
    email: headers.indexOf("Email"),
    firstName: headers.indexOf("First Name"),
    lastName: headers.indexOf("Last Name"),
    fullName: headers.indexOf("Full Name"),
    joinDate: headers.indexOf("Join Date"),
    homeLocation: headers.indexOf("Home Location"),
    totalUpcoming: headers.indexOf("Total Upcoming Reservations"),
  };

  for (const [key, val] of Object.entries(idx)) {
    if (val === -1) throw new Error(`Header not found: ${key}`);
  }

  let sent = 0;
  let skippedUpcoming = 0;
  let skippedNoEmail = 0;
  let debugLogged = 0;

  for (const row of rows) {
    const email = row[idx.email];
    if (!email) {
      skippedNoEmail++;
      continue;
    }

    const totalUpcoming = Number(row[idx.totalUpcoming] || 0);
    if (totalUpcoming !== 0) {
      skippedUpcoming++;
      continue;
    }

    const customerId = row[idx.customerId];
    const firstName = row[idx.firstName] || "";
    const lastName = row[idx.lastName] || "";
    const fullName = row[idx.fullName] || `${firstName} ${lastName}`.trim() || email;
    const joinDate = row[idx.joinDate] || null;
    const joinDateDateOnly = joinDate ? String(joinDate).slice(0, 10) : null;
    const homeLocation = row[idx.homeLocation] || null;

    const body = {
      target_date: TARGET_DATE,
      customer_id: customerId,
      email,
      email_lower: normalize(email),
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      join_date: joinDate,
      join_date_date_only: joinDateDateOnly,
      home_location: homeLocation,
      total_upcoming_reservations: totalUpcoming,
    };

    if (debugLogged < 3) {
      console.log("🧪 Webhook body:", body);
      debugLogged++;
    }

    await postToWebhook(body);
    sent++;

    // gentle with Make
    await sleep(100);
  }

  console.log(
    `🎉 Done. Sent ${sent} rows to webhook, skipped (upcoming > 0): ${skippedUpcoming}, skipped (no email): ${skippedNoEmail}`
  );
}

// Run
main().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
