// scripts/mtek-customers-to-airtable.js
// For each customer in the "Customers - Details" report on TARGET_DATE:
//  - fetch all reservations with user=<Customer ID>
//  - ignore any reservation whose status contains "cancel" (case-insensitive)
//  - pick the OLDEST remaining reservation (by date)
//  - send one flat webhook call per customer with:
//        customer info + phone_number + reservation_status + class_session_id (if any)

// =====================
// CONFIG / ENV
// =====================

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();

const REPORT_ID = process.env.MTEK_CUSTOMERS_REPORT_ID || "336";
const REPORT_SLUG = process.env.MTEK_CUSTOMERS_REPORT_SLUG || "customers-details";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");

// Hard-coded Make webhook (per your request)
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
// MTEK: TABLE REPORT
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
// MTEK: RESERVATIONS LOOKUP
// =====================

// Fetch all reservations for this user and return the OLDEST non-canceled one
async function fetchOldestNonCanceledReservation(customerId) {
  if (!customerId) return null;

  const url = new URL("/api/reservations", MTEK_BASE_URL);
  // user=<Customer ID> as you confirmed
  url.searchParams.set("user", String(customerId));
  url.searchParams.set("page_size", "100"); // reasonable batch size

  const finalUrl = url.toString();
  console.log(`   ↪︎ Reservations for user ${customerId}: ${finalUrl}`);

  try {
    const json = await fetchJson(finalUrl, {
      headers: {
        Authorization: `Bearer ${MTEK_API_TOKEN}`,
        Accept: "application/vnd.api+json",
      },
    });

    let data = json?.data;
    if (!data) return null;
    if (!Array.isArray(data)) data = [data];
    if (data.length === 0) return null;

    // 1) Filter out any reservation whose status contains "cancel" (case-insensitive)
    const nonCanceled = data.filter((item) => {
      const status = (item.attributes?.status || "").toString().toLowerCase();
      return status && !status.includes("cancel");
    });

    if (nonCanceled.length === 0) return null;

    // 2) Pick the OLDEST reservation by a best-guess date field
    const withDates = nonCanceled.map((item) => {
      const attrs = item.attributes || {};
      const dateStr =
        attrs.class_session_start_datetime ||
        attrs.class_session_start ||
        attrs.start_datetime ||
        attrs.created_at ||
        attrs.updated_at ||
        null;

      const ts = dateStr ? Date.parse(dateStr) : null;
      return { item, ts };
    });

    // If none have a parsable date, just pick the first non-canceled reservation
    if (withDates.every((x) => x.ts === null)) {
      return nonCanceled[0];
    }

    withDates.sort((a, b) => {
      if (a.ts === null) return 1;
      if (b.ts === null) return -1;
      return a.ts - b.ts; // smallest (oldest) timestamp first
    });

    return withDates[0].item;
  } catch (err) {
    console.error(`   ⚠️ Error fetching reservations for user ${customerId}:`, err.message);
    return null;
  }
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
  console.log("WEBHOOK_URL:", WEBHOOK_URL);

  if (!MTEK_API_TOKEN) {
    throw new Error("Missing MTEK_API_TOKEN");
  }

  const { headers, rows } = await fetchAllRowsAndHeaders();
  console.log(`Total rows from report: ${rows.length}`);

  if (!headers || headers.length === 0) {
    throw new Error("No headers returned from report");
  }

  // Dynamically find positions from headers
  const idx = {
    customerId: headers.indexOf("Customer ID"),
    email: headers.indexOf("Email"),
    firstName: headers.indexOf("First Name"),
    lastName: headers.indexOf("Last Name"),
    fullName: headers.indexOf("Full Name"),
    joinDate: headers.indexOf("Join Date"),
    homeLocation: headers.indexOf("Home Location"),
    phoneNumber: headers.indexOf("Phone Number"),
  };

  for (const [key, val] of Object.entries(idx)) {
    if (val === -1) throw new Error(`Header not found: ${key}`);
  }

  let sent = 0;
  let debugLogged = 0;

  for (const row of rows) {
    const customerId = row[idx.customerId];
    const email = row[idx.email] || null;
    const firstName = row[idx.firstName] || "";
    const lastName = row[idx.lastName] || "";
    const fullName = row[idx.fullName] || `${firstName} ${lastName}`.trim() || email || "";
    const joinDate = row[idx.joinDate] || null;
    const joinDateDateOnly = joinDate ? String(joinDate).slice(0, 10) : null;
    const homeLocation = row[idx.homeLocation] || null;
    const phoneNumber = row[idx.phoneNumber] || null;

    // Oldest non-canceled reservation for this user
    const reservation = await fetchOldestNonCanceledReservation(customerId);

    let reservationStatus = null;
    let classSessionId = null;

    if (reservation) {
      reservationStatus = reservation.attributes?.status || null;
      classSessionId = reservation.relationships?.class_session?.data?.id || null;
    }

    // Build flat body: ALL customers pushed, even if no reservation or no class ID
    const body = {
      target_date: TARGET_DATE,
      customer_id: customerId,
      email,
      email_lower: email ? normalize(email) : null,
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      join_date: joinDate,
      join_date_date_only: joinDateDateOnly,
      home_location: homeLocation,
      phone_number: phoneNumber,
      reservation_status: reservationStatus,
    };

    // Only include class_session_id if we actually have one
    if (classSessionId) {
      body.class_session_id = classSessionId;
    }

    if (debugLogged < 3) {
      console.log("🧪 Webhook body:", body);
      debugLogged++;
    }

    await postToWebhook(body);
    sent++;

    // Be gentle with both MTEK + Make
    await sleep(150);
  }

  console.log(`🎉 Done. Sent ${sent} customers to webhook`);
}

// Run
main().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
