// scripts/find-second-class.js
import process from "node:process";

const {
  MTEK_API_TOKEN,
  AIRTABLE_TOKEN,
  CUSTOMER_BASE_ID,
  CUSTOMERS_TABLE_NAME = "Customers",
  CUSTOMER_EMAIL_FIELD = "Email",
  SECOND_CLASS_WEBHOOK_URL,
} = process.env;

if (!MTEK_API_TOKEN) throw new Error("Missing env: MTEK_API_TOKEN");
if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!CUSTOMER_BASE_ID) throw new Error("Missing env: CUSTOMER_BASE_ID");
if (!SECOND_CLASS_WEBHOOK_URL) throw new Error("Missing env: SECOND_CLASS_WEBHOOK_URL");

const MTEK_BASE = "https://bcycle.marianatek.com/api";
const AIRTABLE_BASE_URL = "https://api.airtable.com/v0";

const MTEK_HEADERS = {
  Authorization: `Bearer ${MTEK_API_TOKEN}`,
  Accept: "application/vnd.api+json",
};

const AIRTABLE_HEADERS = {
  Authorization: `Bearer ${AIRTABLE_TOKEN}`,
  Accept: "application/json",
};

/**************************************************
 * Helpers
 **************************************************/
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Airtable: get all customers where First Class Taken is checked
async function fetchAllAirtableCustomers() {
  const table = encodeURIComponent(CUSTOMERS_TABLE_NAME);
  const base = `${AIRTABLE_BASE_URL}/${CUSTOMER_BASE_ID}/${table}`;

  // Only filter on First Class Taken = 1
  const formula = `AND({First Class Taken} = 1, {2nd Class Taken} = 0)`;
  let url = `${base}?filterByFormula=${encodeURIComponent(formula)}`;

  const records = [];

  while (url) {
    const res = await fetch(url, { headers: AIRTABLE_HEADERS });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(
        `Airtable request failed ${res.status} ${res.statusText} for ${url}: ${text}`
      );
    }
    const data = await res.json();
    records.push(...(data.records || []));

    if (data.offset) {
      url = `${base}?filterByFormula=${encodeURIComponent(
        formula
      )}&offset=${data.offset}`;
    } else {
      url = null;
    }
  }

  return records;
}

// MTEK: rate-limit aware fetch
async function fetchJsonWithRateLimit(url, options = {}, maxRetries = 5) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const res = await fetch(url, options);

    if (res.status !== 429) {
      if (!res.ok) {
        const text = await res.text();
        throw new Error(
          `Request failed ${res.status} ${res.statusText} for ${url}: ${text}`
        );
      }
      return res.json();
    }

    const retryAfterHeader = res.headers.get("Retry-After");
    let delayMs = 0;

    if (retryAfterHeader) {
      const secs = Number(retryAfterHeader);
      if (!Number.isNaN(secs)) {
        delayMs = secs * 1000;
      } else {
        const retryDate = new Date(retryAfterHeader);
        if (!Number.isNaN(retryDate.getTime())) {
          delayMs = retryDate.getTime() - Date.now();
        }
      }
    }

    if (!delayMs || delayMs < 0) delayMs = 2000;

    console.warn(
      `Got 429 from MTEK for ${url}. Retry-After=${retryAfterHeader || "n/a"}; ` +
        `waiting ${Math.round(delayMs / 1000)}s before retry (attempt ${attempt}/${maxRetries})`
    );

    await sleep(delayMs);
  }

  throw new Error(`Exceeded max retries (${maxRetries}) for ${url} after repeated 429s.`);
}

/**************************************************
 * MTEK: get user ID from email, then reservations by user ID
 **************************************************/

// 1) Find user ID by email using /users?email=
async function findUserIdByEmail(email) {
  if (!email) return null;

  const url =
    `${MTEK_BASE}/users` +
    `?email=${encodeURIComponent(email)}`;

  const data = await fetchJsonWithRateLimit(url, { headers: MTEK_HEADERS });
  const users = data.data || [];

  if (!users.length) return null;

  if (users.length > 1) {
    console.warn(
      `Warning: multiple users found for email ${email}, using the first one (id=${users[0].id})`
    );
  }

  return users[0].id;
}

// 2) Fetch reservations for that user ID using /reservations?user=
async function fetchReservationsForUser(userId) {
  if (!userId) return [];

  const url =
    `${MTEK_BASE}/reservations` +
    `?user=${encodeURIComponent(userId)}`;

  const data = await fetchJsonWithRateLimit(url, { headers: MTEK_HEADERS });
  const reservations = data.data || [];
  return reservations;
}

/**************************************************
 * Sort reservations and build summary
 **************************************************/
function extractSecondAndSummary(reservations) {
  if (!reservations.length) return null;

  const sorted = [...reservations].sort((a, b) => {
    const aAttrs = a.attributes || {};
    const bAttrs = b.attributes || {};

    const da =
      aAttrs.class_session_min_datetime ||
      `${aAttrs.start_date || ""}T${aAttrs.start_time || ""}`;
    const db =
      bAttrs.class_session_min_datetime ||
      `${bAttrs.start_date || ""}T${bAttrs.start_time || ""}`;

    return String(da).localeCompare(String(db));
  });

  if (sorted.length < 2) return null;

  const summary = sorted.map((r, index) => {
    const attrs = r.attributes || {};
    const rels = r.relationships || {};
    const classSessionId = rels.class_session?.data?.id || null;

    return {
      order: index + 1,         // 1st, 2nd, 3rd...
      reservation_id: r.id,
      class_session_id: classSessionId,
      status: attrs.status || "",
    };
  });

  const second = summary[1];

  if (!second || !second.class_session_id) {
    return null;
  }

  return {
    second,
    summary,
  };
}

/**************************************************
 * Main
 **************************************************/
async function main() {
  console.log("Fetching matching customers from Airtable…");
  const customers = await fetchAllAirtableCustomers();
  console.log(`Found ${customers.length} customer(s) with First Class Taken checked.`);

  let processed = 0;

  for (const rec of customers) {
    const fields = rec.fields || {};
    const email = fields[CUSTOMER_EMAIL_FIELD];

    if (!email) {
      console.warn(
        `Skipping Airtable record ${rec.id}: no email in field '${CUSTOMER_EMAIL_FIELD}'`
      );
      continue;
    }

    try {
      console.log(`Looking up user for email ${email}…`);
      const userId = await findUserIdByEmail(email);

      if (!userId) {
        console.log(`No MTEK user found for ${email}, skipping.`);
        continue;
      }

      console.log(`Fetching reservations for user ${userId} (${email})…`);
      const reservations = await fetchReservationsForUser(userId);

      if (!reservations.length) {
        console.log(`No reservations for ${email}, skipping.`);
        continue;
      }

      const info = extractSecondAndSummary(reservations);
      if (!info) {
        console.log(
          `User ${email} has fewer than 2 reservations, skipping.`
        );
        continue;
      }

      const { second, summary } = info;

      const payload = {
        email,
        airtable_customer_id: rec.id,
        mtek_user_id: userId,
        second_reservation_id: second.reservation_id,
        second_class_session_id: second.class_session_id,
        second_status: second.status,
        reservations: summary, // FULL ordered list with statuses
      };

      console.log(
        `Sending second-class info for ${email} (second_class_session_id=${second.class_session_id}, status=${second.status}) to webhook…`
      );

      const res = await fetch(SECOND_CLASS_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const text = await res.text();
        console.warn(
          `Webhook failed for ${email}: ${res.status} ${res.statusText} – ${text}`
        );
      } else {
        processed++;
      }
    } catch (err) {
      console.error(`Error processing customer ${email}:`, err.message);
    }
  }

  console.log(`Done. Successfully sent ${processed} second-class payload(s) to webhook.`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
