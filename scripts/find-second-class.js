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

// Airtable: get all customers matching the criteria
async function fetchAllAirtableCustomers() {
  const table = encodeURIComponent(CUSTOMERS_TABLE_NAME);
  const base = `${AIRTABLE_BASE_URL}/${CUSTOMER_BASE_ID}/${table}`;

  // Lead status - $15 = "First Class" AND First Class Taken is checked
  const formula = `AND({Lead status - $15} = 'First Class', {First Class Taken} = 1)`;
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
 * MTEK: fetch ALL reservations for a given user email
 * No status filter – we want every reservation, any status.
 **************************************************/
async function fetchReservationsForEmail(email) {
  if (!email) return [];

  // If MTEK uses a different filter syntax (e.g. filter[user_email]),
  // adjust this query param accordingly.
  const url =
    `${MTEK_BASE}/reservations` +
    `?user_email=${encodeURIComponent(email)}`;

  const data = await fetchJsonWithRateLimit(url, { headers: MTEK_HEADERS });
  const reservations = data.data || [];
  return reservations;
}

/**************************************************
 * Sort reservations and extract second + full status list
 **************************************************/
function extractSecondAndSummary(reservations) {
  if (!reservations.length) return null;

  // Sort by datetime ascending so "second" is truly the 2nd class
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
      order: index + 1,                         // 1st, 2nd, 3rd, ...
      reservation_id: r.id,
      class_session_id: classSessionId,
      status: attrs.status || "",               // push status for ALL classes
    };
  });

  const second = summary[1]; // index 1 = second reservation

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
  console.log(`Found ${customers.length} customer(s) matching criteria.`);

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
      console.log(`Fetching ALL reservations for ${email}…`);
      const reservations = await fetchReservationsForEmail(email);

      if (!reservations.length) {
        console.log(`No reservations found for ${email}, skipping.`);
        continue;
      }

      const info = extractSecondAndSummary(reservations);
      if (!info) {
        console.log(
          `User ${email} has fewer than 2 reservations or missing class_session relationships, skipping.`
        );
        continue;
      }

      const { second, summary } = info;

      const payload = {
        email,
        airtable_customer_id: rec.id,
        second_reservation_id: second.reservation_id,
        second_class_session_id: second.class_session_id,
        second_status: second.status,
        // full ordered list of all reservations (1st, 2nd, 3rd, 4th...)
        reservations: summary,
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
