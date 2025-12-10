// scripts/sync-mtek-emails.js

/**
 * CONFIG — EDIT THESE CONSTANTS ONLY
 * ----------------------------------
 * You can change these without touching the rest of the code.
 */

const AIRTABLE_BASE_ID = "appWPXRyXX3KHoJRI";      // your base
const AIRTABLE_TABLE_NAME = "Ratings";             // your table
const AIRTABLE_VIEW_NAME = "ADD EMAIL DO NOT TOUCH";
const AIRTABLE_EMAIL_FIELD = "EMAIL";              // where we'll write the email
const AIRTABLE_CONTACT_FIELD = "Contact";          // name to search in MTEK
const AIRTABLE_CAL_NAME_FIELD = "CAL_NAME";        // optional, just for logs

// Example: "https://bcycle.marianatek.com/api"
const MTEK_BASE_URL = "https://YOUR-STUDIO-SUBDOMAIN.marianatek.com/api";

/**
 * END CONFIG
 * ----------
 * MTEK_API_TOKEN and AIRTABLE_TOKEN must come from GitHub secrets.
 */

const MTEK_API_TOKEN = process.env.MTEK_API_TOKEN;
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

if (!MTEK_API_TOKEN || !AIRTABLE_TOKEN) {
  console.error("Missing required env vars: MTEK_API_TOKEN and/or AIRTABLE_TOKEN.");
  process.exit(1);
}

/**
 * Helper: generic HTTP error check
 */
async function assertOk(response) {
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(
      `HTTP ${response.status} ${response.statusText} - ${response.url}\n` +
      (body ? `Response body: ${body}` : "")
    );
  }
  return response;
}

/**
 * Step 1: Fetch all records from the Airtable view "ADD EMAIL DO NOT TOUCH"
 * (paginated)
 */
async function fetchAirtableRecords() {
  const records = [];
  let offset;

  do {
    const params = new URLSearchParams({
      view: AIRTABLE_VIEW_NAME,
      // If you only want records where EMAIL is empty, uncomment:
      // filterByFormula: `NOT({${AIRTABLE_EMAIL_FIELD}})`
    });

    if (offset) {
      params.append("offset", offset);
    }

    const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(
      AIRTABLE_TABLE_NAME
    )}?${params.toString()}`;

    const res = await assertOk(
      await fetch(url, {
        headers: {
          Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        },
      })
    );

    const json = await res.json();
    if (Array.isArray(json.records)) {
      records.push(...json.records);
    }

    offset = json.offset;
  } while (offset);

  return records;
}

/**
 * Step 2: Query Mariana Tek users by name using ?name_query=...&page_size=1
 * Returns a single email string or null if not found.
 */
async function fetchMtekEmailByName(name) {
  if (!name || typeof name !== "string" || !name.trim()) {
    return null;
  }

  const params = new URLSearchParams({
    name_query: name.trim(), // << important: name_query, not name
    page_size: "1",
  });

  const url = `${MTEK_BASE_URL.replace(/\/$/, "")}/users?${params.toString()}`;

  const res = await assertOk(
    await fetch(url, {
      headers: {
        // Mariana Tek auth style
        Authorization: `Token token="${MTEK_API_TOKEN}"`,
        Accept: "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
      },
    })
  );

  const json = await res.json();

  if (!json || !Array.isArray(json.data) || json.data.length === 0) {
    return null;
  }

  const user = json.data[0];
  const email = user?.attributes?.email || null;

  return email && typeof email === "string" && email.trim() ? email.trim() : null;
}

/**
 * Step 3: Update a single Airtable record's EMAIL field
 */
async function updateAirtableEmail(recordId, email) {
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    AIRTABLE_TABLE_NAME
  )}/${recordId}`;

  const body = {
    fields: {
      [AIRTABLE_EMAIL_FIELD]: email,
    },
  };

  const res = await assertOk(
    await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    })
  );

  return res.json();
}

/**
 * Main worker: loop through records and fill EMAIL if we find a match in MTEK.
 */
async function main() {
  console.log("Fetching Airtable records from view:", AIRTABLE_VIEW_NAME);
  const records = await fetchAirtableRecords();
  console.log(`Found ${records.length} records in view.`);

  let updatedCount = 0;
  let skippedNoContact = 0;
  let skippedNoMatch = 0;

  for (const record of records) {
    const fields = record.fields || {};
    const recordId = record.id;

    const contactName = fields[AIRTABLE_CONTACT_FIELD];
    const calName = fields[AIRTABLE_CAL_NAME_FIELD]; // just for logs

    if (!contactName || typeof contactName !== "string" || !contactName.trim()) {
      skippedNoContact++;
      console.log(
        `[SKIP - no Contact] recordId=${recordId} CAL_NAME=${JSON.stringify(calName)}`
      );
      continue;
    }

    console.log(
      `\n[LOOKUP] recordId=${recordId} CAL_NAME=${JSON.stringify(
        calName
      )} Contact=${JSON.stringify(contactName)}`
    );

    try {
      const email = await fetchMtekEmailByName(contactName);

      if (!email) {
        skippedNoMatch++;
        console.log(
          `[NO MATCH] No user returned from MTEK for Contact=${JSON.stringify(
            contactName
          )}.`
        );
        continue;
      }

      console.log(
        `[UPDATE] recordId=${recordId} Setting EMAIL=${JSON.stringify(email)}`
      );
      await updateAirtableEmail(recordId, email);
      updatedCount++;

      // Mild throttle
      await new Promise((r) => setTimeout(r, 150));

    } catch (err) {
      console.error(
        `[ERROR] recordId=${recordId} Contact=${JSON.stringify(
          contactName
        )} -> ${err.message}`
      );
      // If you prefer fail-fast, you can throw here.
      // throw err;
    }
  }

  console.log("\n--- SUMMARY ---");
  console.log("Total records in view:    ", records.length);
  console.log("Updated with EMAIL:       ", updatedCount);
  console.log("Skipped (no Contact):     ", skippedNoContact);
  console.log("Skipped (no MTEK match):  ", skippedNoMatch);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
