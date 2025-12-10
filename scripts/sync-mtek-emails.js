// scripts/sync-mtek-emails.js

/**
 * CONFIG — EDIT THESE CONSTANTS ONLY
 * ----------------------------------
 */

const AIRTABLE_BASE_ID = "appWPXRyXX3KHoJRI";      // your base
const AIRTABLE_TABLE_NAME = "Ratings";             // table with Contact / CAL_NAME / EMAIL (link field)
const AIRTABLE_VIEW_NAME = "ADD EMAIL DO NOT TOUCH";

const AIRTABLE_EMAIL_LINK_FIELD = "EMAIL";         // linked-record field
const AIRTABLE_CONTACT_FIELD = "Contact";          // name to search in MTEK
const AIRTABLE_CAL_NAME_FIELD = "CAL_NAME";        // optional, logs only

// Linked table (where the contact/email records live)
const LINKED_TABLE_NAME = "Clients";              // <-- CHANGE to your linked table name
const LINKED_EMAIL_FIELD = "Name";                // field in that table that stores the email

// Your MTEK API base
const MTEK_BASE_URL = "https://bcycle.marianatek.com/api";

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
 * Helper: HTTP error check
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
 * Fetch all Ratings records from view "ADD EMAIL DO NOT TOUCH"
 * Only ones where EMAIL link is empty (so we don't touch already linked rows).
 */
async function fetchAirtableRecords() {
  const records = [];
  let offset;

  do {
    const params = new URLSearchParams({
      view: AIRTABLE_VIEW_NAME,
      // only process rows where EMAIL link is empty
      filterByFormula: `NOT({${AIRTABLE_EMAIL_LINK_FIELD}})`
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
 * Query Mariana Tek users by name using ?name_query=...&page_size=1
 * Returns email string or null.
 */
async function fetchMtekEmailByName(name) {
  if (!name || typeof name !== "string" || !name.trim()) {
    return null;
  }

  const params = new URLSearchParams({
    name_query: name.trim(),
    page_size: "1",
  });

  const url = `${MTEK_BASE_URL.replace(/\/$/, "")}/users?${params.toString()}`;

  const res = await assertOk(
    await fetch(url, {
      headers: {
        Authorization: `Bearer ${MTEK_API_TOKEN}`,   // Bearer, as required
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
 * Get OR create a record in the linked table for this email.
 * Returns the linked record ID.
 */
async function getOrCreateLinkedRecordIdForEmail(email) {
  const emailTrimmed = email.trim();

  // Escape single quotes in formula (rare in emails, but just in case)
  const safeEmail = emailTrimmed.replace(/'/g, "''");
  const filterFormula = `LOWER({${LINKED_EMAIL_FIELD}}) = '${safeEmail.toLowerCase()}'`;

  const params = new URLSearchParams({
    filterByFormula: filterFormula,
    maxRecords: "1",
  });

  // 1) Try to find existing
  const listUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    LINKED_TABLE_NAME
  )}?${params.toString()}`;

  const listRes = await assertOk(
    await fetch(listUrl, {
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      },
    })
  );

  const listJson = await listRes.json();
  if (Array.isArray(listJson.records) && listJson.records.length > 0) {
    return listJson.records[0].id;
  }

  // 2) No existing record -> CREATE one
  const createUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    LINKED_TABLE_NAME
  )}`;

  const createBody = {
    fields: {
      [LINKED_EMAIL_FIELD]: emailTrimmed,
    },
  };

  const createRes = await assertOk(
    await fetch(createUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(createBody),
    })
  );

  const createJson = await createRes.json();
  return createJson.id;
}

/**
 * Update the Ratings record's EMAIL (linked-record field) with the linked record ID.
 */
async function updateAirtableEmailLink(recordId, linkedRecordId) {
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    AIRTABLE_TABLE_NAME
  )}/${recordId}`;

  // Ensure we always have a plain string ID
  const id =
    typeof linkedRecordId === "object" && linkedRecordId !== null
      ? linkedRecordId.id
      : linkedRecordId;

  const body = {
    fields: {
      // ✅ CORRECT: array of record ID strings
      [AIRTABLE_EMAIL_LINK_FIELD]: [id],
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
 * Main: loop Ratings records, call MTEK, ensure contact record exists, link it.
 */
async function main() {
  console.log("Fetching Airtable records from view:", AIRTABLE_VIEW_NAME);
  const records = await fetchAirtableRecords();
  console.log(`Found ${records.length} records with empty EMAIL link.`);

  let updatedCount = 0;
  let skippedNoContact = 0;
  let skippedNoMatch = 0;

  for (const record of records) {
    const fields = record.fields || {};
    const recordId = record.id;

    const contactName = fields[AIRTABLE_CONTACT_FIELD];
    const calName = fields[AIRTABLE_CAL_NAME_FIELD];

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
          `[NO MATCH] No MTEK user for Contact=${JSON.stringify(contactName)}`
        );
        continue;
      }

      console.log(`[EMAIL FOUND] ${email} — getting/creating linked contact record…`);
      const linkedRecordId = await getOrCreateLinkedRecordIdForEmail(email);

      console.log(
        `[UPDATE] recordId=${recordId} Setting ${AIRTABLE_EMAIL_LINK_FIELD} -> [${linkedRecordId}]`
      );
      await updateAirtableEmailLink(recordId, linkedRecordId);
      updatedCount++;

      await new Promise((r) => setTimeout(r, 150)); // light throttle
    } catch (err) {
      console.error(
        `[ERROR] recordId=${recordId} Contact=${JSON.stringify(
          contactName
        )} -> ${err.message}`
      );
    }
  }

  console.log("\n--- SUMMARY ---");
  console.log("Total records processed:  ", records.length);
  console.log("Updated linked EMAIL:     ", updatedCount);
  console.log("Skipped (no Contact):     ", skippedNoContact);
  console.log("Skipped (no MTEK match):  ", skippedNoMatch);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
