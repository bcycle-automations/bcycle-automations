// scripts/test-meta-offline-conversion.mjs
// Exploratory test: sends one dummy Purchase event to the "b.cycle Offline
// Conversions" dataset via Meta's Conversions API, tagged with a
// test_event_code so it shows up in Events Manager > Test events without
// touching real ad optimization data. Not wired into any pipeline.

import { createHash } from "node:crypto";

const META_TOKEN = (process.env.META_OFFLINE_CONVERSIONS_TOKEN || "").trim();
const DATASET_ID = (process.env.META_OFFLINE_DATASET_ID || "").trim();
const TEST_EVENT_CODE = process.env.TEST_EVENT_CODE || "TEST64748";
const GRAPH_VERSION = "v19.0";

function sha256(value) {
  return createHash("sha256").update(value.trim().toLowerCase()).digest("hex");
}

async function main() {
  if (!META_TOKEN) throw new Error("Missing META_OFFLINE_CONVERSIONS_TOKEN (set it in .env)");
  if (!DATASET_ID) throw new Error("Missing META_OFFLINE_DATASET_ID (set it in .env)");

  // Sample transaction pulled from the 2026-08-04 MTEK test fetch.
  const sample = {
    email: "sebastienhc@gmail.com",
    firstName: "Sébastien",
    lastName: "Harrison Cloutier",
    value: 17.25,
    currency: "CAD",
    transactionDateUTC: "2026-08-04T00:22:15Z",
  };

  const eventTimeUnix = Math.floor(new Date(sample.transactionDateUTC).getTime() / 1000);

  const payload = {
    data: [
      {
        event_name: "Purchase",
        event_time: eventTimeUnix,
        user_data: {
          em: [sha256(sample.email)],
          fn: [sha256(sample.firstName)],
          ln: [sha256(sample.lastName)],
        },
        custom_data: {
          value: sample.value,
          currency: sample.currency,
        },
      },
    ],
    test_event_code: TEST_EVENT_CODE,
    access_token: META_TOKEN,
  };

  console.log(`Posting to dataset ${DATASET_ID} with test_event_code=${TEST_EVENT_CODE}...`);

  const url = `https://graph.facebook.com/${GRAPH_VERSION}/${DATASET_ID}/events`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const json = await res.json();
  console.log(`HTTP ${res.status}`);
  console.log(JSON.stringify(json, null, 2));

  if (!res.ok) {
    throw new Error(`Meta API returned an error (see above)`);
  }
}

main().catch((err) => {
  console.error("Test failed:", err.message);
  process.exit(1);
});
