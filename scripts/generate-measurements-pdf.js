// scripts/generate-measurements-pdf.js
import process from "node:process";
import { google } from "googleapis";

/**
 * OPTION 2 (single working sheet + live PDF export link)
 *
 * What it does:
 * 1) Clears previous run data at the START (A1 and B4:I{MAX_ROWS})
 * 2) Reads the Airtable class + linked reservations
 * 3) Writes rows into the WORKING spreadsheet (no Drive file creation)
 * 4) Updates Airtable:
 *    - PDF LINK (text) = sheet edit link
 *    - Download PDF (attachment) = simple PDF export URL (most compatible with Airtable fetcher)
 *
 * IMPORTANT tradeoff:
 * - The "PDF" is a LIVE export of the working sheet, not a snapshot.
 * - If the working sheet is overwritten later, old Airtable records will point to the new content.
 *
 * One-time Google Drive requirements:
 * - Share the working spreadsheet to the service account as Editor
 * - Set working spreadsheet "General access" to "Anyone with the link" = Viewer (so Airtable can fetch)
 *
 * Usage:
 *   node scripts/generate-measurements-pdf.js <CLASS_RECORD_ID>
 */

// -------------------------------
// CONFIG
// -------------------------------

// Airtable
const AIRTABLE_BASE_ID = "appofCRTxHoIe6dXI"; // Customer Tracking Tool
const AIRTABLE_CLASSES_TABLE = "CTT SYNC DO NOT TOUCH";
const AIRTABLE_CLASS_RES_TABLE = "Class Reservations";

// Class table fields
const CLASS_FIELDS = {
  NAME: "Class!",
  RESERVATIONS: "Class Reservations",
  DOWNLOAD_PDF: "Download PDF", // Attachment field
  PDF_LINK: "PDF LINK", // URL/text field
};

// Reservation table fields
const RES_FIELDS = {
  STATUS: "Status",
  SPOT: "Spot number",
  NAME: "Name (from Customer)",
  SHOE: "Shoe Size (from Customer)",
  SEAT_HEIGHT: "Seat height (from Customer)",
  SEAT_POS: "Seat position (from Customer)",
  HB_HEIGHT: "Handlebar height (from Customer)",
  HB_POS: "Handlebar position (from Customer)",
  CHANGE: "CHANGE SINCE LAST UPDATE?",
  NOTES: "Class NOTES (from Customer)",
};

// Google Sheets (working sheet only — hardcoded)
const WORKING_SPREADSHEET_ID = "11P2Yn3VYkmH-tq8pEc-oA2fRQerBlyC7OwBHB82omv4";
const GOOGLE_SHEET_NAME = "Sheet1";

// Layout assumptions for YOUR sheet:
// - A1: class name
// - Spots are in column A starting at row 4
// - Data should be written across A:I on each spot row
const CLASS_NAME_CELL = "A1";
const DATA_START_ROW = 4; // you said your sheet’s data begins at B4
const MAX_ROWS = 250;

// -------------------------------
// SECRETS
// -------------------------------

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;

if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!GOOGLE_SERVICE_ACCOUNT_JSON)
  throw new Error("Missing env: GOOGLE_SERVICE_ACCOUNT_JSON");
if (!WORKING_SPREADSHEET_ID || WORKING_SPREADSHEET_ID.includes("PASTE_")) {
  throw new Error(
    "Missing config: WORKING_SPREADSHEET_ID (paste the ID into the script)"
  );
}

// -------------------------------
// GOOGLE (Sheets only)
// -------------------------------

async function getSheetsClient() {
  let creds;
  try {
    creds = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
  } catch {
    throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON");
  }

  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  await auth.getClient();
  return google.sheets({ version: "v4", auth });
}

// -------------------------------
// AIRTABLE HELPERS
// -------------------------------

const AIRTABLE_API_BASE = "https://api.airtable.com/v0";

async function airtableGetRecord(tableName, recordId) {
  const url = `${AIRTABLE_API_BASE}/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    tableName
  )}/${recordId}`;

  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(
      `Airtable GET ${tableName}/${recordId} failed: ${res.status} ${txt}`
    );
  }

  return res.json();
}

async function airtableUpdateRecord(tableName, recordId, fields) {
  const url = `${AIRTABLE_API_BASE}/${AIRTABLE_BASE_ID}/${encodeURIComponent(
    tableName
  )}/${recordId}`;

  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });

  const txt = await res.text();
  if (!res.ok) {
    throw new Error(`Airtable PATCH ${tableName}/${recordId} failed: ${res.status} ${txt}`);
  }

  try {
    return JSON.parse(txt);
  } catch {
    // Extremely rare, but keep it debuggable
    return { raw: txt };
  }
}

// -------------------------------
// UTIL
// -------------------------------

function firstLookup(val) {
  return Array.isArray(val) ? val[0] ?? "" : val ?? "";
}

function normalizeSpot(val) {
  if (val == null) return "";
  return String(val).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function withRetry(fn, { tries = 5, baseDelayMs = 400 } = {}) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const msg = String(err?.message || err);

      const transient =
        msg.includes("429") ||
        msg.toLowerCase().includes("rate limit") ||
        msg.toLowerCase().includes("quota") ||
        msg.includes("ECONNRESET") ||
        msg.includes("ETIMEDOUT") ||
        msg.includes("503") ||
        msg.includes("500");

      if (!transient || i === tries - 1) throw err;

      const delay = baseDelayMs * Math.pow(2, i);
      console.warn(`Retrying (${i + 1}/${tries}) after: ${msg}`);
      await sleep(delay);
    }
  }
  throw lastErr;
}

// Airtable attachment fetcher tends to behave better with the simplest URL:
function pdfExportUrlSimple(spreadsheetId) {
  return `https://docs.google.com/spreadsheets/d/${spreadsheetId}/export?format=pdf`;
}

function sheetEditLink(spreadsheetId) {
  return `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit`;
}

// -------------------------------
// MAIN
// -------------------------------

async function main() {
  const recordId = process.argv[2];
  if (!recordId) {
    console.error(
      "Usage: node scripts/generate-measurements-pdf.js <CLASS_RECORD_ID>"
    );
    process.exit(1);
  }

  console.log("Processing class:", recordId);

  const sheets = await getSheetsClient();

  // 1) CLEAR PREVIOUS RUN AT START (A1 and B4:I...)
  console.log("Clearing previous run data (start of run)...");
  await withRetry(() =>
    sheets.spreadsheets.values.clear({
      spreadsheetId: WORKING_SPREADSHEET_ID,
      range: `${GOOGLE_SHEET_NAME}!${CLASS_NAME_CELL}`,
    })
  );

  await withRetry(() =>
    sheets.spreadsheets.values.clear({
      spreadsheetId: WORKING_SPREADSHEET_ID,
      range: `${GOOGLE_SHEET_NAME}!B${DATA_START_ROW}:I${MAX_ROWS}`,
    })
  );

  // 2) Fetch class record
  const classRecord = await airtableGetRecord(AIRTABLE_CLASSES_TABLE, recordId);
  const cf = classRecord.fields || {};

  const className =
    (cf[CLASS_FIELDS.NAME] && String(cf[CLASS_FIELDS.NAME]).trim()) || recordId;

  const reservationIds = Array.isArray(cf[CLASS_FIELDS.RESERVATIONS])
    ? cf[CLASS_FIELDS.RESERVATIONS]
    : [];

  console.log(`Found ${reservationIds.length} reservations`);

  // 3) Write class name
  await withRetry(() =>
    sheets.spreadsheets.values.update({
      spreadsheetId: WORKING_SPREADSHEET_ID,
      range: `${GOOGLE_SHEET_NAME}!${CLASS_NAME_CELL}`,
      valueInputOption: "RAW",
      requestBody: { values: [[className]] },
    })
  );

  // 4) Map spot numbers -> rows from column A starting at row 4
  const spotRange = `${GOOGLE_SHEET_NAME}!A${DATA_START_ROW}:A${MAX_ROWS}`;
  const spotRes = await withRetry(() =>
    sheets.spreadsheets.values.get({
      spreadsheetId: WORKING_SPREADSHEET_ID,
      range: spotRange,
    })
  );

  const spotRows = spotRes.data.values || [];
  const spotToRow = {};

  for (let i = 0; i < spotRows.length; i++) {
    const row = spotRows[i];
    if (!row || row.length === 0) continue;
    const spot = normalizeSpot(row[0]);
    if (!spot) continue;
    spotToRow[spot] = DATA_START_ROW + i;
  }

  console.log("Mapped spot numbers:", Object.keys(spotToRow).length);

  // 5) Loop reservations and write rows
  for (const resId of reservationIds) {
    const r = await withRetry(() =>
      airtableGetRecord(AIRTABLE_CLASS_RES_TABLE, resId)
    );
    const f = r.fields || {};

    const status = String(f[RES_FIELDS.STATUS] || "").toLowerCase();
    if (status.includes("cancel")) {
      console.log(`Skipping ${resId} (cancelled)`);
      continue;
    }

    const spot = normalizeSpot(f[RES_FIELDS.SPOT]);
    const rowNumber = spotToRow[spot];

    if (!spot || !rowNumber) {
      console.log(`Skipping ${resId} (invalid/missing spot=${spot})`);
      continue;
    }

    const row = [
      spot,
      firstLookup(f[RES_FIELDS.NAME]),
      firstLookup(f[RES_FIELDS.SHOE]),
      firstLookup(f[RES_FIELDS.SEAT_HEIGHT]),
      firstLookup(f[RES_FIELDS.SEAT_POS]),
      firstLookup(f[RES_FIELDS.HB_HEIGHT]),
      firstLookup(f[RES_FIELDS.HB_POS]),
      f[RES_FIELDS.CHANGE] ?? "",
      firstLookup(f[RES_FIELDS.NOTES]),
    ];

    await withRetry(() =>
      sheets.spreadsheets.values.update({
        spreadsheetId: WORKING_SPREADSHEET_ID,
        range: `${GOOGLE_SHEET_NAME}!A${rowNumber}:I${rowNumber}`,
        valueInputOption: "RAW",
        requestBody: { values: [row] },
      })
    );

    console.log(`Wrote row ${rowNumber} for reservation ${resId}`);
  }

  // 6) Update Airtable with links
  const pdfUrl = pdfExportUrlSimple(WORKING_SPREADSHEET_ID);
  const sheetUrl = sheetEditLink(WORKING_SPREADSHEET_ID);

  console.log("Updating Airtable...");
  const updated = await airtableUpdateRecord(AIRTABLE_CLASSES_TABLE, recordId, {
    [CLASS_FIELDS.DOWNLOAD_PDF]: [{ url: pdfUrl }],
    [CLASS_FIELDS.PDF_LINK]: sheetUrl,
  });

  // Debug visibility: show what Airtable says it stored
  const downloadField = updated?.fields?.[CLASS_FIELDS.DOWNLOAD_PDF];
  console.log("Airtable returned Download PDF:", downloadField);
  console.log("PDF export URL:", pdfUrl);
  console.log("Done (no end-of-run cleanup).");
}

main().catch((err) => {
  console.error("Error:", err?.stack || err);
  process.exit(1);
});
