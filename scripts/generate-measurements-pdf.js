// scripts/generate-measurements-pdf.js
import process from "node:process";
import { google } from "googleapis";

/**
 * OPTION 3 (No Drive file creation, keep WORKING SHEET hardcoded):
 * - Re-uses ONE existing Google Sheet ("working sheet") every run.
 * - Service account edits the sheet (Sheets API only).
 * - PDF is provided as a PUBLIC export URL (so Airtable can fetch it).
 * - Optional cleanup clears data after updating Airtable (keeps spots in col A).
 *
 * IMPORTANT REQUIREMENTS (do once in Google Drive):
 * 1) Share the working sheet to the service account email as Editor
 * 2) Set the working sheet "General access" to "Anyone with the link" as Viewer
 *
 * Usage:
 *   node scripts/generate-measurements-pdf.js <CLASS_RECORD_ID>
 */

// -------------------------------
// NON-SENSITIVE CONFIG
// -------------------------------

// Airtable
const AIRTABLE_BASE_ID = "appofCRTxHoIe6dXI"; // Customer Tracking Tool
const AIRTABLE_CLASSES_TABLE = "CTT SYNC DO NOT TOUCH";
const AIRTABLE_CLASS_RES_TABLE = "Class Reservations";

// Class table fields
const CLASS_FIELDS = {
  NAME: "Class!",
  RESERVATIONS: "Class Reservations",
  DOWNLOAD_PDF: "Download PDF",
  PDF_LINK: "PDF LINK",
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

// Google Sheets (WORKING SHEET — hardcoded)
const WORKING_SPREADSHEET_ID = "PASTE_WORKING_SPREADSHEET_ID_HERE";
const GOOGLE_SHEET_NAME = "Sheet1";

// Template layout assumptions:
// - A1 = class name (we overwrite)
// - A2:A{MAX_ROWS} = spot numbers pre-filled
// - We write A:I on the row for each spot
const CLASS_NAME_CELL = "A1";
const SPOT_COL_RANGE_START_ROW = 2;
const MAX_ROWS = 250;

// Cleanup:
// - clears A1 and B:I rows 2..MAX_ROWS (keeps spot numbers in col A)
const CLEANUP_AFTER_EXPORT = true;

// -------------------------------
// SECRETS FROM ENV
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
// GOOGLE AUTH (Service Account - Sheets only)
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

  // sanity check
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

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(
      `Airtable PATCH ${tableName}/${recordId} failed: ${res.status} ${txt}`
    );
  }

  return res.json();
}

// -------------------------------
// UTILITIES
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

// Public PDF export URL
// NOTE: Works only if the sheet is "Anyone with the link" viewable.
function publicPdfExportUrl(spreadsheetId) {
  const params = new URLSearchParams({
    format: "pdf",
    portrait: "true",
    fitw: "true",
    sheetnames: "false",
    printtitle: "false",
    pagenumbers: "false",
    gridlines: "false",
    fzr: "false",
  });

  return `https://docs.google.com/spreadsheets/d/${spreadsheetId}/export?${params.toString()}`;
}

function publicSheetLink(spreadsheetId) {
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

  // Fetch class record
  const classRecord = await airtableGetRecord(AIRTABLE_CLASSES_TABLE, recordId);
  const cf = classRecord.fields || {};

  const className =
    (cf[CLASS_FIELDS.NAME] && String(cf[CLASS_FIELDS.NAME]).trim()) || recordId;

  const reservationIds = Array.isArray(cf[CLASS_FIELDS.RESERVATIONS])
    ? cf[CLASS_FIELDS.RESERVATIONS]
    : [];

  console.log(`Found ${reservationIds.length} reservations`);

  // Write class name into A1
  await withRetry(() =>
    sheets.spreadsheets.values.update({
      spreadsheetId: WORKING_SPREADSHEET_ID,
      range: `${GOOGLE_SHEET_NAME}!${CLASS_NAME_CELL}`,
      valueInputOption: "RAW",
      requestBody: { values: [[className]] },
    })
  );

  // Map spot numbers -> row numbers using column A (starting at row 2)
  const spotRange = `${GOOGLE_SHEET_NAME}!A${SPOT_COL_RANGE_START_ROW}:A${MAX_ROWS}`;
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

    // i=0 corresponds to sheet row 2
    spotToRow[spot] = SPOT_COL_RANGE_START_ROW + i;
  }

  console.log("Mapped spot numbers:", Object.keys(spotToRow).length);

  // Fill rows for each reservation
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

  // Build links for Airtable
  const pdfUrl = publicPdfExportUrl(WORKING_SPREADSHEET_ID);
  const sheetUrl = publicSheetLink(WORKING_SPREADSHEET_ID);

  // Update Airtable
  await airtableUpdateRecord(AIRTABLE_CLASSES_TABLE, recordId, {
    [CLASS_FIELDS.DOWNLOAD_PDF]: [{ url: pdfUrl }],
    [CLASS_FIELDS.PDF_LINK]: sheetUrl,
  });

  console.log("Updated Airtable with PDF + sheet link");
  console.log("PDF:", pdfUrl);

  // Optional cleanup
  if (CLEANUP_AFTER_EXPORT) {
    console.log("Cleaning up working sheet...");

    // Clear A1 (class name)
    await withRetry(() =>
      sheets.spreadsheets.values.clear({
        spreadsheetId: WORKING_SPREADSHEET_ID,
        range: `${GOOGLE_SHEET_NAME}!A1`,
      })
    );

    // Clear B:I (keep spot numbers in col A)
    await withRetry(() =>
      sheets.spreadsheets.values.clear({
        spreadsheetId: WORKING_SPREADSHEET_ID,
        range: `${GOOGLE_SHEET_NAME}!B${SPOT_COL_RANGE_START_ROW}:I${MAX_ROWS}`,
      })
    );

    console.log("Cleanup done.");
  }

  console.log("Success!");
}

main().catch((err) => {
  console.error("Error:", err?.stack || err);
  process.exit(1);
});
