// scripts/generate-measurements-pdf.js
/**
 * Generate class measurements sheet + export PDF + update Airtable
 *
 * Permanent Google auth fix:
 * - Uses GOOGLE_SERVICE_ACCOUNT_JSON (no OAuth refresh tokens, no weekly re-auth)
 *
 * What it does:
 * 1) Fetch class record from Airtable
 * 2) Copy a Google Sheets template into destination Drive folder
 * 3) Write class name into the sheet
 * 4) Map spot numbers (from column A) to row numbers
 * 5) For each reservation (non-cancelled), write measurements into the correct row
 * 6) Export the sheet to PDF (Drive API)
 * 7) Upload PDF back to Drive (in same destination folder)
 * 8) Make the PDF "anyone with link can view"
 * 9) Update Airtable with:
 *    - DOWNLOAD_PDF attachment = public download URL
 *    - PDF LINK = spreadsheet webViewLink
 *
 * Required env:
 * - AIRTABLE_TOKEN
 * - GOOGLE_SERVICE_ACCOUNT_JSON   (entire service account JSON key file contents)
 *
 * Usage:
 *   node scripts/generate-measurements-pdf.js <CLASS_RECORD_ID>
 */

import process from "node:process";
import { google } from "googleapis";

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

// Google Sheets / Drive
const GOOGLE_TEMPLATE_SPREADSHEET_ID =
  "11P2Yn3VYkmH-tq8pEc-oA2fRQerBlyC7OwBHB82omv4";
const GOOGLE_DESTINATION_FOLDER_ID = "19K3Cvfuxr6Zszjvrr0xRtlEX9V5Bg_M6";
const GOOGLE_SHEET_NAME = "Sheet1";

// IMPORTANT: if A1 is used for class name, spot numbers should start below it.
// We will write class name into A1 and map spots from A2 down.
const CLASS_NAME_CELL = "A1";
const SPOT_COL_RANGE_START_ROW = 2;

// Safety bound for reading the template spots
const MAX_ROWS = 250;

// -------------------------------
// SECRETS FROM ENV
// -------------------------------

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;

if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!GOOGLE_SERVICE_ACCOUNT_JSON)
  throw new Error("Missing env: GOOGLE_SERVICE_ACCOUNT_JSON");

// -------------------------------
// GOOGLE AUTH (Service Account)
// -------------------------------

async function getGoogleClients() {
  let creds;
  try {
    creds = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
  } catch {
    throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON");
  }

  const scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
  ];

  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes,
  });

  // sanity check
  await auth.getClient();

  const drive = google.drive({ version: "v3", auth });
  const sheets = google.sheets({ version: "v4", auth });

  return { drive, sheets };
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

      // Retry on common transient issues
      const transient =
        msg.includes("429") ||
        msg.includes("Rate Limit") ||
        msg.includes("quota") ||
        msg.includes("ECONNRESET") ||
        msg.includes("ETIMEDOUT") ||
        msg.includes("503") ||
        msg.includes("500");

      if (!transient || i === tries - 1) throw err;

      const delay = baseDelayMs * Math.pow(2, i);
      console.warn(`Retrying after error (${i + 1}/${tries}): ${msg}`);
      await sleep(delay);
    }
  }
  throw lastErr;
}

// -------------------------------
// PDF EXPORT + UPLOAD HELPERS
// -------------------------------

async function exportSpreadsheetToPdfBuffer(drive, spreadsheetId) {
  // Export the Google Sheet to PDF (authenticated)
  const resp = await withRetry(() =>
    drive.files.export(
      {
        fileId: spreadsheetId,
        mimeType: "application/pdf",
      },
      { responseType: "arraybuffer" }
    )
  );

  // googleapis returns ArrayBuffer-ish data in resp.data
  return Buffer.from(resp.data);
}

async function uploadPdfToDrive(drive, { pdfBuffer, name, parentFolderId }) {
  const createResp = await withRetry(() =>
    drive.files.create({
      requestBody: {
        name,
        parents: [parentFolderId],
        mimeType: "application/pdf",
      },
      media: {
        mimeType: "application/pdf",
        body: pdfBuffer,
      },
      fields: "id, webViewLink, webContentLink",
    })
  );

  const fileId = createResp.data.id;
  if (!fileId) throw new Error("Drive PDF upload failed (no file id)");

  return createResp.data;
}

async function makeAnyoneWithLinkReader(drive, fileId) {
  await withRetry(() =>
    drive.permissions.create({
      fileId,
      requestBody: {
        role: "reader",
        type: "anyone",
      },
    })
  );
}

function driveDirectDownloadUrl(fileId) {
  // Public (when permission is "anyone with link")
  return `https://drive.google.com/uc?export=download&id=${fileId}`;
}

// -------------------------------
// MAIN LOGIC
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

  // Auth clients
  const { drive, sheets } = await getGoogleClients();

  // Fetch class record
  const classRecord = await airtableGetRecord(AIRTABLE_CLASSES_TABLE, recordId);
  const cf = classRecord.fields || {};

  const className =
    (cf[CLASS_FIELDS.NAME] && String(cf[CLASS_FIELDS.NAME]).trim()) || recordId;

  const reservationIds = Array.isArray(cf[CLASS_FIELDS.RESERVATIONS])
    ? cf[CLASS_FIELDS.RESERVATIONS]
    : [];

  console.log(`Found ${reservationIds.length} reservations`);

  // Copy template spreadsheet
  const copyResp = await withRetry(() =>
    drive.files.copy({
      fileId: GOOGLE_TEMPLATE_SPREADSHEET_ID,
      requestBody: {
        name: `Class Info + Measurements - ${className}`,
        parents: [GOOGLE_DESTINATION_FOLDER_ID],
      },
      fields: "id, webViewLink",
    })
  );

  const spreadsheetId = copyResp.data.id;
  if (!spreadsheetId)
    throw new Error("Drive copy failed (no new spreadsheet ID)");

  console.log("New spreadsheet:", spreadsheetId);

  // Write class name into the template
  await withRetry(() =>
    sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${GOOGLE_SHEET_NAME}!${CLASS_NAME_CELL}`,
      valueInputOption: "RAW",
      requestBody: { values: [[className]] },
    })
  );

  // Load spot numbers from column A (starting from row 2 to avoid A1 class name)
  const spotRange = `${GOOGLE_SHEET_NAME}!A${SPOT_COL_RANGE_START_ROW}:A${MAX_ROWS}`;
  const spotRes = await withRetry(() =>
    sheets.spreadsheets.values.get({
      spreadsheetId,
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

    // i=0 corresponds to sheet row SPOT_COL_RANGE_START_ROW
    const sheetRowNumber = SPOT_COL_RANGE_START_ROW + i;
    spotToRow[spot] = sheetRowNumber;
  }

  console.log("Mapped spot numbers:", Object.keys(spotToRow).length);

  // Loop through reservations
  for (const resId of reservationIds) {
    const r = await withRetry(() => airtableGetRecord(AIRTABLE_CLASS_RES_TABLE, resId));
    const f = r.fields || {};

    const status = String(f[RES_FIELDS.STATUS] || "").toLowerCase();

    if (status.includes("cancel")) {
      console.log(`Skipping ${resId} (cancelled)`);
      continue;
    }

    const spot = normalizeSpot(f[RES_FIELDS.SPOT]);
    const rowNumber = spotToRow[spot];

    if (!spot || !rowNumber) {
      console.log(`Skipping ${resId} (invalid or missing spot=${spot})`);
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
        spreadsheetId,
        range: `${GOOGLE_SHEET_NAME}!A${rowNumber}:I${rowNumber}`,
        valueInputOption: "RAW",
        requestBody: { values: [row] },
      })
    );

    console.log(`Wrote row ${rowNumber} for reservation ${resId}`);
  }

  // Get spreadsheet webViewLink (for your "PDF LINK" field)
  const sheetMeta = await withRetry(() =>
    drive.files.get({
      fileId: spreadsheetId,
      fields: "webViewLink",
    })
  );
  const spreadsheetWebViewLink = sheetMeta.data.webViewLink || "";

  // Export spreadsheet to PDF buffer (authenticated)
  console.log("Exporting spreadsheet to PDF...");
  const pdfBuffer = await exportSpreadsheetToPdfBuffer(drive, spreadsheetId);

  // Upload PDF back to Drive (in destination folder)
  const pdfName = `Class Measurements - ${className}.pdf`;
  console.log("Uploading PDF to Drive:", pdfName);

  const pdfFile = await uploadPdfToDrive(drive, {
    pdfBuffer,
    name: pdfName,
    parentFolderId: GOOGLE_DESTINATION_FOLDER_ID,
  });

  const pdfFileId = pdfFile.id;
  if (!pdfFileId) throw new Error("Uploaded PDF missing file id");

  // Make PDF publicly accessible (anyone with link)
  await makeAnyoneWithLinkReader(drive, pdfFileId);

  // Public direct download URL for Airtable attachment
  const pdfDownloadUrl = driveDirectDownloadUrl(pdfFileId);

  // Update Airtable
  await airtableUpdateRecord(AIRTABLE_CLASSES_TABLE, recordId, {
    [CLASS_FIELDS.DOWNLOAD_PDF]: [{ url: pdfDownloadUrl }],
    [CLASS_FIELDS.PDF_LINK]: spreadsheetWebViewLink,
  });

  console.log("Success! Updated Airtable with PDF + sheet link");
  console.log("PDF download:", pdfDownloadUrl);
  if (spreadsheetWebViewLink) console.log("Sheet link:", spreadsheetWebViewLink);
}

main().catch((err) => {
  console.error("Error:", err?.stack || err);
  process.exit(1);
});
