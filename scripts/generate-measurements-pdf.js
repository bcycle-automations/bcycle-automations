// scripts/generate-measurements-pdf.js
import process from "node:process";
import { google } from "googleapis";

// -------------------------------
// NON-SENSITIVE CONFIG
// -------------------------------

// Airtable
const AIRTABLE_BASE_ID = "appofCRTxHoIe6dXI"; // Customer Tracking Tool
const AIRTABLE_CLASSES_TABLE = "CTT SYNC DO NOT TOUCH";
const AIRTABLE_CLASS_RES_TABLE = "Class Reservations";

// Class table fields (CTT SYNC DO NOT TOUCH)
const CLASS_FIELDS = {
  NAME: "Class!",
  RESERVATIONS: "Class Reservations",
  DOWNLOAD_PDF: "Download PDF",
  PDF_LINK: "PDF LINK",
};

// Class Reservations table fields
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
const GOOGLE_TEMPLATE_SPREADSHEET_ID = "11P2Yn3VYkmH-tq8pEc-oA2fRQerBlyC7OwBHB82omv4";
const GOOGLE_DESTINATION_FOLDER_ID = "19K3Cvfuxr6Zszjvrr0xRtlEX9V5Bg_M6";
const GOOGLE_SHEET_NAME = "Sheet1";     // tab name in your template
const MAX_ROWS = 200;                   // how many rows (spots) exist in the template
const CLASS_NAME_CELL = "A1";           // where to write the class name (adjust to match template)

// -------------------------------
// SECRETS FROM ENV
// -------------------------------

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;

if (!AIRTABLE_TOKEN) throw new Error("Missing env: AIRTABLE_TOKEN");
if (!GOOGLE_SERVICE_ACCOUNT_JSON) throw new Error("Missing env: GOOGLE_SERVICE_ACCOUNT_JSON");

// -------------------------------
// GOOGLE AUTH
// -------------------------------

function getGoogleClients() {
  const sa = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);

  const scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
  ];

  const jwt = new google.auth.JWT(
    sa.client_email,
    null,
    sa.private_key,
    scopes
  );

  const drive = google.drive({ version: "v3", auth: jwt });
  const sheets = google.sheets({ version: "v4", auth: jwt });

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
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Airtable GET ${tableName}/${recordId} failed: ${res.status} ${text}`
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
    const text = await res.text();
    throw new Error(
      `Airtable PATCH ${tableName}/${recordId} failed: ${res.status} ${text}`
    );
  }

  return res.json();
}

// -------------------------------
// UTILITIES
// -------------------------------

function firstLookup(value) {
  if (Array.isArray(value)) {
    return value[0] ?? "";
  }
  return value ?? "";
}

function normalizeSpot(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

// -------------------------------
// CORE LOGIC
// -------------------------------

async function main() {
  const recordId = process.argv[2];
  if (!recordId) {
    console.error(
      "Usage: node scripts/generate-measurements-pdf.js <CLASS_RECORD_ID>"
    );
    process.exit(1);
  }

  console.log(`Generating measurements PDF for class record: ${recordId}`);

  const { drive, sheets } = getGoogleClients();

  // 1) Fetch the Class record
  const classRecord = await airtableGetRecord(AIRTABLE_CLASSES_TABLE, recordId);
  const classFields = classRecord.fields || {};

  const className =
    classFields[CLASS_FIELDS.NAME] && String(classFields[CLASS_FIELDS.NAME]).trim().length
      ? String(classFields[CLASS_FIELDS.NAME]).trim()
      : recordId;

  const reservationIds =
    classFields[CLASS_FIELDS.RESERVATIONS] && Array.isArray(classFields[CLASS_FIELDS.RESERVATIONS])
      ? classFields[CLASS_FIELDS.RESERVATIONS]
      : [];

  console.log(
    `Class name: ${className} | Reservations linked: ${reservationIds.length}`
  );

  // 2) Copy the template to a new spreadsheet in the destination folder
  const copyResp = await drive.files.copy({
    fileId: GOOGLE_TEMPLATE_SPREADSHEET_ID,
    requestBody: {
      name: `Class Info + Measurements - ${className}`,
      parents: [GOOGLE_DESTINATION_FOLDER_ID],
    },
  });

  const spreadsheetId = copyResp.data.id;
  if (!spreadsheetId) {
    throw new Error("Failed to create spreadsheet from template (no ID)");
  }

  console.log(`Created spreadsheet: ${spreadsheetId}`);

  // 3) Write the Class name into the template (optional; adjust CLASS_NAME_CELL as needed)
  try {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${GOOGLE_SHEET_NAME}!${CLASS_NAME_CELL}`,
      valueInputOption: "RAW",
      requestBody: { values: [[className]] },
    });
    console.log(`Wrote class name into ${GOOGLE_SHEET_NAME}!${CLASS_NAME_CELL}`);
  } catch (err) {
    console.warn(
      `Warning: failed to write class name into sheet (${GOOGLE_SHEET_NAME}!${CLASS_NAME_CELL}):`,
      err.message
    );
  }

  // 4) Build a map of Spot -> rowNumber from column A in the template
  const spotsResp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${GOOGLE_SHEET_NAME}!A1:A${MAX_ROWS}`,
  });

  const spotRows = spotsResp.data.values || [];
  const spotToRow = {};

  for (let i = 0; i < spotRows.length; i += 1) {
    const row = spotRows[i];
    if (!row || row.length === 0) continue;
    const val = normalizeSpot(row[0]);
    if (!val) continue;
    const rowNumber = i + 1; // Sheets rows are 1-based
    spotToRow[val] = rowNumber;
  }

  console.log(
    `Indexed ${Object.keys(spotToRow).length} spot rows from template (${GOOGLE_SHEET_NAME})`
  );

  // 5) Iterate reservations and update rows
  for (const resId of reservationIds) {
    console.log(`Processing reservation: ${resId}`);
    const resRecord = await airtableGetRecord(AIRTABLE_CLASS_RES_TABLE, resId);
    const f = resRecord.fields || {};

    const statusRaw = f[RES_FIELDS.STATUS] ?? "";
    const status = String(statusRaw).toLowerCase();

    // Skip cancelled reservations (anything that contains "cancel")
    if (status.includes("cancel")) {
      console.log(`  Skipping reservation ${resId} (status: ${statusRaw})`);
      continue;
    }

    const spotRaw = f[RES_FIELDS.SPOT];
    const spot = normalizeSpot(spotRaw);

    if (!spot) {
      console.log(`  Skipping reservation ${resId} (no spot number)`);
      continue;
    }

    const rowNumber = spotToRow[spot];
    if (!rowNumber) {
      console.log(
        `  Skipping reservation ${resId} (spot ${spot} not found in template)`
      );
      continue;
    }

    const name = firstLookup(f[RES_FIELDS.NAME]);
    const shoe = firstLookup(f[RES_FIELDS.SHOE]);
    const seatHeight = firstLookup(f[RES_FIELDS.SEAT_HEIGHT]);
    const seatPos = firstLookup(f[RES_FIELDS.SEAT_POS]);
    const hbHeight = firstLookup(f[RES_FIELDS.HB_HEIGHT]);
    const hbPos = firstLookup(f[RES_FIELDS.HB_POS]);
    const change = f[RES_FIELDS.CHANGE] ?? "";
    const notes = firstLookup(f[RES_FIELDS.NOTES]);

    const rowValues = [
      spot,
      name,
      shoe,
      seatHeight,
      seatPos,
      hbHeight,
      hbPos,
      change,
      notes,
    ];

    console.log(
      `  Writing to row ${rowNumber} (spot ${spot}): ${JSON.stringify(
        rowValues
      )}`
    );

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${GOOGLE_SHEET_NAME}!A${rowNumber}:I${rowNumber}`,
      valueInputOption: "RAW",
      requestBody: { values: [rowValues] },
    });
  }

  // 6) Get the webViewLink for the sheet
  const fileMetaResp = await drive.files.get({
    fileId: spreadsheetId,
    fields: "webViewLink",
  });

  const webViewLink = fileMetaResp.data.webViewLink || "";
  console.log(`webViewLink: ${webViewLink}`);

  // 7) Build a PDF download URL
  // IMPORTANT: For this to work without auth, the destination folder should be
  // "Anyone with the link can view" so Airtable can fetch the PDF.
  const pdfUrl = `https://docs.google.com/spreadsheets/d/${spreadsheetId}/export?format=pdf`;

  console.log(`PDF URL: ${pdfUrl}`);

  // 8) Update the Class record in Airtable with Download PDF + PDF LINK
  const updateFields = {
    [CLASS_FIELDS.DOWNLOAD_PDF]: [{ url: pdfUrl }],
    [CLASS_FIELDS.PDF_LINK]: webViewLink,
  };

  await airtableUpdateRecord(AIRTABLE_CLASSES_TABLE, recordId, updateFields);

  console.log("Updated Airtable class record with PDF link + view link");
  console.log("Done ✅");
}

// Run
main().catch((err) => {
  console.error("Error generating measurements PDF:", err);
  process.exit(1);
});