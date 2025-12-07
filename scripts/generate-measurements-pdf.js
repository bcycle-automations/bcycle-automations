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
const GOOGLE_TEMPLATE_SPREADSHEET_ID = "11P2Yn3VYkmH-tq8pEc-oA2fRQerBlyC7OwBHB82omv4";
const GOOGLE_DESTINATION_FOLDER_ID = "19K3Cvfuxr6Zszjvrr0xRtlEX9V5Bg_M6";
const GOOGLE_SHEET_NAME = "Sheet1";
const CLASS_NAME_CELL = "A1";  // Write class name into A1
const MAX_ROWS = 200;

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

async function getGoogleClients() {
  if (!GOOGLE_SERVICE_ACCOUNT_JSON) {
    throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON env var is empty or not set.");
  }

  console.log(
    "GOOGLE_SERVICE_ACCOUNT_JSON length:",
    GOOGLE_SERVICE_ACCOUNT_JSON.length
  );

  let sa;
  try {
    sa = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
  } catch (e) {
    console.error("Failed to JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON):", e.message);
    throw new Error("Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON.");
  }

  // DEBUG: show what fields exist (NOT the values)
  const keys = Object.keys(sa);
  console.log("Service account JSON keys:", keys);

  console.log("Service account type:", sa.type);
  console.log("Has private_key:", !!sa.private_key);
  console.log("Has client_email:", !!sa.client_email);

  if (!sa.private_key || !sa.client_email || sa.type !== "service_account") {
    throw new Error(
      "Invalid GOOGLE_SERVICE_ACCOUNT_JSON: missing private_key/client_email or not type=service_account."
    );
  }

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

  await jwt.authorize();

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
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Airtable GET ${tableName}/${recordId} failed: ${res.status} ${txt}`);
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
    throw new Error(`Airtable PATCH ${tableName}/${recordId} failed: ${res.status} ${txt}`);
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

// -------------------------------
// MAIN LOGIC
// -------------------------------

async function main() {
  const recordId = process.argv[2];
  if (!recordId) {
    console.error("Usage: node generate-measurements-pdf.js <CLASS_RECORD_ID>");
    process.exit(1);
  }

  console.log("Processing class:", recordId);

  // Auth clients
  const { drive, sheets } = await getGoogleClients();

  // Fetch class record
  const classRecord = await airtableGetRecord(AIRTABLE_CLASSES_TABLE, recordId);
  const cf = classRecord.fields || {};

  const className = cf[CLASS_FIELDS.NAME] || recordId;

  const reservationIds =
    Array.isArray(cf[CLASS_FIELDS.RESERVATIONS]) ? cf[CLASS_FIELDS.RESERVATIONS] : [];

  console.log(`Found ${reservationIds.length} reservations`);

  // Copy template spreadsheet
  const copyResp = await drive.files.copy({
    fileId: GOOGLE_TEMPLATE_SPREADSHEET_ID,
    requestBody: {
      name: `Class Info + Measurements – ${className}`,
      parents: [GOOGLE_DESTINATION_FOLDER_ID],
    },
  });

  const spreadsheetId = copyResp.data.id;
  if (!spreadsheetId) throw new Error("Drive copy failed (no new spreadsheet ID)");

  console.log("New spreadsheet:", spreadsheetId);

  // Write class name into A1 (or wherever you want)
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${GOOGLE_SHEET_NAME}!${CLASS_NAME_CELL}`,
    valueInputOption: "RAW",
    requestBody: { values: [[className]] },
  });

  // Load spot numbers from column A
  const spotRes = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${GOOGLE_SHEET_NAME}!A1:A${MAX_ROWS}`,
  });

  const spotRows = spotRes.data.values || [];
  const spotToRow = {};

  for (let i = 0; i < spotRows.length; i++) {
    const spot = normalizeSpot(spotRows[i][0]);
    if (spot) spotToRow[spot] = i + 1;
  }

  console.log("Mapped spot numbers:", Object.keys(spotToRow).length);

  // Loop through reservations
  for (const resId of reservationIds) {
    const r = await airtableGetRecord(AIRTABLE_CLASS_RES_TABLE, resId);
    const f = r.fields || {};

    const status = (f[RES_FIELDS.STATUS] || "").toLowerCase();

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

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${GOOGLE_SHEET_NAME}!A${rowNumber}:I${rowNumber}`,
      valueInputOption: "RAW",
      requestBody: { values: [row] },
    });

    console.log(`Wrote row ${rowNumber} for reservation ${resId}`);
  }

  // Get webViewLink
  const meta = await drive.files.get({
    fileId: spreadsheetId,
    fields: "webViewLink",
  });

  const webViewLink = meta.data.webViewLink;

  // Construct PDF URL
  const pdfUrl = `https://docs.google.com/spreadsheets/d/${spreadsheetId}/export?format=pdf`;

  // Update Airtable
  await airtableUpdateRecord(AIRTABLE_CLASSES_TABLE, recordId, {
    [CLASS_FIELDS.DOWNLOAD_PDF]: [{ url: pdfUrl }],
    [CLASS_FIELDS.PDF_LINK]: webViewLink,
  });

  console.log("Success! Updated Airtable with PDF + link");
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
