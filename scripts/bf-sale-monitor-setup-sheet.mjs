// scripts/bf-sale-monitor-setup-sheet.mjs
// One-off (re-runnable) builder for the "BF 2026 Sale Monitor" Google Sheet:
// creates the Settings / Dashboard / Sales_Raw / Customers_Raw / Import_Log
// tabs, every dashboard number as a formula over the two raw tabs, and the
// formatting. Does NOT touch raw data rows. The sheet itself is created by hand
// (or Drive) and shared with the service account; pass its id as SHEET_ID.
//
//   GOOGLE_APPLICATION_CREDENTIALS=.gcp-bq-key.json node scripts/bf-sale-monitor-setup-sheet.mjs

import { google } from "googleapis";

const SHEET_ID = process.env.SHEET_ID || "1QzHukVUupfa-BkzJOrdkKNipGc0Wfx2dsaaBoAajVOc";

const auth = new google.auth.GoogleAuth({ scopes: ["https://www.googleapis.com/auth/spreadsheets"] });
const sheets = google.sheets({ version: "v4", auth });

const C = "Customers_Raw";
const S = "Sales_Raw";
const cr = (col) => `${C}!$${col}$2:$${col}$5000`;
const sr = (col) => `${S}!$${col}$2:$${col}$10000`;

const SEGMENTS = [
  ["Member", "Cannibalization - already pays monthly"],
  ["New", "Incremental - first-timer"],
  ["Active pack buyer", "Cannibalization - pull-forward of a pack they'd buy anyway"],
  ["Active - own credits", "Neutral - attending on credits already owned"],
  ["Inactive 90d+", "Incremental - reactivated"],
];

const PRODUCTS = [
  ["Prévente Vendredi fou – 10 cours achetés, 5 offerts", 10, 5, 270],
  ["Prévente Vendredi fou – 20 cours achetés, 10 offerts", 20, 10, 520],
  ["Vendredi Fou – 5 cours achetés, 1 offert", 5, 1, 140],
  ["Vendredi Fou – 10 cours achetés, 2 offerts", 10, 2, 270],
  ["Vendredi Fou – 20 cours achetés, 5 offerts", 20, 5, 520],
];
const PRODUCT_ROWS = 8; // Settings rows 12-19

async function main() {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const existing = new Map(meta.data.sheets.map((s) => [s.properties.title, s.properties.sheetId]));
  const wanted = ["Dashboard", "Settings", "Sales_Raw", "Customers_Raw", "Import_Log"];
  const reqs = [];
  if (existing.has("Sheet1") && !existing.has("Dashboard")) {
    reqs.push({ updateSheetProperties: { properties: { sheetId: existing.get("Sheet1"), title: "Dashboard" }, fields: "title" } });
    existing.set("Dashboard", existing.get("Sheet1"));
    existing.delete("Sheet1");
  }
  for (const t of wanted) if (!existing.has(t)) reqs.push({ addSheet: { properties: { title: t } } });
  reqs.push({ updateSpreadsheetProperties: { properties: { timeZone: "America/Toronto" }, fields: "timeZone" } });
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: reqs } });

  const meta2 = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const id = Object.fromEntries(meta2.data.sheets.map((s) => [s.properties.title, s.properties.sheetId]));

  const data = [];
  const put = (range, values) => data.push({ range, values });

  // ---------------------------------------------------------------- Settings
  put("Settings!A1", [["Settings - blue cells are editable; the Dashboard reads everything from here"]]);
  put("Settings!A3:C8", [
    ["First date shown in the Dashboard daily table", "2026-11-24", ""],
    ["Minimum buyers before the verdict is trusted", 40, "Below this the verdict says WAIT"],
    ["Max % of buyers who are Members", 0.3, "Members buying packs = revenue we'd likely have had anyway"],
    ["Max % of buyers who are Active pack buyers", 0.3, "Bought a pack in the prior 90 days = pull-forward"],
    ["Max % of buyers Members + Active pack buyers combined", 0.5, "Combined cannibalization ceiling"],
    ["Min % of buyers New + Inactive 90d+ (incremental)", 0.25, "Floor of genuinely new / reactivated money"],
  ]);
  put("Settings!A11:D11", [["Product (exact MTEK name)", "Paid classes", "Free classes", "Pre-tax price"]]);
  const prodRows = [];
  for (let i = 0; i < PRODUCT_ROWS; i++) prodRows.push(PRODUCTS[i] || ["", "", "", ""]);
  put(`Settings!A12:D${11 + PRODUCT_ROWS}`, prodRows);
  put("Settings!A21", [["Add a new pack by typing its exact MTEK product name in the next free row (the import only pulls products whose name contains 'offert')."]]);

  // --------------------------------------------------------------- Import log
  put("Import_Log!D1", [["Written by scripts/bf-sale-monitor-import.mjs on every run - do not edit."]]);

  // ------------------------------------------------------------ Raw helpers
  put(`${S}!S1:T1`, [["segment", "order_ts_utc"]]);
  put(`${S}!S2`, [[`=ARRAYFORMULA(IF(E2:E="","",IFERROR(VLOOKUP(E2:E,${C}!A:D,4,FALSE),"Unknown")))`]]);
  put(`${S}!T2`, [[`=ARRAYFORMULA(IF(B2:B="","",B2:B+C2:C))`]]);

  // --------------------------------------------------------------- Dashboard
  const D = [];
  const set = (cell, v) => D.push({ range: `Dashboard!${cell}`, values: [[v]] });
  set("A1", "BF 2026 - Vendredi Fou class packs: keep the sale on?");
  set("A2", "Last import (UTC)");
  set("B2", "=Import_Log!B1");
  set("A4", "VERDICT");
  set(
    "B4",
    `=IF(B7<D7,"WAIT - only "&B7&" buyers so far (need "&D7&")",IF(COUNTIF(E8:E11,"OK")=4,"KEEP THE SALE ACTIVE",IF(COUNTIF(E8:E11,"OK")>=2,"WATCH CLOSELY - "&(4-COUNTIF(E8:E11,"OK"))&" check(s) failing","PAUSE / REVIEW - "&(4-COUNTIF(E8:E11,"OK"))&" checks failing")))`
  );
  D.push({ range: "Dashboard!A6:E6", values: [["Decision checks", "Actual", "Rule", "Threshold", "Status"]] });
  D.push({
    range: "Dashboard!A7:E12",
    values: [
      ["Distinct buyers", "=B21", "at least", "=Settings!B4", '=IF(B7>=D7,"OK","TOO FEW")'],
      ["% of buyers who are Members", "=C16", "at most", "=Settings!B5", '=IF(B8<=D8,"OK","OVER")'],
      ["% of buyers who are Active pack buyers", "=C18", "at most", "=Settings!B6", '=IF(B9<=D9,"OK","OVER")'],
      ["% Members + Active pack buyers", "=C16+C18", "at most", "=Settings!B7", '=IF(B10<=D10,"OK","OVER")'],
      ["% New + Inactive 90d+ (incremental)", "=C17+C20", "at least", "=Settings!B8", '=IF(B11>=D11,"OK","LOW")'],
      ["Buyers without a segment", `=COUNTA(${cr("A")})-B21`, "should be", 0, '=IF(B12=D12,"OK","CHECK")'],
    ],
  });
  D.push({
    range: "Dashboard!G6:H11",
    values: [
      ["Last 24 hours (to last import)", ""],
      ["Units sold", `=SUMIFS(${sr("L")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed")`],
      ["Units to Members", `=SUMIFS(${sr("L")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed",${sr("S")},"Member")`],
      ["Units to Active pack buyers", `=SUMIFS(${sr("L")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed",${sr("S")},"Active pack buyer")`],
      ["% cannibalizing units", "=IFERROR((H8+H9)/H7,0)"],
      ["Revenue (pre-tax)", `=SUMIFS(${sr("M")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed")`],
    ],
  });

  D.push({
    range: "Dashboard!A15:J15",
    values: [["Segment (prior 90d, before first sale purchase)", "Buyers", "% of buyers", "Units", "% of units", "Revenue (pre-tax)", "% of revenue", "Avg prior-90d spend", "Avg classes attended prior 90d", "Read as"]],
  });
  SEGMENTS.forEach(([seg, note], i) => {
    const r = 16 + i;
    D.push({
      range: `Dashboard!A${r}:J${r}`,
      values: [[
        seg,
        `=COUNTIFS(${cr("D")},$A${r})`,
        `=IFERROR(B${r}/B$21,0)`,
        `=SUMIFS(${cr("F")},${cr("D")},$A${r})`,
        `=IFERROR(D${r}/D$21,0)`,
        `=SUMIFS(${cr("G")},${cr("D")},$A${r})`,
        `=IFERROR(F${r}/F$21,0)`,
        `=IFERROR(SUMIFS(${cr("H")},${cr("D")},$A${r})/B${r},0)`,
        `=IFERROR(SUMIFS(${cr("M")},${cr("D")},$A${r})/B${r},0)`,
        note,
      ]],
    });
  });
  D.push({
    range: "Dashboard!A21:I21",
    values: [[
      "Total", "=SUM(B16:B20)", "=SUM(C16:C20)", "=SUM(D16:D20)", "=SUM(E16:E20)", "=SUM(F16:F20)", "=SUM(G16:G20)",
      `=IFERROR(SUM(${cr("H")})/B21,0)`, `=IFERROR(SUM(${cr("M")})/B21,0)`,
    ]],
  });

  D.push({
    range: "Dashboard!A24:K24",
    values: [["Product", "Paid classes", "Free classes", "Pre-tax price", "Units sold", "Units refunded", "Revenue (pre-tax)", "Free classes given", "Free-class retail value", "Free value % of revenue", "Share of units"]],
  });
  for (let i = 0; i < PRODUCT_ROWS; i++) {
    const r = 25 + i;
    const s = 12 + i;
    const blank = (f) => `=IF($A${r}="","",${f})`;
    D.push({
      range: `Dashboard!A${r}:K${r}`,
      values: [[
        `=IF(Settings!A${s}="","",Settings!A${s})`,
        blank(`Settings!B${s}`),
        blank(`Settings!C${s}`),
        blank(`Settings!D${s}`),
        blank(`SUMIFS(${sr("L")},${sr("H")},$A${r},${sr("K")},"Completed")`),
        blank(`SUMIFS(${sr("L")},${sr("H")},$A${r},${sr("K")},"Refunded")`),
        blank(`SUMIFS(${sr("M")},${sr("H")},$A${r},${sr("K")},"Completed")`),
        blank(`E${r}*C${r}`),
        blank(`H${r}*IFERROR(D${r}/B${r},0)`),
        blank(`IFERROR(I${r}/G${r},0)`),
        blank(`IFERROR(E${r}/E$33,0)`),
      ]],
    });
  }
  D.push({
    range: "Dashboard!A33:K33",
    values: [["Total", "", "", "", "=SUM(E25:E32)", "=SUM(F25:F32)", "=SUM(G25:G32)", "=SUM(H25:H32)", "=SUM(I25:I32)", "=IFERROR(I33/G33,0)", "=SUM(K25:K32)"]],
  });

  const matrixHeader = ["Units by segment x product"];
  for (let i = 0; i < PRODUCT_ROWS; i++) matrixHeader.push(`=$A$${25 + i}`);
  matrixHeader.push("Total");
  D.push({ range: "Dashboard!A36:J36", values: [matrixHeader] });
  const cols = "BCDEFGHI";
  SEGMENTS.forEach(([seg], i) => {
    const r = 37 + i;
    const row = [`=A${16 + i}`];
    for (let k = 0; k < PRODUCT_ROWS; k++) {
      const c = cols[k];
      row.push(`=IF(${c}$36="","",SUMIFS(${sr("L")},${sr("S")},$A${r},${sr("H")},${c}$36,${sr("K")},"Completed"))`);
    }
    row.push(`=SUM(B${r}:I${r})`);
    D.push({ range: `Dashboard!A${r}:J${r}`, values: [row] });
  });
  const totRow = ["Total"];
  for (let k = 0; k < PRODUCT_ROWS; k++) totRow.push(`=IF(${cols[k]}$36="","",SUM(${cols[k]}37:${cols[k]}41))`);
  totRow.push("=SUM(J37:J41)");
  D.push({ range: "Dashboard!A42:J42", values: [totRow] });

  D.push({
    range: "Dashboard!A45:F45",
    values: [["Day (transaction date)", "Units", "Revenue (pre-tax)", "Units to Members", "Units to Active pack buyers", "% cannibalizing units"]],
  });
  for (let i = 0; i < 10; i++) {
    const r = 46 + i;
    D.push({
      range: `Dashboard!A${r}:F${r}`,
      values: [[
        i === 0 ? "=Settings!B3" : `=A${r - 1}+1`,
        `=SUMIFS(${sr("L")},${sr("D")},$A${r},${sr("K")},"Completed")`,
        `=SUMIFS(${sr("M")},${sr("D")},$A${r},${sr("K")},"Completed")`,
        `=SUMIFS(${sr("L")},${sr("D")},$A${r},${sr("K")},"Completed",${sr("S")},"Member")`,
        `=SUMIFS(${sr("L")},${sr("D")},$A${r},${sr("K")},"Completed",${sr("S")},"Active pack buyer")`,
        `=IFERROR((D${r}+E${r})/B${r},0)`,
      ]],
    });
  }
  D.push({ range: "Dashboard!A56:F56", values: [["Total", "=SUM(B46:B55)", "=SUM(C46:C55)", "=SUM(D46:D55)", "=SUM(E46:E55)", "=IFERROR((D56+E56)/B56,0)"]] });
  D.push({ range: "Dashboard!A58", values: [["Segments use the 90 days before each buyer's first sale purchase (SalesMTEK / ReservationsMTEK / FirstTimersMTEK in BigQuery, current through yesterday). Raw data: Sales_Raw and Customers_Raw."]] });

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: "USER_ENTERED", data: [...data, ...D] },
  });

  // -------------------------------------------------------------- Formatting
  const rng = (name, r1, r2, c1, c2) => ({ sheetId: id[name], startRowIndex: r1, endRowIndex: r2, startColumnIndex: c1, endColumnIndex: c2 });
  const fmt = (range, cell, fields) => ({ repeatCell: { range, cell: { userEnteredFormat: cell }, fields: `userEnteredFormat(${fields})` } });
  const nf = (type, pattern) => ({ numberFormat: { type, pattern } });
  const bold = { textFormat: { bold: true } };
  const head = { textFormat: { bold: true, foregroundColor: { red: 1, green: 1, blue: 1 } }, backgroundColor: { red: 0.17, green: 0.24, blue: 0.31 }, wrapStrategy: "WRAP" };
  const input = { textFormat: { foregroundColor: { red: 0.1, green: 0.3, blue: 0.9 } }, backgroundColor: { red: 0.93, green: 0.96, blue: 1 } };
  const total = { textFormat: { bold: true }, borders: { top: { style: "SOLID" } } };
  const dash = "Dashboard";

  const f = [
    fmt(rng(dash, 0, 1, 0, 1), { textFormat: { bold: true, fontSize: 16 } }, "textFormat"),
    fmt(rng(dash, 3, 4, 0, 6), { textFormat: { bold: true, fontSize: 14 } }, "textFormat"),
    fmt(rng(dash, 5, 6, 0, 5), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 5, 6, 6, 8), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 14, 15, 0, 10), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 23, 24, 0, 11), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 35, 36, 0, 10), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 44, 45, 0, 6), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 20, 21, 0, 10), total, "textFormat,borders"),
    fmt(rng(dash, 32, 33, 0, 11), total, "textFormat,borders"),
    fmt(rng(dash, 41, 42, 0, 10), total, "textFormat,borders"),
    fmt(rng(dash, 55, 56, 0, 6), total, "textFormat,borders"),
    // number formats
    fmt(rng(dash, 6, 7, 1, 2), nf("NUMBER", "0"), "numberFormat"),
    fmt(rng(dash, 7, 11, 1, 2), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 6, 7, 3, 4), nf("NUMBER", "0"), "numberFormat"),
    fmt(rng(dash, 7, 11, 3, 4), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 9, 10, 7, 8), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 10, 11, 7, 8), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 15, 21, 2, 3), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 15, 21, 4, 5), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 15, 21, 6, 7), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 15, 21, 5, 6), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 15, 21, 7, 8), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 15, 21, 8, 9), nf("NUMBER", "0.0"), "numberFormat"),
    fmt(rng(dash, 24, 33, 3, 4), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 24, 33, 6, 7), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 24, 33, 8, 9), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 24, 33, 9, 11), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 45, 56, 0, 1), nf("DATE", "ddd mmm d"), "numberFormat"),
    fmt(rng(dash, 45, 56, 2, 3), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 45, 56, 5, 6), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 1, 2, 1, 2), nf("DATE_TIME", "yyyy-mm-dd hh:mm"), "numberFormat"),
    // Settings
    fmt(rng("Settings", 0, 1, 0, 1), { textFormat: { bold: true, fontSize: 14 } }, "textFormat"),
    fmt(rng("Settings", 2, 8, 1, 2), input, "textFormat,backgroundColor"),
    fmt(rng("Settings", 11, 11 + PRODUCT_ROWS, 0, 4), input, "textFormat,backgroundColor"),
    fmt(rng("Settings", 10, 11, 0, 4), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng("Settings", 2, 3, 1, 2), nf("DATE", "yyyy-mm-dd"), "numberFormat"),
    fmt(rng("Settings", 4, 8, 1, 2), nf("PERCENT", "0%"), "numberFormat"),
    // raw tab headers
    fmt(rng("Sales_Raw", 0, 1, 0, 20), head, "textFormat,backgroundColor"),
    fmt(rng("Customers_Raw", 0, 1, 0, 19), head, "textFormat,backgroundColor"),
    fmt(rng("Sales_Raw", 1, 10000, 19, 20), nf("DATE_TIME", "yyyy-mm-dd hh:mm"), "numberFormat"),
    { updateDimensionProperties: { range: { sheetId: id[dash], dimension: "COLUMNS", startIndex: 0, endIndex: 1 }, properties: { pixelSize: 330 }, fields: "pixelSize" } },
    { updateDimensionProperties: { range: { sheetId: id[dash], dimension: "COLUMNS", startIndex: 1, endIndex: 10 }, properties: { pixelSize: 125 }, fields: "pixelSize" } },
    { updateDimensionProperties: { range: { sheetId: id[dash], dimension: "COLUMNS", startIndex: 9, endIndex: 10 }, properties: { pixelSize: 330 }, fields: "pixelSize" } },
    { updateDimensionProperties: { range: { sheetId: id[dash], dimension: "COLUMNS", startIndex: 6, endIndex: 7 }, properties: { pixelSize: 210 }, fields: "pixelSize" } },
    { updateDimensionProperties: { range: { sheetId: id["Settings"], dimension: "COLUMNS", startIndex: 0, endIndex: 1 }, properties: { pixelSize: 420 }, fields: "pixelSize" } },
    { updateDimensionProperties: { range: { sheetId: id["Settings"], dimension: "COLUMNS", startIndex: 1, endIndex: 4 }, properties: { pixelSize: 130 }, fields: "pixelSize" } },
    { updateSheetProperties: { properties: { sheetId: id["Sales_Raw"], gridProperties: { frozenRowCount: 1 } }, fields: "gridProperties.frozenRowCount" } },
    { updateSheetProperties: { properties: { sheetId: id["Customers_Raw"], gridProperties: { frozenRowCount: 1 } }, fields: "gridProperties.frozenRowCount" } },
  ];

  // Conditional formatting (verdict + status cells)
  const cf = (range, text, bg, fg) => ({
    addConditionalFormatRule: {
      index: 0,
      rule: {
        ranges: [range],
        booleanRule: {
          condition: { type: "TEXT_STARTS_WITH", values: [{ userEnteredValue: text }] },
          format: { backgroundColor: bg, textFormat: { foregroundColor: fg, bold: true } },
        },
      },
    },
  });
  const green = { red: 0.85, green: 0.94, blue: 0.85 }, gFg = { red: 0.1, green: 0.4, blue: 0.1 };
  const amber = { red: 1, green: 0.93, blue: 0.7 }, aFg = { red: 0.55, green: 0.35, blue: 0 };
  const red = { red: 0.98, green: 0.82, blue: 0.82 }, rFg = { red: 0.65, green: 0.1, blue: 0.1 };
  const verdict = rng(dash, 3, 4, 1, 6);
  const status = rng(dash, 6, 12, 4, 5);
  f.push(
    cf(verdict, "KEEP", green, gFg), cf(verdict, "WATCH", amber, aFg), cf(verdict, "PAUSE", red, rFg), cf(verdict, "WAIT", amber, aFg),
    cf(status, "OK", green, gFg), cf(status, "OVER", red, rFg), cf(status, "LOW", red, rFg), cf(status, "TOO FEW", amber, aFg), cf(status, "CHECK", amber, aFg)
  );
  f.push({ mergeCells: { range: verdict, mergeType: "MERGE_ALL" } });

  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: f } });
  console.log(`Sheet built: https://docs.google.com/spreadsheets/d/${SHEET_ID}/edit`);
}

main().catch((e) => {
  console.error("Setup failed:", e.message);
  process.exit(1);
});
