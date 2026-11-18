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

// Black Friday analysis segments (Bijan's black_friday_customer_analysis), first match wins:
// New (FirstTimersMTEK only - b.cycle hard rule) > Member > Low activity > Credit holder > Old credit holder.
const SEGMENTS = [
  ["New", "Incremental - first-timer (FirstTimersMTEK, first class <=30d before purchase or later)"],
  ["Member", "Cannibalization - paid a membership in the prior 45 days"],
  ["Low activity", "Incremental - fewer than 4 check-ins in the prior 90 days"],
  ["Credit holder", "Cannibalization - 4+ check-ins and bought credits in the prior 90 days"],
  ["Old credit holder", "Cannibalization - 4+ check-ins, no credit purchase in the prior 90 days"],
];
const PRODUCTS = [
  ["Prévente Vendredi fou – 10 cours achetés, 5 offerts", 10, 5, 270],
  ["Prévente Vendredi fou – 20 cours achetés, 10 offerts", 20, 10, 520],
];
const PRODUCT_ROWS = 8; // Settings rows 12-19

async function main() {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const existing = new Map(meta.data.sheets.map((s) => [s.properties.title, s.properties.sheetId]));
  const wanted = ["Dashboard", "Settings", "Sales_Raw", "Customers_Raw", "Packs_Raw", "Import_Log"];
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

  // Reset the Dashboard (values, formats, merges, conditional formats) so layout changes rebuild cleanly.
  const dashMeta = meta2.data.sheets.find((x) => x.properties.title === "Dashboard");
  const reset = [{ unmergeCells: { range: { sheetId: id["Dashboard"] } } }, { updateCells: { range: { sheetId: id["Dashboard"] }, fields: "userEnteredValue,userEnteredFormat" } }];
  for (let i = 0; i < (dashMeta.conditionalFormats || []).length; i++) reset.push({ deleteConditionalFormatRule: { sheetId: id["Dashboard"], index: 0 } });
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: reset } });

  const data = [];
  const put = (range, values) => data.push({ range, values });

  // ---------------------------------------------------------------- Settings
  put("Settings!A1", [["Settings - blue cells are editable; the Dashboard reads everything from here"]]);
  put("Settings!A3:C10", [
    ["First date shown in the Dashboard daily table", "2026-11-24", ""],
    ["Minimum buyers before the verdict is trusted", 40, "Below this the verdict says WAIT"],
    ["Max % of buyers who are Members", 0.3, "Members buying packs = revenue we'd likely have had anyway"],
    ["Max % of buyers who are credit holders (Credit holder + Old credit holder)", 0.3, "Current credit holders buying = the failure case"],
    ["Max % of buyers Members + credit holders combined", 0.5, "Combined cannibalization ceiling"],
    ["Min % of buyers New + Low activity (incremental)", 0.25, "Floor of genuinely new / low-activity money"],
    ["Min net revenue lift ($)", 0, "Pack revenue minus what the same buyers spent in the pack's validity window before (0 = break-even)"],
    ["Days since last membership purchase to count a buyer as 'on a membership'", 45, "Same 45 days as the Member segment; used by the 'Where buyers are coming from' block"],
  ]);
  put("Settings!A11:D11", [["Product (exact MTEK name)", "Paid classes", "Free classes", "Pre-tax price"]]);
  const prodRows = [];
  for (let i = 0; i < PRODUCT_ROWS; i++) prodRows.push(PRODUCTS[i] || ["", "", "", ""]);
  put(`Settings!A12:D${11 + PRODUCT_ROWS}`, prodRows);
  put("Settings!A21", [["Add a new pack by typing its exact MTEK product name in the next free row (and widen PRODUCT_PATTERN in the import script - it only pulls the two presale packs by default)."]]);

  // --------------------------------------------------------------- Import log
  put("Import_Log!D1", [["Written by scripts/bf-sale-monitor-import.mjs on every run - do not edit."]]);

  // ------------------------------------------------------------ Raw helpers
  put(`${S}!S1:T1`, [["segment", "order_ts_utc"]]);
  put(`${S}!S2`, [[`=ARRAYFORMULA(IF(E2:E="","",IFERROR(VLOOKUP(E2:E,${C}!A:D,4,FALSE),"Unknown")))`]]);
  put(`${S}!T2`, [[`=ARRAYFORMULA(IF(B2:B="","",B2:B+C2:C))`]]);

  // --------------------------------------------------------------- Dashboard
  const D = [];
  const set = (cell, v) => D.push({ range: `Dashboard!${cell}`, values: [[v]] });
  set("A1", "BF 2026 - Vendredi Fou presale packs (10+5, 20+10): keep the sale on?");
  set("A2", "Last import (UTC)");
  set("B2", "=Import_Log!B1");
  set("A4", "VERDICT");
  set(
    "B4",
    `=IF(B7<D7,"WAIT - only "&B7&" buyers so far (need "&D7&")",IF(COUNTIF(E8:E12,"OK")=5,"KEEP THE SALE ACTIVE",IF(COUNTIF(E8:E12,"OK")>=3,"WATCH CLOSELY - "&(5-COUNTIF(E8:E12,"OK"))&" check(s) failing","PAUSE / REVIEW - "&(5-COUNTIF(E8:E12,"OK"))&" checks failing")))`
  );
  D.push({ range: "Dashboard!A6:E6", values: [["Decision checks", "Actual", "Rule", "Threshold", "Status"]] });
  D.push({
    range: "Dashboard!A7:E13",
    values: [
      ["Distinct buyers", "=B21", "at least", "=Settings!B4", '=IF(B7>=D7,"OK","TOO FEW")'],
      ["% of buyers who are Members", "=C17", "at most", "=Settings!B5", '=IF(B8<=D8,"OK","OVER")'],
      ["% of buyers who are credit holders (Credit + Old credit holder)", "=C19+C20", "at most", "=Settings!B6", '=IF(B9<=D9,"OK","OVER")'],
      ["% Members + credit holders", "=C17+C19+C20", "at most", "=Settings!B7", '=IF(B10<=D10,"OK","OVER")'],
      ["% New + Low activity (incremental)", "=C16+C18", "at least", "=Settings!B8", '=IF(B11>=D11,"OK","LOW")'],
      ["Net revenue lift (pack revenue - what these buyers spent in the same window before)", "=I21", "at least", "=Settings!B9", '=IF(B12>=D12,"OK","NEGATIVE")'],
      ["Buyers without a segment", `=COUNTA(${cr("A")})-B21`, "should be", 0, '=IF(B13=D13,"OK","CHECK")'],
    ],
  });
  D.push({
    range: "Dashboard!G6:H11",
    values: [
      ["Last 24 hours (to last import)", ""],
      ["Units sold", `=SUMIFS(${sr("L")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed")`],
      ["Units to Members", `=SUMIFS(${sr("L")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed",${sr("S")},"Member")`],
      ["Units to credit holders", `=SUMIFS(${sr("L")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed",${sr("S")},"Credit holder")+SUMIFS(${sr("L")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed",${sr("S")},"Old credit holder")`],
      ["% cannibalizing units", "=IFERROR((H8+H9)/H7,0)"],
      ["Revenue (pre-tax)", `=SUMIFS(${sr("M")},${sr("T")},">"&(Import_Log!$B$1-1),${sr("K")},"Completed")`],
    ],
  });

  D.push({
    range: "Dashboard!J6:K12",
    values: [
      ["Revenue won vs lost (all buyers, pre-tax)", ""],
      ["Revenue won - pack sales", "=F21"],
      ["Revenue lost - same-window spend displaced", "=H21"],
      ["Net revenue lift", "=K7-K8"],
      ["Net lift per buyer", "=IFERROR(K9/B21,0)"],
      ["Displaced: membership payments", `=SUM(${cr("J")})`],
      ["Displaced: credit / pack purchases", `=SUM(${cr("I")})`],
    ],
  });

  D.push({
    range: "Dashboard!A15:L15",
    values: [["Segment (Black Friday analysis segments)", "Buyers", "% of buyers", "Units", "% of units", "Revenue won (pack sales, pre-tax)", "% of revenue", "Revenue lost (spend in the pack's validity window before)", "Net revenue lift", "Net lift per buyer", "Avg classes attended prior 90d", "Read as"]],
  });
  SEGMENTS.forEach(([seg, note], i) => {
    const r = 16 + i;
    D.push({
      range: `Dashboard!A${r}:L${r}`,
      values: [[
        seg,
        `=COUNTIFS(${cr("D")},$A${r})`,
        `=IFERROR(B${r}/B$21,0)`,
        `=SUMIFS(${cr("F")},${cr("D")},$A${r})`,
        `=IFERROR(D${r}/D$21,0)`,
        `=SUMIFS(${cr("G")},${cr("D")},$A${r})`,
        `=IFERROR(F${r}/F$21,0)`,
        `=SUMIFS(${cr("H")},${cr("D")},$A${r})`,
        `=F${r}-H${r}`,
        `=IFERROR(I${r}/B${r},0)`,
        `=IFERROR(SUMIFS(${cr("M")},${cr("D")},$A${r})/B${r},0)`,
        note,
      ]],
    });
  });
  D.push({
    range: "Dashboard!A21:K21",
    values: [[
      "Total", "=SUM(B16:B20)", "=SUM(C16:C20)", "=SUM(D16:D20)", "=SUM(E16:E20)", "=SUM(F16:F20)", "=SUM(G16:G20)",
      "=SUM(H16:H20)", "=SUM(I16:I20)", "=IFERROR(I21/B21,0)", `=IFERROR(SUM(${cr("M")})/B21,0)`,
    ]],
  });

  D.push({
    range: "Dashboard!A24:L24",
    values: [["Product", "Paid classes", "Free classes", "Pre-tax price", "Units sold", "Units refunded", "Revenue (pre-tax)", "Free classes given", "Free-class retail value", "Free value % of revenue", "Share of units", "Valid for (months, from MTEK)"]],
  });
  for (let i = 0; i < PRODUCT_ROWS; i++) {
    const r = 25 + i;
    const s = 12 + i;
    const blank = (f) => `=IF($A${r}="","",${f})`;
    D.push({
      range: `Dashboard!A${r}:L${r}`,
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
        blank(`IFERROR(VLOOKUP($A${r},Packs_Raw!$A:$G,7,FALSE),"")`),
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
    values: [["Day (transaction date)", "Units", "Revenue (pre-tax)", "Units to Members", "Units to credit holders", "% cannibalizing units"]],
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
        `=SUMIFS(${sr("L")},${sr("D")},$A${r},${sr("K")},"Completed",${sr("S")},"Credit holder")+SUMIFS(${sr("L")},${sr("D")},$A${r},${sr("K")},"Completed",${sr("S")},"Old credit holder")`,
        `=IFERROR((D${r}+E${r})/B${r},0)`,
      ]],
    });
  }
  D.push({ range: "Dashboard!A56:F56", values: [["Total", "=SUM(B46:B55)", "=SUM(C46:C55)", "=SUM(D46:D55)", "=SUM(E46:E55)", "=IFERROR((D56+E56)/B56,0)"]] });
  D.push({ range: "Dashboard!A58", values: [["Segments (first match): New = in FirstTimersMTEK with first class <=30d before the buyer's first sale purchase or later; Member = paid a membership in the 45 days before; Low activity = <4 check-ins in the 90 days before; Credit holder = 4+ check-ins and bought credits in the 90 days before; Old credit holder = 4+ check-ins, no credit purchase. History from SalesMTEK / ReservationsMTEK / FirstTimersMTEK in BigQuery (current through yesterday)."]] });
  D.push({ range: "Dashboard!A59", values: [["Revenue won = pack sales (pre-tax). Revenue lost = what the same buyer spent (memberships + credits, pre-tax, excluding the sale packs) in the N days before their first sale purchase, where N = that pack's validity in MTEK (Packs_Raw). It is the spend we assume the pack replaces for as long as the credits last: a Member who stops paying for the pack's validity, a credit holder who would have bought credits anyway. Net lift = won - lost."]] });

  // ---- Bijan: which memberships are buyers leaving, annual-commitment plans, billing cycles
  const X = `${C}!$X:$X`, Y = `${C}!$Y:$Y`, Z = `${C}!$Z:$Z`;
  const thr = "Settings!$B$10";
  D.push({ range: "Dashboard!A61", values: [["Where buyers are coming from: memberships they were on before the sale"]] });
  D.push({
    range: "Dashboard!A62:D62",
    values: [["Buyer's last membership before the sale", "Buyers", "% of buyers (row 63: of all; rows 64-65: of those on a membership)", "Avg billing cycles on it"]],
  });
  D.push({
    range: "Dashboard!A63:D67",
    values: [
      ["On a membership (last membership purchase within the Settings window)", `=COUNTIFS(${X},"<="&${thr})`, "=IFERROR(B63/B21,0)", `=IFERROR(AVERAGEIFS(${Y},${X},"<="&${thr}),0)`],
      ["   of which Annual commitment plans (1 an / 1 year / 13-cycle commitment)", `=COUNTIFS(${X},"<="&${thr},${Z},"Annual commitment")`, "=IFERROR(B64/B63,0)", `=IFERROR(AVERAGEIFS(${Y},${X},"<="&${thr},${Z},"Annual commitment"),0)`],
      ["   of which Short term / other (2, 4, 8 weeks, trials)", `=COUNTIFS(${X},"<="&${thr},${Z},"Short term / other")`, "=IFERROR(B65/B63,0)", `=IFERROR(AVERAGEIFS(${Y},${X},"<="&${thr},${Z},"Short term / other"),0)`],
      ["Membership lapsed before the sale (last purchase older than the window)", `=COUNTIFS(${X},">"&${thr})`, "=IFERROR(B66/B21,0)", ""],
      ["No membership in the 400 days before", `=B21-COUNT(${X})`, "=IFERROR(B67/B21,0)", ""],
    ],
  });
  D.push({ range: "Dashboard!A69:C69", values: [["Billing cycles on that membership (buyers on a membership)", "Buyers", "% of buyers on a membership"]] });
  D.push({
    range: "Dashboard!A70:C74",
    values: [
      ["1 cycle (just started)", `=COUNTIFS(${X},"<="&${thr},${Y},1)`, "=IFERROR(B70/B$63,0)"],
      ["2 to 3 cycles", `=COUNTIFS(${X},"<="&${thr},${Y},">=2",${Y},"<=3")`, "=IFERROR(B71/B$63,0)"],
      ["4 to 6 cycles", `=COUNTIFS(${X},"<="&${thr},${Y},">=4",${Y},"<=6")`, "=IFERROR(B72/B$63,0)"],
      ["7 to 12 cycles", `=COUNTIFS(${X},"<="&${thr},${Y},">=7",${Y},"<=12")`, "=IFERROR(B73/B$63,0)"],
      ["13 or more cycles", `=COUNTIFS(${X},"<="&${thr},${Y},">=13")`, "=IFERROR(B74/B$63,0)"],
    ],
  });
  D.push({
    range: "Dashboard!A76",
    values: [[`=IFERROR(QUERY(${C}!A:Z,"select V, count(A), avg(Y), max(Z) where V is not null and X <= "&${thr}&" group by V order by count(A) desc limit 15 label V 'Which membership they were on (top 15)', count(A) 'Buyers', avg(Y) 'Avg billing cycles', max(Z) 'Plan type'",1),"")`]],
  });
  // membership tier x what they bought x value
  const AB = `${C}!$AB:$AB`, AA = `${C}!$AA:$AA`, G = `${C}!$G:$G`, AD = `${C}!$AC:$AC`;
  D.push({ range: "Dashboard!A94", values: [["Which kind of membership they were on, what they bought, and the value"]] });
  D.push({
    range: "Dashboard!A95:L95",
    values: [["Membership tier (from product name)", "Buyers", "% of buyers on a membership", "Avg price per cycle (membership they left)", "Membership value per cycle (sum)", "Bought 10+5 pack", "Bought 20+10 pack", "Pack revenue won", "Pack revenue / membership value per cycle", "On an annual commitment", "% annual commitment", "Avg billing cycles"]],
  });
  const tiers = ["4-pack (1x/week)", "8-pack (2x/week)", "12-pack (3x/week)", "Unlimited", "Other"];
  tiers.forEach((t, i) => {
    const r = 96 + i;
    const on = `${X},"<="&${thr},${AB},$A${r}`;
    D.push({
      range: `Dashboard!A${r}:L${r}`,
      values: [[t, `=COUNTIFS(${on})`, "=IFERROR(B" + r + "/B$63,0)", `=IFERROR(AVERAGEIFS(${AA},${on}),0)`, `=SUMIFS(${AA},${on})`,
        `=COUNTIFS(${on},${AD},"*10 cours achetés*")`, `=COUNTIFS(${on},${AD},"*20 cours achetés*")`, `=SUMIFS(${G},${on})`,
        `=IFERROR(H${r}/E${r},0)`, `=COUNTIFS(${on},${Z},"Annual commitment")`, `=IFERROR(J${r}/B${r},0)`, `=IFERROR(AVERAGEIFS(${Y},${on}),0)`]],
    });
  });
  D.push({
    range: "Dashboard!A101:L101",
    values: [["Total", "=SUM(B96:B100)", "=SUM(C96:C100)", "=IFERROR(E101/B101,0)", "=SUM(E96:E100)", "=SUM(F96:F100)", "=SUM(G96:G100)", "=SUM(H96:H100)", "=IFERROR(H101/E101,0)", "=SUM(J96:J100)", "=IFERROR(J101/B101,0)", `=IFERROR(AVERAGEIFS(${Y},${X},"<="&${thr}),0)`]],
  });
  D.push({ range: "Dashboard!A103", values: [["Tier is read from the membership product name: 1x/week = 4-pack, 2x/week = 8-pack, 3x/week = 12-pack, any 'illimité/unlimited' = Unlimited. 'Membership value per cycle' is what they paid for their last membership cycle (pre-tax); 'Pack revenue / membership value' above 1 means the pack brought in more than one cycle of what they were paying."]] });
  D.push({ range: "Dashboard!A92", values: [["Cycles = purchases of that same membership product in the 400 days before the buyer's first sale purchase (one purchase per billing cycle). Annual commitment is detected from the product name (1 an / 1 year / commitment) - adjust the ANNUAL_PLAN pattern in the import script if your APAC naming differs."]] });

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
    fmt(rng(dash, 5, 6, 9, 11), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 14, 15, 0, 12), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 23, 24, 0, 12), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 35, 36, 0, 10), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 44, 45, 0, 6), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 20, 21, 0, 12), total, "textFormat,borders"),
    fmt(rng(dash, 32, 33, 0, 12), total, "textFormat,borders"),
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
    fmt(rng(dash, 15, 21, 7, 10), nf("CURRENCY", "$#,##0;[Red]-$#,##0"), "numberFormat"),
    fmt(rng(dash, 15, 21, 10, 11), nf("NUMBER", "0.0"), "numberFormat"),
    fmt(rng(dash, 11, 12, 1, 2), nf("CURRENCY", "$#,##0;[Red]-$#,##0"), "numberFormat"),
    fmt(rng(dash, 11, 12, 3, 4), nf("CURRENCY", "$#,##0;[Red]-$#,##0"), "numberFormat"),
    fmt(rng(dash, 6, 12, 10, 11), nf("CURRENCY", "$#,##0;[Red]-$#,##0"), "numberFormat"),
    fmt(rng(dash, 8, 9, 9, 11), bold, "textFormat"),
    fmt(rng(dash, 23, 33, 11, 12), nf("NUMBER", "0.#"), "numberFormat"),
    fmt(rng(dash, 24, 33, 3, 4), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 24, 33, 6, 7), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 24, 33, 8, 9), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 24, 33, 9, 11), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 45, 56, 0, 1), nf("DATE", "ddd mmm d"), "numberFormat"),
    fmt(rng(dash, 45, 56, 2, 3), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 45, 56, 5, 6), nf("PERCENT", "0.0%"), "numberFormat"),
    fmt(rng(dash, 1, 2, 1, 2), nf("DATE_TIME", "yyyy-mm-dd hh:mm"), "numberFormat"),
    fmt(rng(dash, 60, 61, 0, 1), { textFormat: { bold: true, fontSize: 13 } }, "textFormat"),
    fmt(rng(dash, 61, 62, 0, 4), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 68, 69, 0, 3), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 75, 76, 0, 4), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 62, 67, 2, 3), nf("PERCENT", "0%"), "numberFormat"),
    fmt(rng(dash, 62, 67, 3, 4), nf("NUMBER", "0.0"), "numberFormat"),
    fmt(rng(dash, 69, 74, 2, 3), nf("PERCENT", "0%"), "numberFormat"),
    fmt(rng(dash, 76, 91, 2, 3), nf("NUMBER", "0.0"), "numberFormat"),
    // Settings
    fmt(rng("Settings", 0, 1, 0, 1), { textFormat: { bold: true, fontSize: 14 } }, "textFormat"),
    fmt(rng("Settings", 2, 10, 1, 2), input, "textFormat,backgroundColor"),
    fmt(rng("Settings", 8, 9, 1, 2), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng("Settings", 11, 11 + PRODUCT_ROWS, 0, 4), input, "textFormat,backgroundColor"),
    fmt(rng("Settings", 10, 11, 0, 4), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng("Settings", 2, 3, 1, 2), nf("DATE", "yyyy-mm-dd"), "numberFormat"),
    fmt(rng("Settings", 4, 8, 1, 2), nf("PERCENT", "0%"), "numberFormat"),
    // raw tab headers
    fmt(rng("Sales_Raw", 0, 1, 0, 20), head, "textFormat,backgroundColor"),
    fmt(rng(dash, 93, 94, 0, 1), { textFormat: { bold: true, fontSize: 13 } }, "textFormat"),
    fmt(rng(dash, 94, 95, 0, 12), head, "textFormat,backgroundColor,wrapStrategy"),
    fmt(rng(dash, 95, 101, 2, 3), nf("PERCENT", "0%"), "numberFormat"),
    fmt(rng(dash, 95, 101, 3, 5), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 95, 101, 7, 8), nf("CURRENCY", "$#,##0"), "numberFormat"),
    fmt(rng(dash, 95, 101, 8, 9), nf("NUMBER", "0.0"), "numberFormat"),
    fmt(rng(dash, 95, 101, 10, 11), nf("PERCENT", "0%"), "numberFormat"),
    fmt(rng(dash, 95, 101, 11, 12), nf("NUMBER", "0.0"), "numberFormat"),
    fmt(rng(dash, 100, 101, 0, 12), total, "textFormat,borders"),
    fmt(rng("Customers_Raw", 0, 1, 0, 30), head, "textFormat,backgroundColor"),
    fmt(rng("Packs_Raw", 0, 1, 0, 7), head, "textFormat,backgroundColor"),
    fmt(rng("Sales_Raw", 1, 10000, 19, 20), nf("DATE_TIME", "yyyy-mm-dd hh:mm"), "numberFormat"),
    { updateDimensionProperties: { range: { sheetId: id[dash], dimension: "COLUMNS", startIndex: 0, endIndex: 1 }, properties: { pixelSize: 330 }, fields: "pixelSize" } },
    { updateDimensionProperties: { range: { sheetId: id[dash], dimension: "COLUMNS", startIndex: 1, endIndex: 11 }, properties: { pixelSize: 125 }, fields: "pixelSize" } },
    { updateDimensionProperties: { range: { sheetId: id[dash], dimension: "COLUMNS", startIndex: 9, endIndex: 10 }, properties: { pixelSize: 290 }, fields: "pixelSize" } },
    { updateDimensionProperties: { range: { sheetId: id[dash], dimension: "COLUMNS", startIndex: 11, endIndex: 12 }, properties: { pixelSize: 330 }, fields: "pixelSize" } },
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
  const status = rng(dash, 6, 13, 4, 5);
  f.push(
    cf(verdict, "KEEP", green, gFg), cf(verdict, "WATCH", amber, aFg), cf(verdict, "PAUSE", red, rFg), cf(verdict, "WAIT", amber, aFg),
    cf(status, "OK", green, gFg), cf(status, "OVER", red, rFg), cf(status, "LOW", red, rFg), cf(status, "NEGATIVE", red, rFg), cf(status, "TOO FEW", amber, aFg), cf(status, "CHECK", amber, aFg)
  );
  f.push({ mergeCells: { range: verdict, mergeType: "MERGE_ALL" } });

  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: f } });
  console.log(`Sheet built: https://docs.google.com/spreadsheets/d/${SHEET_ID}/edit`);
}

main().catch((e) => {
  console.error("Setup failed:", e.message);
  process.exit(1);
});
