// scripts/bf-sale-monitor-import.mjs
// Black Friday 2026 sale monitor: refreshes two raw tabs in the Google Sheet
// "BF 2026 Sale Monitor" (the dashboard tab is all formulas on top of them).
//
//   Sales_Raw      - every line of the Vendredi Fou products, pulled live from
//                    the MTEK "Orders - UTC" report (id 353), today included.
//   Customers_Raw  - one row per buyer: segment, attendance, and the spend in
//                    the N days before their first sale purchase, where N = the
//                    pack's validity (expiry) read from the MTEK /packages API.
//                    That prior spend is "what we would have collected over the
//                    same stretch without the pack" (revenue displaced).
//   Packs_Raw      - the sale packs as MTEK has them (price, credits, expiry).
//
// Customer history comes from BigQuery SalesMTEK / ReservationsMTEK /
// FirstTimersMTEK (the daily MTEK syncs, current through yesterday), never from
// per-customer MTEK calls (the /reservations/ endpoint throttles hard).
// Everything on the sheet is overwritten each run, so it is safe to re-run.
//
// Segments = the Black Friday analysis segments (Bijan's black_friday_customer_analysis),
// first match wins, measured against each buyer's first sale purchase:
//   New               in FirstTimersMTEK with first class <=30 days before the purchase or
//                     later (b.cycle hard rule: never inferred from check-ins)
//   Member            membership purchase in the prior 45 days
//   Low activity      fewer than 4 check-ins in the prior 90 days
//   Credit holder     4+ check-ins and a credit (pack) purchase in the prior 90 days
//   Old credit holder 4+ check-ins and no credit purchase in the prior 90 days
//
// Env: MTEK_API_TOKEN, SHEET_ID, GOOGLE_APPLICATION_CREDENTIALS (via the auth
// action), optional SALE_START (default 2026-11-04, the presale),
// PRODUCT_PATTERN (default: the two presale packs 10 cours achetés, 5 offerts and 20 cours achetés, 10 offerts), DRY_RUN=true to skip the sheet.

import { BigQuery } from "@google-cloud/bigquery";
import { google } from "googleapis";
import { fetchMtekReport, isoDate, addDaysUTC } from "./lib/mtek-report.mjs";
import { TABLES } from "./lib/bcycle-mtek-tables.mjs";
import { fetchAllPages } from "./lib/mtek.mjs";

const SALES = TABLES.sales;
const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const SHEET_ID = process.env.SHEET_ID || "1QzHukVUupfa-BkzJOrdkKNipGc0Wfx2dsaaBoAajVOc";
const SALE_START = process.env.SALE_START || "2026-11-04";
const PRODUCT_PATTERN = new RegExp(process.env.PRODUCT_PATTERN || "(10 cours achetés, 5 offerts|20 cours achetés, 10 offerts)", "i");
const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || "root-cargo-453703-k7";
const BQ_DATASET = process.env.BQ_DATASET || "SalesZF";
const DRY_RUN = process.env.DRY_RUN === "true";
const CHUNK_DAYS = 7;
const DEFAULT_VALID_MONTHS = Number(process.env.DEFAULT_VALID_MONTHS || 6); // only if a pack is not found in /packages

const SALES_HEADERS = [
  "order_number", "order_date_utc", "order_time_utc", "transaction_date", "customer_id",
  "customer_email", "customer_name", "product", "product_id", "product_type", "line_status",
  "quantity", "subtotal", "tax", "total", "currency", "payment_method", "purchase_location",
];

const CUSTOMER_HEADERS = [
  "customer_id", "email", "name", "segment", "first_purchase_date", "sale_units_completed",
  "sale_revenue", "window_revenue", "window_credit_revenue", "window_membership_revenue",
  "window_orders", "prior90_pack_purchases", "prior90_classes_attended", "last_attended_prior",
  "last_purchase_prior", "membership_reservations_prior90", "membership_revenue_prior45", "in_first_timers",
  "first_class_date", "window_start", "window_days",
];

const PACK_HEADERS = ["product", "package_id", "price", "credit_quantity", "expiry_length", "expiry_unit", "expiry_months"];

const num = (v) => {
  const n = Number(String(v ?? "").replace(/,/g, ""));
  return Number.isFinite(n) ? n : 0;
};

function bqDate(v) {
  if (v == null) return "";
  return typeof v === "object" && "value" in v ? v.value : String(v);
}

async function fetchSaleLines() {
  const headerIndex = {};
  SALES.expectedHeaders.forEach((h, i) => (headerIndex[h] = i));
  const today = isoDate(new Date());
  const lines = [];
  const seen = new Set();

  for (let minDate = SALE_START; minDate <= today; minDate = addDaysUTC(minDate, CHUNK_DAYS)) {
    const end = addDaysUTC(minDate, CHUNK_DAYS - 1);
    const maxDate = end < today ? end : today;
    const report = await fetchMtekReport({
      baseUrl: MTEK_BASE_URL,
      token: MTEK_API_TOKEN,
      reportId: SALES.reportId,
      slug: SALES.reportSlug,
      pageSize: 500,
      dateParams: SALES.dateParams(minDate, maxDate),
    });
    if (JSON.stringify(report.headers) !== JSON.stringify(SALES.expectedHeaders)) {
      throw new Error(`Sales report headers changed shape: ${JSON.stringify(report.headers)}`);
    }
    console.log(`Chunk ${minDate}..${maxDate}: ${report.rows.length} report rows`);
    for (const raw of report.rows) {
      const g = (name) => raw[headerIndex[name]];
      if (!PRODUCT_PATTERN.test(String(g("Product") || ""))) continue;
      const row = SALES.mapRow(raw);
      const key = SALES.dedupKey(row);
      if (seen.has(key)) continue;
      seen.add(key);
      lines.push([
        g("Order Number"), g("Order Date (UTC)"), g("Order Time (UTC)"), isoFromMdy(row.transaction_date),
        String(g("Customer ID") ?? ""), g("Customer Email") ?? "", g("Customer Name") ?? "",
        g("Product") ?? "", String(g("Product ID") ?? ""), g("Product Type") ?? "", g("Line Status") ?? "",
        num(g("Line Quantity")), num(g("Line Subtotal")), num(g("Line Tax")), num(g("Line Total")),
        g("Currency") ?? "", g("Order Payment Method(s)") ?? "", g("Purchase Location") ?? "",
      ]);
    }
  }
  return lines;
}

function isoFromMdy(s) {
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(String(s || "").trim());
  return m ? `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}` : "";
}

// Pack validity from the MTEK /packages API (expiration_type relative: length + YR/MO/WK/DY;
// absolute: a fixed datetime). The product description text can be stale - the package
// attributes are what MTEK actually applies.
async function fetchPacks() {
  const rows = await fetchAllPages(
    `${MTEK_BASE_URL}/api/packages`,
    { page_size: "100" },
    { Authorization: `Bearer ${MTEK_API_TOKEN}` }
  );
  return rows
    .map((r) => ({ id: r.id, ...r.attributes }))
    .filter((a) => PRODUCT_PATTERN.test(String(a.title || "")))
    .map((a) => ({
      id: a.id, title: a.title, price: a.price, credits: a.credit_quantity,
      type: a.expiration_type, length: a.expiration_interval_length, unit: a.expiration_interval,
      until: a.expiration_datetime,
    }));
}

function monthsOf(p) {
  if (p.type === "relative" && p.length) {
    return { YR: p.length * 12, MO: p.length, WK: (p.length * 7 * 12) / 365, DY: (p.length * 12) / 365 }[p.unit] ?? null;
  }
  return null;
}

// Date the pack's validity window reaches back to: anchor minus the pack's validity.
function windowStartFor(anchor, p) {
  const d = new Date(`${anchor}T00:00:00Z`);
  if (p && p.type === "relative" && p.length) {
    if (p.unit === "YR") d.setUTCFullYear(d.getUTCFullYear() - p.length);
    else if (p.unit === "MO") d.setUTCMonth(d.getUTCMonth() - p.length);
    else if (p.unit === "WK") d.setUTCDate(d.getUTCDate() - p.length * 7);
    else if (p.unit === "DY") d.setUTCDate(d.getUTCDate() - p.length);
    else d.setUTCMonth(d.getUTCMonth() - DEFAULT_VALID_MONTHS);
  } else if (p && p.until) {
    const days = Math.round((new Date(p.until) - new Date(`${anchor}T00:00:00Z`)) / 86400000);
    d.setUTCDate(d.getUTCDate() - Math.max(days, 1));
  } else {
    d.setUTCMonth(d.getUTCMonth() - DEFAULT_VALID_MONTHS);
  }
  return isoDate(d);
}

async function fetchCustomerFacts(buyers) {
  const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
  const ids = buyers.map((b) => Number(b.id));
  const anchors = buyers.map((b) => b.firstDate);
  const starts = buyers.map((b) => b.windowStart);
  const T = (t) => `\`${BQ_PROJECT_ID}.${BQ_DATASET}.${t}\``;
  const salePattern = PRODUCT_PATTERN.source.replace(/'/g, "");

  const query = `
    WITH buyers AS (
      SELECT id, CAST(a AS DATE) AS anchor, CAST(w AS DATE) AS win_start
      FROM UNNEST(@ids) id WITH OFFSET i
      JOIN UNNEST(@anchors) a WITH OFFSET j ON i = j
      JOIN UNNEST(@starts) w WITH OFFSET k ON i = k
    ),
    s AS (
      SELECT b.id,
        SAFE.PARSE_DATE('%m/%d/%Y', x.transaction_date) AS d,
        b.anchor, b.win_start, x.order_number, x.product_type,
        SAFE_CAST(REPLACE(x.line_subtotal, ',', '') AS FLOAT64) AS amt
      FROM ${T("SalesMTEK")} x
      JOIN buyers b ON SAFE_CAST(x.customer_id AS INT64) = b.id
      WHERE x.order_status != 'Order Status'
        AND x.line_status = 'Completed'
        AND NOT REGEXP_CONTAINS(LOWER(x.product), r'${salePattern}')
        AND SAFE.PARSE_DATE('%m/%d/%Y', x.transaction_date) >= LEAST(b.win_start, DATE_SUB(b.anchor, INTERVAL 90 DAY))
        AND SAFE.PARSE_DATE('%m/%d/%Y', x.transaction_date) < b.anchor
    ),
    -- rev/credit_rev/member_rev/orders = spend over the pack's validity window (revenue displaced);
    -- member_rev45 and pack_orders (90d) only feed the segment rules.
    sa AS (
      SELECT id,
        SUM(IF(d >= win_start, amt, 0)) AS rev,
        SUM(IF(d >= win_start AND product_type = 'Credits', amt, 0)) AS credit_rev,
        SUM(IF(d >= win_start AND product_type = 'Memberships', amt, 0)) AS member_rev,
        SUM(IF(product_type = 'Memberships' AND d >= DATE_SUB(anchor, INTERVAL 45 DAY), amt, 0)) AS member_rev45,
        COUNT(DISTINCT IF(d >= win_start, order_number, NULL)) AS orders,
        COUNT(DISTINCT IF(product_type = 'Credits' AND d >= DATE_SUB(anchor, INTERVAL 90 DAY), order_number, NULL)) AS pack_orders,
        MAX(d) AS last_purchase
      FROM s GROUP BY id
    ),
    r AS (
      SELECT b.id,
        COUNTIF(x.Status = 'check in') AS attended,
        MAX(IF(x.Status = 'check in', x.\`Class Start Date\`, NULL)) AS last_attended,
        COUNTIF(x.Status = 'check in' AND IFNULL(TRIM(x.Contract), '') != '') AS contract_res
      FROM ${T("ReservationsMTEK")} x
      JOIN buyers b ON x.\`Customer ID\` = b.id
      WHERE x.\`Class Start Date\` >= DATE_SUB(b.anchor, INTERVAL 90 DAY)
        AND x.\`Class Start Date\` < b.anchor
      GROUP BY b.id
    ),
    ft AS (
      SELECT SAFE_CAST(string_field_1 AS INT64) AS id,
        MIN(SAFE.PARSE_DATE('%m/%d/%Y', string_field_5)) AS first_class
      FROM ${T("FirstTimersMTEK")}
      WHERE SAFE_CAST(string_field_1 AS INT64) IN UNNEST(@ids)
      GROUP BY id
    )
    SELECT b.id,
      IFNULL(sa.rev, 0) AS rev, IFNULL(sa.credit_rev, 0) AS credit_rev, IFNULL(sa.member_rev, 0) AS member_rev, IFNULL(sa.member_rev45, 0) AS member_rev45,
      IFNULL(sa.orders, 0) AS orders, IFNULL(sa.pack_orders, 0) AS pack_orders, sa.last_purchase,
      IFNULL(r.attended, 0) AS attended, r.last_attended, IFNULL(r.contract_res, 0) AS contract_res,
      ft.first_class
    FROM buyers b
    LEFT JOIN sa ON sa.id = b.id
    LEFT JOIN r ON r.id = b.id
    LEFT JOIN ft ON ft.id = b.id`;

  const [rows] = await bq.query({ query, params: { ids, anchors, starts } });
  return new Map(rows.map((x) => [String(x.id), x]));
}

function segmentFor(f, anchor) {
  const firstClass = bqDate(f.first_class);
  if (firstClass && firstClass >= addDaysUTC(anchor, -30)) return "New";
  if (f.member_rev45 > 0) return "Member";
  if (f.attended < 4) return "Low activity";
  return f.pack_orders > 0 ? "Credit holder" : "Old credit holder";
}

async function sheetsClient() {
  const auth = new google.auth.GoogleAuth({ scopes: ["https://www.googleapis.com/auth/spreadsheets"] });
  return google.sheets({ version: "v4", auth });
}

async function writeTab(sheets, tab, headers, rows) {
  // Clear only the imported columns - helper formula columns to the right survive.
  const lastCol = String.fromCharCode(64 + headers.length);
  await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: `${tab}!A:${lastCol}` });
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${tab}!A1`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [headers, ...rows] },
  });
}

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_API_TOKEN");

  const lines = await fetchSaleLines();
  console.log(`${lines.length} sale lines for pattern /${PRODUCT_PATTERN.source}/`);

  const packs = await fetchPacks();
  for (const p of packs) console.log(`Pack ${p.id} "${p.title}": $${p.price}, ${p.credits} credits, valid ${p.length} ${p.unit}`);
  const packByTitle = new Map(packs.map((p) => [p.title.trim().toLowerCase(), p]));

  // One row per buyer with at least one completed line; anchor = first purchase date.
  // Window = the longest validity among the packs they bought.
  const byCustomer = new Map();
  for (const l of lines) {
    const [, , , date, id, email, name, product, , , status, qty, subtotal] = l;
    if (!id) continue;
    const c = byCustomer.get(id) || { id, email, name, firstDate: date, units: 0, revenue: 0, products: new Set() };
    if (date && (!c.firstDate || date < c.firstDate)) c.firstDate = date;
    if (status === "Completed") {
      c.units += qty;
      c.revenue += subtotal;
      c.products.add(String(product).trim().toLowerCase());
    }
    byCustomer.set(id, c);
  }
  const buyers = [...byCustomer.values()].filter((b) => b.firstDate && b.units > 0);
  for (const b of buyers) {
    const starts = [...b.products].map((t) => {
      const p = packByTitle.get(t);
      if (!p) console.warn(`No /packages match for "${t}" - using ${DEFAULT_VALID_MONTHS} months`);
      return windowStartFor(b.firstDate, p);
    });
    b.windowStart = starts.sort()[0]; // earliest start = longest window
    b.windowDays = Math.round((new Date(`${b.firstDate}T00:00:00Z`) - new Date(`${b.windowStart}T00:00:00Z`)) / 86400000);
  }
  console.log(`${buyers.length} distinct buyers`);

  const facts = buyers.length ? await fetchCustomerFacts(buyers) : new Map();
  const customerRows = buyers.map((b) => {
    const f = facts.get(b.id) || { rev: 0, credit_rev: 0, member_rev: 0, orders: 0, pack_orders: 0, attended: 0, member_rev45: 0, contract_res: 0 };
    const segment = segmentFor(f, b.firstDate);
    const firstClass = bqDate(f.first_class);
    return [
      b.id, b.email, b.name, segment, b.firstDate, b.units, Math.round(b.revenue * 100) / 100,
      Math.round(f.rev * 100) / 100, Math.round(f.credit_rev * 100) / 100, Math.round(f.member_rev * 100) / 100,
      f.orders, f.pack_orders, f.attended, bqDate(f.last_attended), bqDate(f.last_purchase), f.contract_res,
      f.member_rev45, firstClass ? 1 : 0, firstClass, b.windowStart, b.windowDays,
    ];
  });

  const counts = {};
  for (const r of customerRows) counts[r[3]] = (counts[r[3]] || 0) + 1;
  console.log("Segments:", counts);

  if (DRY_RUN) {
    console.log("DRY RUN - sheet not written. Sample sale line:", lines[0], "\nSample customer:", customerRows[0]);
    return;
  }

  const sheets = await sheetsClient();
  await writeTab(sheets, "Sales_Raw", SALES_HEADERS, lines);
  await writeTab(sheets, "Customers_Raw", CUSTOMER_HEADERS, customerRows);
  await writeTab(sheets, "Packs_Raw", PACK_HEADERS, packs.map((p) => [
    p.title, p.id, p.price, p.credits, p.length ?? "", p.unit ?? "", monthsOf(p) == null ? "" : Math.round(monthsOf(p) * 100) / 100,
  ]));
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: "Import_Log!A1",
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: [
        ["Last import (UTC)", new Date().toISOString().replace("T", " ").slice(0, 19)],
        ["Sale lines imported", lines.length],
        ["Buyers imported", customerRows.length],
        ["Sale window start", SALE_START],
      ],
    },
  });
  console.log("Sheet updated.");
}

main().catch((err) => {
  console.error("BF sale monitor import failed:", err.message);
  process.exit(1);
});
