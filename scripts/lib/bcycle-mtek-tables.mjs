// scripts/lib/bcycle-mtek-tables.mjs
// Per-table config registry for b.cycle's three MTEK -> BigQuery tables
// (SalesMTEK, FirstTimersMTEK, ReservationsMTEK). Single source of truth for
// each table's MTEK report shape, column mapping, and dedup key — shared by
// the three daily/weekly sync scripts (scripts/mtek-*-to-bigquery.mjs) AND
// scripts/bcycle-mtek-monthly-data-audit.mjs, so there is never a second,
// drifting copy of a 35-column positional map or the subtle Sales dedup key.
//
// This is b.cycle-specific (dataset SalesZF) — SPINCO's sibling sync scripts
// have their own separate table shapes against a different dataset and are
// NOT covered by this registry.

import { toMDYYYY, toTime12h, boolToString, nullToEmpty } from "./format.mjs";

const identity = nullToEmpty;

// ---------------------------------------------------------------------------
// Sales

const SALES_EXPECTED_HEADERS = [
  "Order Number",
  "Order Status",
  "Order Date (UTC)",
  "Order Time (UTC)",
  "Order Device",
  "Processed by System?",
  "Customer ID",
  "Customer Email",
  "Customer Name",
  "Company Name",
  "Broker ID",
  "Broker Email",
  "Broker Name",
  "Product",
  "Product ID",
  "Variant ID",
  "Product Barcode",
  "Product SKU",
  "Product Color",
  "Product Size",
  "Product Type",
  "Vendor",
  "Sub Category",
  "Line Status",
  "Transaction Date",
  "Currency",
  "Line Quantity",
  "Line Subtotal",
  "Original Price",
  "Line Tax",
  "Line Total",
  "Order Payment Method(s)",
  "Purchase Location",
  "Fulfillment Location",
  "Fulfillment Region",
];

// Positional map: report column index -> [bqColumnName, transformFn]
const SALES_COLUMN_MAP = [
  ["order_number", identity],
  ["order_status", identity],
  ["order_date_utc", toMDYYYY],
  ["order_time_utc", toTime12h],
  ["order_device", identity],
  ["processed_by_system", boolToString],
  ["customer_id", identity],
  ["customer_email", identity],
  ["customer_name", identity],
  ["company_name", identity],
  ["broker_id", identity],
  ["broker_email", identity],
  ["broker_name", identity],
  ["product", identity],
  ["product_id", identity],
  ["variant_id", identity],
  ["product_barcode", identity],
  ["product_sku", identity],
  ["product_color", identity],
  ["product_size", identity],
  ["product_type", identity],
  ["vendor", identity],
  ["sub_category", identity],
  ["line_status", identity],
  ["transaction_date", toMDYYYY],
  ["currency", identity],
  ["line_quantity", identity],
  ["line_subtotal", identity],
  ["original_price", identity],
  ["line_tax", identity],
  ["line_total", identity],
  ["order_payment_methods", identity],
  ["purchase_location", identity],
  ["fulfillment_location", identity],
  ["fulfillment_region", identity],
];

function mapSalesRow(row) {
  const out = {};
  SALES_COLUMN_MAP.forEach(([bqCol, transform], i) => {
    out[bqCol] = transform(row[i]);
  });
  return out;
}

// transaction_date, line_status, and line_quantity are all part of the key
// because refunds create a second line with the same order_number/
// product_id/variant_id — sometimes on a later transaction_date, sometimes
// the exact same day as the original purchase — distinguished only by
// line_status ("Completed" vs "Refunded") and the sign of line_quantity.
// Without all of these, a refund can look like "already present" (matching
// its own original purchase line) and get silently dropped.
function salesDedupKey(row) {
  return `${row.order_number}|${row.product_id}|${row.variant_id}|${row.transaction_date}|${row.line_status}|${row.line_quantity}`;
}

const sales = {
  key: "sales",
  label: "Sales",
  bqTable: process.env.BQ_TABLE_SALES || "SalesMTEK",
  reportId: process.env.MTEK_SALES_REPORT_ID || "353",
  reportSlug: process.env.MTEK_SALES_REPORT_SLUG || "orders-utc",
  expectedHeaders: SALES_EXPECTED_HEADERS,
  mapRow: mapSalesRow,
  dedupKey: salesDedupKey,
  dedupKeyColumns: ["order_number", "product_id", "variant_id", "transaction_date", "line_status", "line_quantity"],
  dateParams: (minDate, maxDate) => ({ min_transaction_date: minDate, max_transaction_date: maxDate }),
  // Audit-only facts (not used by the daily sync scripts):
  auditDateColumn: "transaction_date",
  auditDateKind: "string-mdy",
  formatColumns: ["order_date_utc", "transaction_date"],
  junkFilterSql: "order_status != 'Order Status'",
};

// ---------------------------------------------------------------------------
// First Timers

const FIRST_TIMERS_EXPECTED_HEADERS = [
  "Customer",
  "Customer ID",
  "Customer Email",
  "Customer Phone Number",
  "Join Date",
  "First Class Date",
  "Class Start Time",
  "Class Location",
  "Class Type",
  "Class Category",
  "Instructor Name",
  "Guest Reservation?",
  "Pay With Type",
];

function mapFirstTimersRow(row) {
  const [
    customer,
    customerId,
    email,
    phone,
    joinDate,
    firstClassDate,
    classStartTime,
    location,
    classType,
    classCategory,
    instructor,
    guestReservation,
    payWithType,
  ] = row;

  return {
    string_field_0: nullToEmpty(customer),
    string_field_1: nullToEmpty(customerId),
    string_field_2: nullToEmpty(email),
    string_field_3: nullToEmpty(phone),
    string_field_4: toMDYYYY(joinDate),
    string_field_5: toMDYYYY(firstClassDate),
    string_field_6: toTime12h(classStartTime),
    string_field_7: nullToEmpty(location),
    string_field_8: nullToEmpty(classType),
    string_field_9: nullToEmpty(classCategory),
    string_field_10: nullToEmpty(instructor),
    string_field_11: boolToString(guestReservation),
    string_field_12: nullToEmpty(payWithType),
  };
}

const firstTimers = {
  key: "firstTimers",
  label: "First Timers",
  bqTable: process.env.BQ_TABLE_FIRST_TIMERS || "FirstTimersMTEK",
  reportId: process.env.MTEK_FIRST_TIMERS_REPORT_ID || "317",
  reportSlug: process.env.MTEK_FIRST_TIMERS_REPORT_SLUG || "first-timers",
  expectedHeaders: FIRST_TIMERS_EXPECTED_HEADERS,
  mapRow: mapFirstTimersRow,
  dedupKey: (row) => row.string_field_1,
  dedupKeyColumns: ["string_field_1"],
  dateParams: (minDate, maxDate) => ({ min_first_class_date_day: minDate, max_first_class_date_day: maxDate }),
  auditDateColumn: "string_field_5",
  auditDateKind: "string-mdy",
  formatColumns: ["string_field_4", "string_field_5"],
  // Matches the junk-row filter documented in docs/bcycle-bigquery schema
  // notes: rows 1-3 are a leaked CSV comment header, row 4 is the literal
  // header row re-imported as data.
  junkFilterSql:
    "string_field_1 NOT IN ('Customer ID') AND string_field_0 IS NOT NULL AND string_field_0 NOT LIKE '#%'",
};

// ---------------------------------------------------------------------------
// Reservations

const RESERVATIONS_EXPECTED_HEADERS = [
  "Reservation ID", "Reservation Type", "Status", "Creation Date", "Updated Date",
  "Cancelled Date", "Unconverted Guest", "Converted Guest", "Guest", "Guest Email",
  "Credit Payment Origin", "Credit", "Package", "Membership", "Contract",
  "Class ID", "Class Start Date", "Class Start Time", "Class Day of Week", "Class Custom Name",
  "Class Is Public?", "Class Tags", "Class Is Free?", "Class Type", "Class Category",
  "Class Duration (Minutes)", "Layout", "Layout Format", "Layout Capacity", "Class Capacity",
  "Classroom", "Location", "Region", "Instructor ID(s)", "Instructor Employee ID(s)",
  "Instructor Names", "Instructor Schedule Names", "Class Has Substitute?", "Customer ID", "Customer Email",
  "Customer Name", "Company Name", "Payer ID", "Payer Email", "Payer Name",
  "Broker ID", "Broker Email", "Broker Name", "Spot", "Spot Type",
];

// Exact BigQuery column names (positional match to RESERVATIONS_EXPECTED_HEADERS)
const RESERVATIONS_BQ_COLUMNS = [
  "Reservation ID", "Reservation Type", "Status", "Creation Date", "Updated Date",
  "Cancelled Date", "Unconverted Guest", "Converted Guest", "Guest", "Guest Email",
  "Credit Payment Origin", "Credit", "Package", "Membership", "Contract",
  "Class ID", "Class Start Date", "Class Start Time", "Class Day of Week", "Class Custom Name",
  "Class Is Public_", "Class Tags", "Class Is Free_", "Class Type", "Class Category",
  "Class Duration _Minutes_", "Layout", "Layout Format", "Layout Capacity", "Class Capacity",
  "Classroom", "Location", "Region", "Instructor ID_s_", "Instructor Employee ID_s_",
  "Instructor Names", "Instructor Schedule Names", "Class Has Substitute_", "Customer ID", "Customer Email",
  "Customer Name", "Company Name", "Payer ID", "Payer Email", "Payer Name",
  "Broker ID", "Broker Email", "Broker Name", "Spot", "Spot Type",
];

const RESERVATIONS_DATE_ONLY_COLUMNS = new Set(["Creation Date", "Updated Date", "Cancelled Date", "Class Start Date"]);
const RESERVATIONS_STRING_BOOL_COLUMNS = new Set(["Class Is Public_"]); // report sends "True"/"False" string, not real bool

function passthrough(v) {
  return v === undefined ? null : v;
}

function toDateOnly(v) {
  if (v === null || v === undefined) return null;
  return String(v).slice(0, 10);
}

function toBool(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "boolean") return v;
  return String(v).toLowerCase() === "true";
}

function mapReservationsRow(row) {
  const out = {};
  RESERVATIONS_BQ_COLUMNS.forEach((bqCol, i) => {
    const raw = row[i];
    if (RESERVATIONS_DATE_ONLY_COLUMNS.has(bqCol)) {
      out[bqCol] = toDateOnly(raw);
    } else if (RESERVATIONS_STRING_BOOL_COLUMNS.has(bqCol)) {
      out[bqCol] = toBool(raw);
    } else {
      out[bqCol] = passthrough(raw);
    }
  });
  return out;
}

const reservations = {
  key: "reservations",
  label: "Reservations",
  bqTable: process.env.BQ_TABLE_RESERVATIONS || "ReservationsMTEK",
  reportId: process.env.MTEK_RESERVATIONS_REPORT_ID || "341",
  reportSlug: process.env.MTEK_RESERVATIONS_REPORT_SLUG || "reservations",
  expectedHeaders: RESERVATIONS_EXPECTED_HEADERS,
  mapRow: mapReservationsRow,
  dedupKey: (row) => String(row["Reservation ID"]),
  dedupKeyColumns: ["Reservation ID"],
  dateParams: (minDate, maxDate) => ({ min_start_date_day: minDate, max_start_date_day: maxDate }),
  auditDateColumn: "Class Start Date",
  auditDateKind: "date", // real typed DATE column — no string-format risk
  formatColumns: [], // schema-typed columns, nothing to format-check
  junkFilterSql: "`Reservation ID` IS NOT NULL",
};

export const TABLES = { sales, firstTimers, reservations };
