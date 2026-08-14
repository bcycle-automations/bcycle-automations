// scripts/lib/spinco-mtek-tables.mjs
// Per-table config registry for SPINCO's three MTEK -> BigQuery tables (Sales,
// First Timers, reservations_raw_2025, dataset root-cargo-453703-k7.SPINCO).
// Single source of truth for each table's MTEK report shape, column mapping, and
// dedup key — shared by the three weekly sync scripts
// (scripts/spinco-*-to-bigquery.mjs) AND scripts/spinco-mtek-monthly-data-audit.mjs.
//
// This is SPINCO-specific — b.cycle's sibling tables (dataset SalesZF) have their
// own separate shapes in scripts/lib/bcycle-mtek-tables.mjs and are NOT covered
// here, matching this repo's established convention of keeping the two companies'
// automations fully separate rather than cross-importing.

import { toTime12h, toMDYYYY } from "./format.mjs";

// ---------------------------------------------------------------------------
// Sales — already real typed columns (DATE/BOOLEAN/INTEGER/FLOAT), no
// string-date-formatting risk.

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

// Exact BigQuery column names (positional match to SALES_EXPECTED_HEADERS)
const SALES_BQ_COLUMNS = [
  "Order Number",
  "Order Status",
  "Order Date _UTC_",
  "Order Time _UTC_",
  "Order Device",
  "Processed by System_",
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
  "Order Payment Method_s_",
  "Purchase Location",
  "Fulfillment Location",
  "Fulfillment Region",
];

const SALES_DATE_ONLY_COLUMNS = new Set(["Order Date _UTC_", "Transaction Date"]);
const SALES_FLOAT_COLUMNS = new Set(["Product Barcode", "Line Subtotal", "Original Price", "Line Tax", "Line Total"]);
const SALES_INT_COLUMNS = new Set(["Customer ID", "Broker ID", "Product ID", "Variant ID", "Line Quantity"]);

function passthrough(v) {
  return v === undefined ? null : v;
}

function toDateOnly(v) {
  if (v === null || v === undefined) return null;
  return String(v).slice(0, 10);
}

function toFloat(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

function toInt(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = parseInt(v, 10);
  return Number.isNaN(n) ? null : n;
}

function mapSalesRow(row) {
  const out = {};
  SALES_BQ_COLUMNS.forEach((bqCol, i) => {
    const raw = row[i];
    if (SALES_DATE_ONLY_COLUMNS.has(bqCol)) {
      out[bqCol] = toDateOnly(raw);
    } else if (SALES_FLOAT_COLUMNS.has(bqCol)) {
      out[bqCol] = toFloat(raw);
    } else if (SALES_INT_COLUMNS.has(bqCol)) {
      out[bqCol] = toInt(raw);
    } else {
      out[bqCol] = passthrough(raw);
    }
  });
  return out;
}

// Same composite key validated for b.cycle's Sales — refunds share
// Order/Product/Variant with their original purchase line, sometimes same-day.
// Note: ~132 pre-existing collisions confirmed in the baseline data even under
// this full composite key — a documented pre-existing data quality quirk, not
// introduced by the sync. The audit's report-only key-collision check may
// surface some of these; that's expected, not a new bug.
function salesDedupKey(row) {
  return `${row["Order Number"]}|${row["Product ID"]}|${row["Variant ID"]}|${row["Transaction Date"]}|${row["Line Status"]}|${row["Line Quantity"]}`;
}

const sales = {
  key: "sales",
  label: "Sales",
  bqTable: process.env.SPINCO_BQ_TABLE_SALES || "Sales",
  reportId: process.env.MTEK_SPINCO_SALES_REPORT_ID || "383",
  reportSlug: process.env.MTEK_SPINCO_SALES_REPORT_SLUG || "orders-utc",
  expectedHeaders: SALES_EXPECTED_HEADERS,
  mapRow: mapSalesRow,
  dedupKey: salesDedupKey,
  dedupKeyColumns: ["Order Number", "Product ID", "Variant ID", "Transaction Date", "Line Status", "Line Quantity"],
  dateParams: (minDate, maxDate) => ({ min_transaction_date: minDate, max_transaction_date: maxDate }),
  auditDateColumn: "Transaction Date",
  auditDateKind: "date",
  formatColumns: [],
  junkFilterSql: "TRUE", // no leaked-header row confirmed in this table
};

// ---------------------------------------------------------------------------
// First Timers — already real typed columns (DATE/BOOLEAN/INTEGER), no
// string-date-formatting risk. Table name has a space: `First Timers`.

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
    Customer: passthrough(customer),
    "Customer ID": passthrough(customerId),
    "Customer Email": passthrough(email),
    "Customer Phone Number": passthrough(phone),
    "Join Date": toDateOnly(joinDate),
    "First Class Date": toDateOnly(firstClassDate),
    "Class Start Time": toTime12h(classStartTime),
    "Class Location": passthrough(location),
    "Class Type": passthrough(classType),
    "Class Category": passthrough(classCategory),
    "Instructor Name": passthrough(instructor),
    "Guest Reservation_": passthrough(guestReservation),
    "Pay With Type": passthrough(payWithType),
  };
}

const firstTimers = {
  key: "firstTimers",
  label: "First Timers",
  bqTable: process.env.SPINCO_BQ_TABLE_FIRST_TIMERS || "First Timers",
  reportId: process.env.MTEK_SPINCO_FIRST_TIMERS_REPORT_ID || "317",
  reportSlug: process.env.MTEK_SPINCO_FIRST_TIMERS_REPORT_SLUG || "first-timers",
  expectedHeaders: FIRST_TIMERS_EXPECTED_HEADERS,
  mapRow: mapFirstTimersRow,
  dedupKey: (row) => String(row["Customer ID"]),
  dedupKeyColumns: ["Customer ID"],
  dateParams: (minDate, maxDate) => ({ min_first_class_date_day: minDate, max_first_class_date_day: maxDate }),
  auditDateColumn: "First Class Date",
  auditDateKind: "date",
  formatColumns: [],
  junkFilterSql: "TRUE",
};

// ---------------------------------------------------------------------------
// reservations_raw_2025 — per explicit instruction, kept exactly as originally
// imported: every field STRING-typed (even numbers/booleans/dates), lowercase
// snake_case column names. This is the one table with real date-formatting
// risk: class_start_date/creation_date/updated_date/cancelled_date are a
// confirmed live mix of ISO "YYYY-MM-DD" and US "M/D/YYYY" (padded/unpadded).

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

// Positional match to RESERVATIONS_EXPECTED_HEADERS above
const RESERVATIONS_BQ_COLUMNS = [
  "reservation_id", "reservation_type", "status", "creation_date", "updated_date",
  "cancelled_date", "unconverted_guest", "converted_guest", "guest", "guest_email",
  "credit_payment_origin", "credit", "package", "membership", "contract",
  "class_id", "class_start_date", "class_start_time", "class_day_of_week", "class_custom_name",
  "class_is_public", "class_tags", "class_is_free", "class_type", "class_category",
  "class_duration_minutes", "layout", "layout_format", "layout_capacity", "class_capacity",
  "classroom", "location", "region", "instructor_ids", "instructor_employee_ids",
  "instructor_names", "instructor_schedule_names", "class_has_substitute", "customer_id", "customer_email",
  "customer_name", "company_name", "payer_id", "payer_email", "payer_name",
  "broker_id", "broker_email", "broker_name", "spot", "spot_type",
];

const RESERVATIONS_DATE_COLUMNS = new Set(["creation_date", "updated_date", "cancelled_date", "class_start_date"]);
const RESERVATIONS_TIME_COLUMNS = new Set(["class_start_time"]);
const RESERVATIONS_BOOL_STRING_COLUMNS = new Set([
  "unconverted_guest",
  "converted_guest",
  "guest",
  "class_is_public",
  "class_is_free",
  "class_has_substitute",
]);

function toStr(v) {
  if (v === null || v === undefined) return null;
  return String(v);
}

function toBoolString(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "boolean") return String(v);
  return String(v).toLowerCase() === "true" ? "true" : "false";
}

function mapReservationsRow(row) {
  const out = {};
  RESERVATIONS_BQ_COLUMNS.forEach((bqCol, i) => {
    const raw = row[i];
    if (RESERVATIONS_DATE_COLUMNS.has(bqCol)) {
      out[bqCol] = toMDYYYY(raw);
    } else if (RESERVATIONS_TIME_COLUMNS.has(bqCol)) {
      out[bqCol] = toTime12h(raw);
    } else if (RESERVATIONS_BOOL_STRING_COLUMNS.has(bqCol)) {
      out[bqCol] = toBoolString(raw);
    } else {
      out[bqCol] = toStr(raw);
    }
  });
  return out;
}

const reservations = {
  key: "reservations",
  label: "Reservations",
  bqTable: process.env.SPINCO_BQ_TABLE_RESERVATIONS || "reservations_raw_2025",
  reportId: process.env.MTEK_SPINCO_RESERVATIONS_REPORT_ID || "341",
  reportSlug: process.env.MTEK_SPINCO_RESERVATIONS_REPORT_SLUG || "reservations",
  expectedHeaders: RESERVATIONS_EXPECTED_HEADERS,
  mapRow: mapReservationsRow,
  dedupKey: (row) => String(row.reservation_id),
  dedupKeyColumns: ["reservation_id"],
  dateParams: (minDate, maxDate) => ({ min_start_date_day: minDate, max_start_date_day: maxDate }),
  auditDateColumn: "class_start_date",
  // Confirmed live: a genuine mix of ISO and US date-string formats, not just
  // padding — needs the dual-format tolerant parser from spinco-mtek-sql-dates.mjs.
  auditDateKind: "string-mixed",
  formatColumns: ["class_start_date", "creation_date", "updated_date", "cancelled_date"],
  junkFilterSql: "TRUE",
};

export const TABLES = { sales, firstTimers, reservations };
