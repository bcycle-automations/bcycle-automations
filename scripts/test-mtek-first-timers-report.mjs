// scripts/test-mtek-first-timers-report.mjs
// Exploratory test: confirms the async MTEK report flow works end to end for
// the "First Timers" report (id 317, slug first-timers) — kicks off the job,
// polls job_status, fetches the S3 result the moment it's ready (the link
// expires within minutes), and prints shape/counts only (no raw PII to stdout).
// Not wired to BigQuery yet — this is just a connectivity/shape check.

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();

const REPORT_ID = process.env.MTEK_FIRST_TIMERS_REPORT_ID || "317";
const REPORT_SLUG = process.env.MTEK_FIRST_TIMERS_REPORT_SLUG || "first-timers";
const PAGE_SIZE = Number(process.env.MTEK_REPORT_PAGE_SIZE || "500");

// Default window: past 7 days (matches the eventual weekly-sync cadence)
function isoDate(d) {
  return d.toISOString().slice(0, 10);
}
const today = new Date();
const weekAgo = new Date(today);
weekAgo.setUTCDate(weekAgo.getUTCDate() - 7);

const MIN_DATE = process.env.TEST_MIN_DATE || isoDate(weekAgo);
const MAX_DATE = process.env.TEST_MAX_DATE || isoDate(today);

const POLL_INTERVAL_MS = 2000;
const POLL_TIMEOUT_MS = 60000;

function authHeaders() {
  return {
    Authorization: `Bearer ${MTEK_API_TOKEN}`,
    Accept: "application/vnd.api+json",
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed ${res.status} ${res.statusText} for ${url}: ${text}`);
  }
  return res.json();
}

async function startJob() {
  const url = new URL("/api/async_table_report_data", MTEK_BASE_URL);
  url.searchParams.set("id", REPORT_ID);
  url.searchParams.set("slug", REPORT_SLUG);
  url.searchParams.set("page_size", String(PAGE_SIZE));
  url.searchParams.set("min_first_class_date_day", MIN_DATE);
  url.searchParams.set("max_first_class_date_day", MAX_DATE);

  console.log(`Starting job: ${url.toString()}`);
  const json = await fetchJson(url.toString(), { headers: authHeaders() });
  const jobId = json?.data?.[0]?.id;
  if (!jobId) throw new Error(`Unexpected start-job response: ${JSON.stringify(json)}`);
  return jobId;
}

async function pollUntilComplete(jobId) {
  const url = new URL("/api/async_table_report_data/job_status", MTEK_BASE_URL);
  url.searchParams.set("id", String(jobId));

  const deadline = Date.now() + POLL_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const json = await fetchJson(url.toString(), { headers: authHeaders() });
    const status = json?.data?.status;
    console.log(`  job ${jobId} status: ${status}`);

    if (status === "complete") {
      const link = json?.data?.s3_link;
      if (!link) throw new Error(`Job complete but no s3_link: ${JSON.stringify(json)}`);
      return link;
    }
    if (status === "failed" || status === "error") {
      throw new Error(`Job ${jobId} failed: ${JSON.stringify(json)}`);
    }
    await sleep(POLL_INTERVAL_MS);
  }
  throw new Error(`Job ${jobId} did not complete within ${POLL_TIMEOUT_MS}ms`);
}

// Mask PII for terminal output — full data still gets saved to the local file
function redactRow(headers, row) {
  return headers.map((h, i) => {
    const v = row[i];
    if (v == null) return v;
    const key = h.toLowerCase();
    if (key.includes("email")) return String(v).replace(/^(.).*(@.*)$/, "$1***$2");
    if (key.includes("phone")) return String(v).replace(/\d(?=\d{2})/g, "*");
    if (key === "customer") return "[redacted name]";
    return v;
  });
}

async function main() {
  console.log("MTEK_BASE_URL:", MTEK_BASE_URL);
  console.log(
    "MTEK_API_TOKEN present?",
    MTEK_API_TOKEN ? `yes (len=${MTEK_API_TOKEN.length})` : "NO"
  );
  console.log(`Date window: ${MIN_DATE} .. ${MAX_DATE}`);

  if (!MTEK_API_TOKEN) throw new Error("Missing MTEK_API_TOKEN (set it in .env)");

  const jobId = await startJob();
  const s3Link = await pollUntilComplete(jobId);

  console.log("Job complete, fetching S3 result immediately...");
  const report = await fetchJson(s3Link); // no auth headers — presigned URL

  const { headers, rows, total_records } = report;
  if (!Array.isArray(headers) || !Array.isArray(rows)) {
    throw new Error(`Unexpected report shape: ${JSON.stringify(Object.keys(report))}`);
  }

  console.log(`\nReport: ${report.name} (id ${report.id})`);
  console.log(`Headers (${headers.length}):`, headers);
  console.log(`Rows: ${rows.length} (total_records field says: ${total_records})`);

  if (rows.length > 0) {
    console.log("Sample row (redacted):", redactRow(headers, rows[0]));
  }

  // Save the full (unredacted) result locally for schema work — gitignored
  const fs = await import("node:fs/promises");
  await fs.mkdir("tmp-reports", { recursive: true });
  const outPath = `tmp-reports/first-timers-${MIN_DATE}_${MAX_DATE}.json`;
  await fs.writeFile(outPath, JSON.stringify(report, null, 2));
  console.log(`\nFull result saved locally to ${outPath} (gitignored, not shown here)`);
}

main().catch((err) => {
  console.error("Test failed:", err.message);
  process.exit(1);
});
