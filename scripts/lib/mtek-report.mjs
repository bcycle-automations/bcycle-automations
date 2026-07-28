// scripts/lib/mtek-report.mjs
// Shared async MTEK "table report" fetcher: kicks off a report job, polls
// job_status, and fetches the S3 result the instant it's ready (the
// presigned link expires within minutes of the job completing).

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

export async function fetchMtekReport({
  baseUrl,
  token,
  reportId,
  slug,
  pageSize = 500,
  dateParams = {},
  pollIntervalMs = 2000,
  pollTimeoutMs = 60000,
}) {
  const authHeaders = {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.api+json",
  };

  const startUrl = new URL("/api/async_table_report_data", baseUrl);
  startUrl.searchParams.set("id", reportId);
  startUrl.searchParams.set("slug", slug);
  startUrl.searchParams.set("page_size", String(pageSize));
  for (const [key, value] of Object.entries(dateParams)) {
    startUrl.searchParams.set(key, value);
  }

  const startJson = await fetchJson(startUrl.toString(), { headers: authHeaders });
  const jobId = startJson?.data?.[0]?.id;
  if (!jobId) throw new Error(`Unexpected start-job response: ${JSON.stringify(startJson)}`);

  const statusUrl = new URL("/api/async_table_report_data/job_status", baseUrl);
  statusUrl.searchParams.set("id", String(jobId));

  const deadline = Date.now() + pollTimeoutMs;
  let s3Link = null;
  while (Date.now() < deadline) {
    const statusJson = await fetchJson(statusUrl.toString(), { headers: authHeaders });
    const status = statusJson?.data?.status;

    if (status === "complete") {
      s3Link = statusJson?.data?.s3_link;
      if (!s3Link) throw new Error(`Job complete but no s3_link: ${JSON.stringify(statusJson)}`);
      break;
    }
    if (status === "failed" || status === "error") {
      throw new Error(`MTEK report job ${jobId} failed: ${JSON.stringify(statusJson)}`);
    }
    await sleep(pollIntervalMs);
  }
  if (!s3Link) throw new Error(`MTEK report job ${jobId} did not complete within ${pollTimeoutMs}ms`);

  // Fetch immediately — presigned URL expires within minutes of job completion
  const report = await fetchJson(s3Link);
  const { headers, rows } = report;
  if (!Array.isArray(headers) || !Array.isArray(rows)) {
    throw new Error(`Unexpected report shape: ${JSON.stringify(Object.keys(report))}`);
  }
  return report;
}

export function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

// Default sync window: past N days through today (UTC)
export function pastDaysWindow(days) {
  const today = new Date();
  const start = new Date(today);
  start.setUTCDate(start.getUTCDate() - days);
  return { minDate: isoDate(start), maxDate: isoDate(today) };
}

export function addDaysUTC(dateStr, days) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return isoDate(d);
}

export function yesterdayUTC() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return isoDate(d);
}

// BigQuery DATE columns come back from the client as {value: "YYYY-MM-DD"}
export function bqDateToString(v) {
  if (v == null) return null;
  if (typeof v === "string") return v;
  if (typeof v === "object" && "value" in v) return v.value;
  return String(v);
}

// Builds the sync window starting at `sinceDate` (inclusive), capped at
// `maxDays`, and never extending past yesterday (UTC) — today's data is
// still incomplete when the job runs. Returns null if already caught up.
export function clampSyncWindow(sinceDate, maxDays = 7) {
  const yesterday = yesterdayUTC();
  if (sinceDate > yesterday) return null;
  const cappedEnd = addDaysUTC(sinceDate, maxDays - 1);
  const maxDate = cappedEnd < yesterday ? cappedEnd : yesterday;
  return { minDate: sinceDate, maxDate };
}

export function formatDisplayDate(dateStr) {
  return new Date(`${dateStr}T00:00:00Z`).toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  });
}
