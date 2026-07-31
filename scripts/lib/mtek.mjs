// scripts/lib/mtek.mjs
// Shared Mariana Tek (MTEK) API helpers: 429-aware fetch with backoff (same
// retry logic previously duplicated in find-second-class.js and
// send-mtek-reminders.js) and a small "collect every page" helper built on
// top of it.

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// MTEK: rate-limit aware fetch. On HTTP 429, waits for the Retry-After
// header (falling back to 2s) before retrying, up to maxRetries times.
export async function fetchJsonWithRateLimit(url, options = {}, maxRetries = 5) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const res = await fetch(url, options);

    if (res.status !== 429) {
      if (!res.ok) {
        const text = await res.text();
        throw new Error(
          `Request failed ${res.status} ${res.statusText} for ${url}: ${text}`
        );
      }
      return res.json();
    }

    const retryAfterHeader = res.headers.get("Retry-After");
    let delayMs = 0;

    if (retryAfterHeader) {
      const secs = Number(retryAfterHeader);
      if (!Number.isNaN(secs)) {
        delayMs = secs * 1000;
      } else {
        const retryDate = new Date(retryAfterHeader);
        if (!Number.isNaN(retryDate.getTime())) {
          delayMs = retryDate.getTime() - Date.now();
        }
      }
    }

    if (!delayMs || delayMs < 0) delayMs = 2000;

    console.warn(
      `Got 429 from MTEK for ${url}. Retry-After=${retryAfterHeader || "n/a"}; ` +
        `waiting ${Math.round(delayMs / 1000)}s before retry (attempt ${attempt}/${maxRetries})`
    );

    await sleep(delayMs);
  }

  throw new Error(`Exceeded max retries (${maxRetries}) for ${url} after repeated 429s.`);
}

// Walks every page of a MTEK list endpoint (page/page_size + meta.pagination.pages)
// and returns the concatenated `data` array.
export async function fetchAllPages(baseUrl, searchParams, headers) {
  const allData = [];
  let page = 1;
  let totalPages = 1;

  do {
    const url = new URL(baseUrl);
    for (const [key, value] of Object.entries(searchParams)) {
      url.searchParams.set(key, value);
    }
    url.searchParams.set("page", String(page));

    const body = await fetchJsonWithRateLimit(url, { headers });

    if (!Array.isArray(body?.data)) {
      throw new Error(`MTEK response for ${url} did not contain a data array.`);
    }

    allData.push(...body.data);
    totalPages = Number(body?.meta?.pagination?.pages || 1);
    page += 1;
  } while (page <= totalPages);

  return allData;
}
