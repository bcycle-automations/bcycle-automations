// Promo class-tag cleanup (b.cycle + SPINCO)
//
// The "Apply Class Tag" button in Airtable (base apploOWxaBgUZa4cz, table
// "Credit Package Expiry") tags every class in a package's date window. Because
// Mariana Tek applies class-session tag/untag to "this and following" sessions
// of a recurring series, the tag spills onto every later class in the series.
//
// Rule enforced here: an upcoming class may carry the promo tag ONLY if its
// local start date falls inside a Credit Package Expiry row window
// (Start date → Final date of package availability) for the same company,
// tag, and location (b.cycle rows have no location = all locations).
// SPINCO classes also tagged Event / Spin-It-Forward never keep the promo tag
// (the apply scenario skips them).
//
// Because untag/tag cascade forward, each recurring series is walked in date
// order with its tag state simulated in memory: untag where the tag must stop,
// re-tag only where a class ORIGINALLY had the tag and still should (so this
// script never introduces a tag the apply button didn't put there).
//
// Env:
//   MTEK_API_TOKEN, MTEK_SPINCO_API_TOKEN   Mariana Tek tokens
//   AIRTABLE_TOKEN                          Airtable PAT (skipped if ROWS_JSON set)
//   ROWS_JSON                               optional local rows file (testing)
//   DRY_RUN                                 "false" to write (default: dry run)
//   COMPANIES                               optional, e.g. "b.cycle" or "SPINCO"

import fs from "node:fs";

const DRY_RUN = process.env.DRY_RUN !== "false";
const AIRTABLE_BASE_ID = "apploOWxaBgUZa4cz";
const AIRTABLE_TABLE_ID = "tblXHMqDXeKnJOPjo";
const TZ = "America/Toronto";

const COMPANIES = [
  {
    name: "b.cycle",
    base: "https://bcycle.marianatek.com/api",
    token: process.env.MTEK_API_TOKEN,
    tag: "5001",
    protectedTags: [],
  },
  {
    name: "SPINCO",
    base: "https://spinco.marianatek.com/api",
    token: process.env.MTEK_SPINCO_API_TOKEN,
    tag: "5035",
    protectedTags: ["4904", "4937"], // Event, Spin-It-Forward
  },
].filter((c) => !process.env.COMPANIES || process.env.COMPANIES.split(",").includes(c.name));

const today = new Intl.DateTimeFormat("en-CA", { timeZone: TZ }).format(new Date()); // YYYY-MM-DD
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- Airtable ----------

async function loadRows() {
  if (process.env.ROWS_JSON) return JSON.parse(fs.readFileSync(process.env.ROWS_JSON, "utf8"));
  if (!process.env.AIRTABLE_TOKEN) throw new Error("Missing AIRTABLE_TOKEN");
  const rows = [];
  let offset;
  do {
    const url = new URL(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    const res = await fetch(url, { headers: { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` } });
    if (!res.ok) throw new Error(`Airtable ${res.status}: ${await res.text()}`);
    const data = await res.json();
    for (const r of data.records) {
      const f = r.fields;
      rows.push({
        id: r.id,
        name: f["Package name"],
        company: f["Company"],
        tag: f["Tag ID MTEK"],
        location: f["Location ID"],
        start: f["Start date"],
        end: f["Final date of package availability"],
        status: f["Status"],
        tagRemoved: Boolean(f["Promo tag removed"]),
      });
    }
    offset = data.offset;
  } while (offset);
  return rows;
}

async function markRowsComplete(rows) {
  if (!rows.length || DRY_RUN || process.env.ROWS_JSON) return;
  for (let i = 0; i < rows.length; i += 10) {
    const batch = rows.slice(i, i + 10).map((r) => ({ id: r.id, fields: { Status: "COMPLETE", "Promo tag removed": true } }));
    const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}`, {
      method: "PATCH",
      headers: { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify({ records: batch }),
    });
    if (!res.ok) throw new Error(`Airtable update ${res.status}: ${await res.text()}`);
  }
}

// ---------- Mariana Tek ----------

async function mtek(company, path, init = {}) {
  for (let attempt = 1; ; attempt++) {
    let res;
    try {
      res = await fetch(`${company.base}${path}`, {
        ...init,
        headers: { Authorization: `Bearer ${company.token}`, "Content-Type": "application/vnd.api+json", ...(init.headers || {}) },
      });
      if (res.ok) return res.status === 204 ? null : await res.json();
    } catch (err) {
      // network drop (connect or mid-body): retry
      if (attempt < 6) {
        await sleep(2000 * attempt);
        continue;
      }
      throw err;
    }
    if ((res.status === 429 || res.status >= 500) && attempt < 6) {
      await sleep(2000 * attempt);
      continue;
    }
    throw new Error(`${company.name} ${init.method || "GET"} ${path} → ${res.status}: ${(await res.text()).slice(0, 300)}`);
  }
}

async function loadSessions(company) {
  const sessions = [];
  const minDatetime = `${today}T00:00:00Z`;
  for (let page = 1; ; page++) {
    const d = await mtek(company, `/class_sessions?min_datetime=${minDatetime}&page_size=200&page=${page}`);
    for (const s of d.data) {
      if (s.attributes.start_date < today) continue;
      sessions.push({
        id: s.id,
        date: s.attributes.start_date,
        datetime: s.attributes.start_datetime,
        recurringId: s.attributes.recurring_id,
        location: s.relationships.location?.data?.id,
        locationName: s.attributes.location_display,
        tags: (s.relationships.tags?.data || []).map((t) => t.id),
      });
    }
    if (page >= d.meta.pagination.pages) break;
  }
  return sessions;
}

const setTag = (company, sessionId, action) =>
  mtek(company, `/class_sessions/${sessionId}/${action}`, {
    method: "POST",
    body: JSON.stringify({ data: { type: "class_sessions", attributes: { tag: company.tag } } }),
  });

// ---------- Planning ----------

function buildWindows(rows, company) {
  return rows
    .filter((r) => r.company === company.name && String(r.tag || "").trim() === company.tag && r.start && r.end)
    .map((r) => ({ location: String(r.location || "").trim() || "*", start: r.start, end: r.end, name: r.name }));
}

function shouldHaveTag(session, windows, company) {
  if (session.tags.some((t) => company.protectedTags.includes(t))) return false;
  return windows.some(
    (w) => (w.location === "*" || w.location === session.location) && w.start <= session.date && session.date <= w.end
  );
}

// Returns ordered ops [{session, action}] for one series (or a lone session).
function planSeries(sessions, windows, company) {
  const ops = [];
  let cascaded = null; // null = no op yet; otherwise the state our last op pushed forward
  for (const s of sessions) {
    const original = s.tags.includes(company.tag);
    const current = cascaded === null ? original : cascaded;
    const desired = shouldHaveTag(s, windows, company);
    if (current && !desired) {
      ops.push({ session: s, action: "untag" });
      cascaded = false;
    } else if (!current && desired && original) {
      ops.push({ session: s, action: "tag" });
      cascaded = true;
    }
  }
  return ops;
}

function plan(sessions, windows, company) {
  const groups = new Map();
  for (const s of sessions) {
    const key = s.recurringId ? `r${s.recurringId}` : `s${s.id}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(s);
  }
  const ops = [];
  for (const group of groups.values()) {
    group.sort((a, b) => (a.datetime < b.datetime ? -1 : 1));
    ops.push(...planSeries(group, windows, company));
  }
  return ops;
}

// ---------- Main ----------

const rows = await loadRows();
console.log(`Today (${TZ}): ${today} · ${DRY_RUN ? "DRY RUN" : "LIVE"} · ${rows.length} Airtable rows`);

let failures = 0;
for (const company of COMPANIES) {
  if (!company.token) throw new Error(`Missing token for ${company.name}`);
  const windows = buildWindows(rows, company);
  const sessions = await loadSessions(company);
  const tagged = sessions.filter((s) => s.tags.includes(company.tag));
  const wrong = tagged.filter((s) => !shouldHaveTag(s, windows, company));
  const ops = plan(sessions, windows, company);

  console.log(`\n== ${company.name} (tag ${company.tag}) ==`);
  console.log(`windows: ${windows.length} · upcoming classes: ${sessions.length} · tagged: ${tagged.length} · tagged outside a window: ${wrong.length}`);
  console.log(`planned calls: ${ops.filter((o) => o.action === "untag").length} untag, ${ops.filter((o) => o.action === "tag").length} re-tag`);

  if (DRY_RUN) continue;
  let done = 0;
  for (const op of ops) {
    try {
      await setTag(company, op.session.id, op.action);
      done++;
    } catch (err) {
      // Logged only: the verify step below decides whether the run is clean
      // (e.g. MTEK rejects edits on classes whose waitlist exceeds its cap, but a
      // cascade from an earlier class may still have fixed that class).
      console.error(`FAILED ${op.action} ${op.session.id} (${op.session.locationName} ${op.session.date}): ${err.message}`);
    }
    await sleep(150);
  }
  console.log(`applied ${done}/${ops.length} calls`);

  // Verify against a fresh read
  const after = await loadSessions(company);
  const stillWrong = after.filter((s) => s.tags.includes(company.tag) && !shouldHaveTag(s, windows, company));
  const lost = after.filter((s) => {
    const before = sessions.find((b) => b.id === s.id);
    return before?.tags.includes(company.tag) && !s.tags.includes(company.tag) && shouldHaveTag(s, windows, company);
  });
  console.log(`verify: ${stillWrong.length} still tagged outside a window · ${lost.length} in-window classes lost their tag`);
  for (const s of stillWrong.slice(0, 20)) console.log(`  still tagged: ${s.id} ${s.locationName} ${s.date}`);
  for (const s of lost.slice(0, 20)) console.log(`  lost tag: ${s.id} ${s.locationName} ${s.date}`);
  if (stillWrong.length || lost.length) failures++;
}

// Close out rows whose window has ended, once every company ran cleanly.
if (!failures && COMPANIES.length === 2) {
  const promoTags = COMPANIES.map((c) => c.tag);
  const ended = rows.filter(
    (r) => r.end && r.end < today && promoTags.includes(String(r.tag || "").trim()) && (r.status !== "COMPLETE" || !r.tagRemoved)
  );
  console.log(`\nMarking ${ended.length} ended rows COMPLETE${DRY_RUN || process.env.ROWS_JSON ? " (skipped: dry run / local rows)" : ""}`);
  await markRowsComplete(ended);
}

if (failures) {
  console.error(`\nFinished with ${failures} problem(s).`);
  process.exit(1);
}
