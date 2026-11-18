#!/usr/bin/env node
/**
 * Subs - Auto-correct future classes (Mon / Wed / Fri).
 *
 * Does by itself what a person does on the Softr page /subs-schedule-changes
 * ("1. Check for changes" -> tick everything -> "Apply"), for the days that matter most:
 * today -> today + 60 days, but never further than the last date that was imported for that studio
 * on /subs-import-mtek (Import Runs: Mode = Import, Status Completed / Completed with warnings).
 *
 * Then it posts to every studio's Slack channel (Studios table, "Slack channel" id, e.g. #vieux-port-notifs) a Block Kit
 * message listing the classes that were updated and telling them to put it back in Airtable
 * (https://bcycle.softr.app/all-classes) if it should not have been done.
 *
 * Why it is cheap on API calls (about 60 Airtable calls + 1 MTEK report per run, regardless of studios):
 *   - ONE MTEK report for the whole window (all studios), filtered per studio in memory.
 *   - ONE read of All Classes for the window, ONE read of the employees, ONE read of Import Runs.
 *   - Writes are batched 10 records per call. The change itself is made by the existing Airtable automation
 *     "Fix schedule (NEW)" (we tick "Fix" on rows of the staging table, same as the Apply button does), so there
 *     is no per-class API call and its run history stays the audit log.
 *   - The MTEK class_sessions list is only fetched when a class is missing from Airtable.
 *
 * Safety:
 *   - A class that was auto-corrected before and that someone then put back by hand is LEFT ALONE
 *     (state/subs-auto-correct-state.json remembers "class -> MTEK instructor already applied"). It is only
 *     touched again if MTEK itself changes to something else.
 *   - If more than MAX_AUTO_CHANGES changes are found in one run, nothing is applied (probably a bad MTEK report)
 *     and the admin is emailed.
 *   - Changes whose instructor name matches no employee's "MTEK Name" are not applied (new time slots are still added).
 *
 * Env: AIRTABLE_TOKEN, AIRTABLE_BASE_ID (Instructors - Subs), MTEK_API_TOKEN,
 *      SLACK_BOT_TOKEN (bot with chat:write + chat:write.public, and im:write for the DM),
 *      SLACK_ADMIN_USER_ID (Slack user id that gets the DM: problems, and the whole dry run),
 *      optional: DRY_RUN=true (reads only: nothing is written; the messages that WOULD be posted are DM'd to the admin),
 *                ONLY_STUDIO (limit to one studio, for tests), WINDOW_DAYS (default 60), MAX_AUTO_CHANGES (default 200).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fetchMtekReport } from "./lib/mtek-report.mjs";

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const BASE_ID = process.env.AIRTABLE_BASE_ID || "applRovmu340OSBSg";
const RUNS_TABLE = process.env.AIRTABLE_RUNS_TABLE_ID || "tblGRf0nyYyCo1bXT";
const CLASSES_TABLE = process.env.AIRTABLE_CLASSES_TABLE_ID || "tblhoowrSqrGUwg6d";
const EMPLOYEES_TABLE = process.env.AIRTABLE_EMPLOYEES_TABLE_ID || "tblVNgEoR1oQQETdF";
const STAGING_TABLE = process.env.AIRTABLE_STAGING_TABLE_ID || "tblTutoySkMiu7TvD";
const STUDIOS_TABLE = process.env.AIRTABLE_STUDIOS_TABLE_ID || "tblsQJddzPnF6MNx2";
const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const MTEK_REPORT_ID = "294";
const MTEK_REPORT_SLUG = "utilization";
const TZ = "America/Toronto";
const DRY_RUN = /^(1|true|yes)$/i.test(process.env.DRY_RUN || "");
const ONLY_STUDIO = (process.env.ONLY_STUDIO || "").trim();
const WINDOW_DAYS = Number(process.env.WINDOW_DAYS || 60);
const MAX_AUTO_CHANGES = Number(process.env.MAX_AUTO_CHANGES || 200);
const SLACK_TOKEN = (process.env.SLACK_BOT_TOKEN || "").trim();
const ADMIN_USER = (process.env.SLACK_ADMIN_USER_ID || "").trim();
const ALL_CLASSES_URL = "https://bcycle.softr.app/all-classes";
const ACTIVE_INSTRUCTORS_URL = "https://bcycle.softr.app/active-instructors";
const STATE_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../state/subs-auto-correct-state.json");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const norm = (s) => String(s ?? "").trim().toLowerCase();
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
const log = (...a) => console.log(new Date().toLocaleTimeString("en-CA", { timeZone: TZ, hour12: false }), ...a);

/* ------------------------------------------------------------------ */
/* Airtable                                                            */
/* ------------------------------------------------------------------ */

async function at(method, p, body, query) {
  const url = new URL(`https://api.airtable.com/v0/${p}`);
  for (const [k, v] of Object.entries(query || {})) {
    if (Array.isArray(v)) v.forEach((x) => url.searchParams.append(k, x));
    else if (v != null) url.searchParams.set(k, v);
  }
  for (let attempt = 1; attempt <= 5; attempt++) {
    const res = await fetch(url, {
      method,
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}`, "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (res.status === 429 && attempt < 5) {
      await sleep(1500 * attempt);
      continue;
    }
    const text = await res.text();
    let json;
    try {
      json = text ? JSON.parse(text) : {};
    } catch {
      json = { raw: text };
    }
    if (!res.ok) {
      const msg = json?.error?.message || json?.error?.type || text.slice(0, 300);
      throw new Error(`Airtable ${method} ${p} -> ${res.status}: ${msg}`);
    }
    return json;
  }
  throw new Error("unreachable");
}

async function atListAll(tableId, query) {
  const out = [];
  let offset;
  do {
    const json = await at("GET", `${BASE_ID}/${tableId}`, null, { pageSize: 100, ...query, offset });
    out.push(...(json.records || []));
    offset = json.offset;
    if (offset) await sleep(220);
  } while (offset);
  return out;
}

const q = (s) => String(s).replace(/'/g, "\\'");
const orIds = (ids) => `OR(${ids.map((id) => `RECORD_ID()='${id}'`).join(",")})`;

/* ------------------------------------------------------------------ */
/* Dates / titles (America/Toronto)                                    */
/* ------------------------------------------------------------------ */

const ymdFromDate = (d) => new Intl.DateTimeFormat("en-CA", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
function addDays(ymd, n) {
  const d = new Date(`${ymd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
function torontoMidnightIso(ymd) {
  const probe = new Date(`${ymd}T05:00:00Z`);
  const part = new Intl.DateTimeFormat("en-US", { timeZone: TZ, timeZoneName: "shortOffset" }).formatToParts(probe).find((x) => x.type === "timeZoneName").value;
  const hrs = Number(part.replace("GMT", "") || 0);
  return new Date(new Date(`${ymd}T00:00:00Z`).getTime() - hrs * 3600000).toISOString();
}
function hhmm(t) {
  const m = String(t ?? "").match(/^(\d{1,2}):(\d{2})/);
  return m ? `${m[1].padStart(2, "0")}:${m[2]}` : "";
}
// "26-10-12 - 05:30pm - Westmount - BARRE" -> { ymd, hhmm, room, cls }
function parseTitle(title) {
  const m = String(title ?? "").match(/^(\d{2})-(\d{2})-(\d{2}) - (\d{1,2}):(\d{2})\s?(am|pm) - (.+?) - (.+)$/i);
  if (!m) return null;
  let h = Number(m[4]) % 12;
  if (m[6].toLowerCase() === "pm") h += 12;
  return { ymd: `20${m[1]}-${m[2]}-${m[3]}`, hhmm: `${String(h).padStart(2, "0")}:${m[5]}`, room: m[7].trim(), cls: m[8].trim() };
}
const keyOf = (room, ymd, time, cls) => `${norm(room)}|${ymd}|${time}|${norm(cls)}`;
const ddmmyyyy = (ymd) => `${ymd.slice(8, 10)}/${ymd.slice(5, 7)}/${ymd.slice(0, 4)}`;
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const DOW = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
function prettyWhen(ymd, time) {
  const [y, m, d] = ymd.split("-").map(Number);
  let [h, mi] = time.split(":").map(Number);
  const ap = h >= 12 ? "pm" : "am";
  h = h % 12 || 12;
  const dow = DOW[new Date(Date.UTC(y, m - 1, d)).getUTCDay()];
  return `${dow} ${MONTHS[m - 1]} ${d}, ${h}:${String(mi).padStart(2, "0")}${ap}`;
}

/* ------------------------------------------------------------------ */
/* MTEK class sessions (only used when a class is missing in Airtable) */
/* ------------------------------------------------------------------ */

async function mtekGet(p, params) {
  const url = new URL(`${MTEK_BASE_URL}${p}`);
  for (const [k, v] of Object.entries(params || {})) url.searchParams.set(k, v);
  for (let attempt = 1; attempt <= 5; attempt++) {
    const res = await fetch(url, { headers: { Authorization: `Bearer ${MTEK_TOKEN}`, Accept: "application/vnd.api+json" } });
    if (res.status === 429) {
      await sleep(Number(res.headers.get("retry-after") || 2) * 1000);
      continue;
    }
    if (!res.ok) throw new Error(`MTEK ${p} -> ${res.status}: ${(await res.text()).slice(0, 200)}`);
    return res.json();
  }
  throw new Error(`MTEK ${p}: rate limited`);
}
async function fetchClassSessions(minYmd, maxYmd) {
  const minIso = new Date(new Date(torontoMidnightIso(minYmd)).getTime() - 3 * 3600000).toISOString();
  const maxIso = new Date(new Date(torontoMidnightIso(addDays(maxYmd, 1))).getTime() + 3 * 3600000).toISOString();
  const sessions = [];
  for (let page = 1; page <= 200; page++) {
    const json = await mtekGet("/api/class_sessions", { page_size: "100", page: String(page), min_datetime: minIso, max_datetime: maxIso });
    sessions.push(...(json.data || []));
    if (page >= (json?.meta?.pagination?.pages || 1)) break;
    await sleep(150);
  }
  return sessions;
}

/* ------------------------------------------------------------------ */
/* Slack (Block Kit)                                                   */
/* ------------------------------------------------------------------ */

async function slackPost(channel, text, blocks, attachments) {
  if (DRY_RUN || !channel || channel === ADMIN_USER) {
    log(`(Slack post not sent${DRY_RUN ? " - dry run" : " - no DMs"}): ${text}`);
    return;
  }
  const res = await fetch("https://slack.com/api/chat.postMessage", {
    method: "POST",
    headers: { Authorization: `Bearer ${SLACK_TOKEN}`, "Content-Type": "application/json; charset=utf-8" },
    body: JSON.stringify({ channel, text, blocks, attachments, unfurl_links: false }),
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok || !j.ok) throw new Error(`Slack chat.postMessage to ${channel} failed: ${j.error || res.status}`);
}

const mrk = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

const classLink = (c) => {
  const id = c.recId || c.rec?.id;
  return id ? `${ALL_CLASSES_URL}?class=${id}&date=${c.ymd}` : "";
};
const EMOJI = { instructor: "🔄", sub: "🤝", blank: "🆕", new: "➕" };

/**
 * Block Kit message for one studio (each class time links to that class's details in ALL CLASSES). Layout: header + one-line summary on top, then (inside an amber attachment)
 * a native Slack table (Class / Type / Was / Now / Change), then how to revert + a button.
 * `variant: "list"` is the fallback when Slack refuses the table block: same info, grouped by day.
 */
function studioMessage(studio, items, { dry, variant = "table" } = {}) {
  const counts = {};
  for (const c of items) counts[c.cat] = (counts[c.cat] || 0) + 1;
  const summary = Object.entries(counts)
    .map(([k, n]) => `${EMOJI[k] || "•"} ${n} ${KIND_LABEL[k].toLowerCase()}`)
    .join("  ·  ");
  const top = [
    { type: "header", text: { type: "plain_text", text: `📅 Schedule auto-update · ${studio}`.slice(0, 150), emoji: true } },
    {
      type: "section",
      text: { type: "mrkdwn", text: `${dry ? "🧪 *DRY RUN – nothing was changed.* " : ""}*${plural(items.length, "class", "classes")}* ${dry ? "would be" : items.length === 1 ? "was" : "were"} updated in Airtable to match MTEK.\n${summary}` },
    },
  ];

  let body;
  if (variant === "table") {
    const cell = (t, bold) => (bold ? { type: "rich_text", elements: [{ type: "rich_text_section", elements: [{ type: "text", text: String(t) || " ", style: { bold: true } }] }] } : { type: "raw_text", text: String(t) || " " });
    const rows = [[cell("Class", true), cell("Type", true), cell("Was", true), cell("Now (MTEK)", true), cell("Change", true)]];
    for (const c of items.slice(0, 95)) {
      const url = classLink(c);
      const when = prettyWhen(c.ymd, c.time);
      const whenCell = url
        ? { type: "rich_text", elements: [{ type: "rich_text_section", elements: [{ type: "link", url, text: when }] }] }
        : cell(when);
      rows.push([whenCell, cell(c.type), cell(c.from || "nobody"), cell(c.to, true), cell(`${EMOJI[c.cat] || ""} ${c.kindLabel}${c.warn ? " ⚠️" : ""}`)]);
    }
    body = [{ type: "table", rows, column_settings: [{ is_wrapped: false }, { is_wrapped: true }, { is_wrapped: true }, { is_wrapped: true }, { is_wrapped: true }] }];
    if (items.length > 95) body.push({ type: "context", elements: [{ type: "mrkdwn", text: `…and ${items.length - 95} more.` }] });
  } else {
    const byDay = new Map();
    for (const c of items) byDay.set(c.ymd, [...(byDay.get(c.ymd) || []), c]);
    const lines = [...byDay].map(([ymd, list]) => {
      const head = `*${mrk(prettyWhen(ymd, "00:00").replace(/, 12:00am$/, ""))}*`;
      return `${head}\n${list.map((c) => `${EMOJI[c.cat] || "•"} ${classLink(c) ? `<${classLink(c)}|${mrk(prettyWhen(c.ymd, c.time).split(", ").pop())}>` : mrk(prettyWhen(c.ymd, c.time).split(", ").pop())} · ${mrk(c.type)}: ${c.from ? `~${mrk(c.from)}~` : "_nobody_"} → *${mrk(c.to)}*${c.warn ? " ⚠️" : ""}`).join("\n")}`;
    });
    const chunks = [];
    let cur = "";
    for (const l of lines) {
      if (cur && cur.length + l.length + 2 > 2800) {
        chunks.push(cur);
        cur = "";
      }
      cur += (cur ? "\n\n" : "") + l;
    }
    if (cur) chunks.push(cur);
    body = chunks.slice(0, 40).map((t) => ({ type: "section", text: { type: "mrkdwn", text: t } }));
  }

  const foot = [
    {
      type: "context",
      elements: [{ type: "mrkdwn", text: `Should this *not* have been done? Put the right instructor / FINAL Sub back in ALL CLASSES; the auto-update then leaves that class alone unless MTEK changes it again. Runs every Sun / Mon / Wed / Fri for the next ${WINDOW_DAYS} days.` }],
    },
    { type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "Open ALL CLASSES" }, url: ALL_CLASSES_URL, style: "primary" }] },
  ];
  return {
    text: `Schedule auto-update – ${studio} – ${plural(items.length, "class", "classes")} updated`,
    blocks: top,
    attachments: [{ color: "#F2A900", blocks: [...body, ...foot] }],
  };
}

/** Post a studio message; if Slack refuses the table block, post the list variant instead. */
async function postStudio(channel, studio, items, opts, prefixBlocks = [], suffixBlocks = []) {
  for (const variant of ["table", "list"]) {
    const m = studioMessage(studio, items, { ...opts, variant });
    try {
      await slackPost(channel, m.text, [...prefixBlocks, ...m.blocks, ...suffixBlocks], m.attachments);
      return variant;
    } catch (e) {
      if (variant === "list" || !/invalid_blocks|invalid_attachments|invalid_arguments/.test(e.message)) throw e;
      log(`Slack refused the table layout (${e.message}); using the list layout`);
    }
  }
}

async function dmAdmin(text, blocks) {
  if (!SLACK_TOKEN || !ADMIN_USER) {
    log("(admin DM skipped: SLACK_BOT_TOKEN / SLACK_ADMIN_USER_ID not set)", text);
    return;
  }
  await slackPost(ADMIN_USER, text, blocks);
}

/* ------------------------------------------------------------------ */
/* State (classes already auto-corrected)                              */
/* ------------------------------------------------------------------ */

function loadState() {
  try {
    const s = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    return { applied: s.applied || {}, tries: s.tries || {} };
  } catch {
    return { applied: {}, tries: {} };
  }
}
function saveState(state, todayYmd) {
  for (const [k, v] of Object.entries(state.applied)) if (v.ymd < addDays(todayYmd, -2)) delete state.applied[k];
  for (const [k, v] of Object.entries(state.tries || {})) if (v.ymd < addDays(todayYmd, -2)) delete state.tries[k];
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify({ applied: state.applied, tries: state.tries || {} }, null, 2) + "\n");
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

const CATEGORY_MAP = {
  "new instructor on timeslot": "instructor",
  "sub not assigned in airtable": "sub",
  "new timeslot added to schedule": "new",
  "all good": "good",
};
const KIND_LABEL = {
  instructor: "New instructor in MTEK",
  sub: "Substitute from MTEK",
  blank: "Instructor was missing",
  new: "New time slot added",
};

async function run() {
  const missing = ["AIRTABLE_TOKEN", "MTEK_API_TOKEN", "SLACK_BOT_TOKEN", "SLACK_ADMIN_USER_ID"].filter((k) => !process.env[k]);
  if (missing.length) throw new Error(`Missing env: ${missing.join(", ")}`);

  const today = ymdFromDate(new Date());
  log(`Subs auto-correct ${DRY_RUN ? "(DRY RUN) " : ""}for ${today}, window ${WINDOW_DAYS} days`);

  // 1. How far each studio was imported (Import Runs) ----------------------------------------
  const runs = await atListAll(RUNS_TABLE, {
    filterByFormula: `AND({Mode}='Import',OR({Status}='Completed',{Status}='Completed with warnings'),NOT(IS_BEFORE({Max date},'${today}')))`,
    "fields[]": ["Studios", "Max date"],
  });
  const capByStudio = new Map(); // normalised studio -> last imported date
  let capAll = null;
  for (const r of runs) {
    const max = String(r.fields["Max date"] || "").slice(0, 10);
    if (!max) continue;
    const studiosRaw = String(r.fields["Studios"] || "All").trim();
    if (/^all$/i.test(studiosRaw) || !studiosRaw) {
      if (!capAll || max > capAll) capAll = max;
      continue;
    }
    for (const s of studiosRaw.split(",").map((x) => norm(x)).filter(Boolean)) {
      if (!capByStudio.has(s) || max > capByStudio.get(s)) capByStudio.set(s, max);
    }
  }
  const windowEnd = addDays(today, WINDOW_DAYS);
  const capFor = (studio) => {
    const k = norm(studio);
    let cap = capByStudio.get(k) || null;
    if (capAll && (!cap || capAll > cap)) cap = capAll;
    if (!cap) return null;
    return cap < windowEnd ? cap : windowEnd;
  };
  log(`Import Runs: ${runs.length} usable imports; per-studio last imported date:`, Object.fromEntries(capByStudio), capAll ? `All=${capAll}` : "");
  if (capByStudio.size === 0 && !capAll) {
    log("No studio has an import reaching today or later. Nothing to do.");
    return;
  }
  const scopeEnd = [...capByStudio.keys()].map((s) => capFor(s)).concat(capAll ? [capFor("*")] : []).filter(Boolean).sort().pop();
  if (!scopeEnd) return;

  // 2. Employees + studios (one read each) ---------------------------------------------------
  const meta = await at("GET", `meta/bases/${BASE_ID}/tables`);
  const empTable = (meta.tables || []).find((t) => t.id === EMPLOYEES_TABLE);
  const primaryName = empTable?.fields?.find((f) => f.id === empTable.primaryFieldId)?.name;
  const empFields = ["MTEK Name", ...(primaryName && primaryName !== "MTEK Name" ? [primaryName] : [])];
  const employees = await atListAll(EMPLOYEES_TABLE, { "fields[]": empFields });
  const empById = new Map();
  const empByMtek = new Map();
  for (const e of employees) {
    const mtek = String(e.fields["MTEK Name"] ?? "").trim();
    const primary = String(primaryName ? e.fields[primaryName] ?? "" : "").trim();
    empById.set(e.id, { id: e.id, mtek, primary });
    if (mtek) empByMtek.set(norm(mtek), [...(empByMtek.get(norm(mtek)) || []), e.id]);
  }
  const studioRecs = await atListAll(STUDIOS_TABLE, { "fields[]": ["Studio name", "Slack channel", "Slack Channel name"] });
  const channelByStudio = new Map(
    studioRecs.map((s) => [norm(s.fields["Studio name"]), String(s.fields["Slack channel"] || "").trim() || String(s.fields["Slack Channel name"] || "").trim().replace(/^#/, "")]),
  );

  // 3. MTEK: ONE report for the whole window ---------------------------------------------------
  log(`Getting the MTEK report ${today} -> ${scopeEnd} …`);
  const report = await fetchMtekReport({
    baseUrl: MTEK_BASE_URL, token: MTEK_TOKEN, reportId: MTEK_REPORT_ID, slug: MTEK_REPORT_SLUG, pageSize: 500,
    dateParams: { min_start_date_day: today, max_start_date_day: scopeEnd }, pollTimeoutMs: 15 * 60000,
  });
  const H = Object.fromEntries(report.headers.map((h, i) => [h, i]));
  for (const h of ["Location", "Class Date", "Class Time", "Instructors", "Class Type"]) {
    if (!(h in H)) throw new Error(`The MTEK report is missing the "${h}" column.`);
  }
  const col = (row, name) => (name in H ? row[H[name]] : undefined);
  const rows = [];
  const studiosInScope = new Set();
  for (const r of report.rows) {
    const location = String(col(r, "Location") ?? "").trim();
    const ymd = String(col(r, "Class Date") ?? "").trim();
    if (norm(col(r, "Class Type")) === "room booked") continue;
    if (ONLY_STUDIO && norm(location) !== norm(ONLY_STUDIO)) continue;
    const cap = capFor(location);
    if (!cap || ymd < today || ymd > cap) continue; // studio never imported, or beyond what was imported / 60 days
    rows.push(r);
    studiosInScope.add(location);
  }
  log(`MTEK: ${report.rows.length} classes in the report, ${rows.length} inside the studios / dates in scope (${[...studiosInScope].join(", ")})`);
  if (rows.length === 0) return;

  // 4. Airtable classes: ONE read ---------------------------------------------------------------
  const roomFilter = [...studiosInScope].map((s) => `{room}='${q(s)}'`);
  const existing = await atListAll(CLASSES_TABLE, {
    filterByFormula: `AND(IS_AFTER({date 2},'${addDays(today, -2)}'),IS_BEFORE({date 2},'${addDays(scopeEnd, 2)}'),OR(${roomFilter.join(",")}))`,
    "fields[]": ["Class ID", "Class!", "date 2", "time 2", "room", "class", "Instructor", "FINAL Sub"],
  });
  log(`All Classes: ${existing.length} classes read`);
  const byId = new Map();
  const byKey = new Map();
  for (const r of existing) {
    const f = r.fields;
    if (f["Class ID"] != null) byId.set(String(f["Class ID"]), r);
    const t = parseTitle(Array.isArray(f["Class!"]) ? f["Class!"][0] : f["Class!"]);
    let k = null;
    if (t) k = keyOf(t.room, t.ymd, t.hhmm, t.cls);
    else if (f["date 2"] && f["room"] && f["class"]) k = keyOf(f["room"], ymdFromDate(new Date(f["date 2"])), hhmm(f["time 2"]), f["class"]);
    if (k) byKey.set(k, [...(byKey.get(k) || []), r]);
  }

  // 5. Compare (same rules as the Schedule changes page) ------------------------------------------
  const state = loadState();
  const usedRecs = new Set();
  const stage = [];
  const leftAlone = [];
  const needsAttention = [];
  let good = 0;
  let airtableAhead = 0;

  for (const row of rows) {
    const location = String(col(row, "Location") ?? "").trim();
    const ymd = String(col(row, "Class Date") ?? "").trim();
    const time = hhmm(col(row, "Class Time"));
    const type = String(col(row, "Class Type") ?? "").trim();
    const mtekName = String(col(row, "Instructors") ?? "").trim();
    if (!location || !/^\d{4}-\d{2}-\d{2}$/.test(ymd) || !time || !type || !mtekName) continue;
    const key = keyOf(location, ymd, time, type);
    const rec = (byKey.get(key) || []).find((r) => !usedRecs.has(r.id)) || null;
    if (!rec) {
      stage.push({ row, location, ymd, time, type, mtekName, key, rec: null });
      continue;
    }
    usedRecs.add(rec.id);
    const finalIds = rec.fields["FINAL Sub"] || [];
    const instrIds = rec.fields["Instructor"] || [];
    const curIds = finalIds.length ? finalIds : instrIds;
    const curEmps = curIds.map((id) => empById.get(id)).filter(Boolean);
    const wanted = mtekName.split(",").map((s) => norm(s)).filter(Boolean);
    const curSet = new Set(curEmps.flatMap((e) => [norm(e.mtek), norm(e.primary)]).filter(Boolean));
    if (curIds.length > 0 && wanted.every((w) => curSet.has(w)) && curEmps.length === wanted.length) {
      good++;
      continue;
    }
    if (curEmps.length > 0 && norm(curEmps.map((e) => e.mtek || e.primary).join(",")) === norm(mtekName)) {
      good++; // same name on both sides (e.g. a duplicate employee row): ticking Fix cannot change anything
      continue;
    }
    const instrEmps = instrIds.map((id) => empById.get(id)).filter(Boolean);
    const instrSet = new Set(instrEmps.flatMap((e) => [norm(e.mtek), norm(e.primary)]).filter(Boolean));
    if (finalIds.length > 0 && instrEmps.length > 0 && instrEmps.length === wanted.length && wanted.every((w) => instrSet.has(w))) {
      airtableAhead++; // a sub is in Airtable, MTEK still shows the regular instructor: nothing to do
      continue;
    }
    stage.push({ row, location, ymd, time, type, mtekName, key, rec, curLabel: curEmps.map((e) => e.mtek || e.primary).join(", "), hasFinal: finalIds.length > 0 });
  }

  // Classes missing from Airtable by exact key: maybe they exist under the MTEK Class ID (moved class). Only then ask MTEK for the session ids.
  const needIds = stage.filter((c) => !c.rec);
  if (needIds.length) {
    log(`${needIds.length} class(es) not found by studio+date+time: fetching MTEK class numbers …`);
    const sessions = await fetchClassSessions(today, scopeEnd);
    const sessionByKey = new Map();
    for (const s of sessions) {
      const a = s.attributes || {};
      if (!a.start_date || !a.start_time) continue;
      const k = keyOf(a.location_display, a.start_date, hhmm(a.start_time), a.class_type_display);
      sessionByKey.set(k, [...(sessionByKey.get(k) || []), { id: String(s.id), classroom: a.classroom_display }]);
    }
    for (const c of needIds) {
      const cands = sessionByKey.get(c.key) || [];
      if (cands.length === 1) c.classId = cands[0].id;
      else if (cands.length > 1) c.classId = (cands.find((x) => norm(x.classroom) === norm(col(c.row, "Classroom"))) || {}).id || null;
      const byIdRec = c.classId ? byId.get(c.classId) : null;
      if (byIdRec && !usedRecs.has(byIdRec.id)) {
        usedRecs.add(byIdRec.id);
        c.rec = byIdRec;
        const finalIds = byIdRec.fields["FINAL Sub"] || [];
        const curEmps = (finalIds.length ? finalIds : byIdRec.fields["Instructor"] || []).map((id) => empById.get(id)).filter(Boolean);
        c.curLabel = curEmps.map((e) => e.mtek || e.primary).join(", ");
        c.hasFinal = finalIds.length > 0;
      }
    }
  }

  // Leave alone what a person put back by hand after we corrected it; skip names we cannot match
  const toApply = [];
  const stuck = [];
  for (const c of stage) {
    const tr = state.tries[c.key];
    if (tr && norm(tr.to) === norm(c.mtekName) && tr.n >= 2) {
      stuck.push({ ...c, firstReport: tr.n === 2 && !tr.told });
      continue; // ticked twice already and it never showed up in All Classes: do not retry every run
    }
    const prev = state.applied[c.key];
    if (prev && norm(prev.to) === norm(c.mtekName)) {
      leftAlone.push(c);
      continue;
    }
    if (c.rec && c.curLabel && norm(c.curLabel) === norm(c.mtekName)) {
      good++; // same name on both sides: assume it is already right
      continue;
    }
    const names = c.mtekName.split(",").map((x) => x.trim()).filter(Boolean);
    const unmatched = names.filter((w) => !(empByMtek.get(norm(w)) || []).length);
    if (unmatched.length && c.rec) {
      needsAttention.push({ ...c, unmatched, why: `No employee has the MTEK Name "${unmatched.join('", "')}", so it was not applied.` });
      continue;
    }
    if (unmatched.length) c.unmatched = unmatched;
    if (unmatched.length) c.warn = `No employee has the MTEK Name "${unmatched.join('", "')}": the class is added without an instructor.`;
    toApply.push(c);
  }

  log(`Compared: ${good} already correct, ${airtableAhead} Airtable-ahead (sub in Airtable), ${toApply.length} to apply, ${leftAlone.length} left alone (reverted by hand), ${needsAttention.length} need attention`);
  const describe = (c) => `${c.location} ${c.ymd} ${c.time} ${c.type}: ${c.rec ? `${c.curLabel || "nobody"} -> ` : "NEW class -> "}${c.mtekName}`;
  toApply.forEach((c) => log("  would apply:", describe(c)));
  leftAlone.forEach((c) => log("  left alone: ", describe(c)));
  needsAttention.forEach((c) => log("  attention:  ", describe(c), "-", c.why));

  stuck.forEach((c) => log("  stuck (not retried):", describe(c)));
  if (toApply.length > MAX_AUTO_CHANGES) {
    const msg = `${toApply.length} changes were found (limit ${MAX_AUTO_CHANGES}). Nothing was applied: this looks abnormal (bad MTEK report?). Run Check for changes on /subs-schedule-changes to review.`;
    log("STOP:", msg);
    await dmAdmin(`Subs auto-correct STOPPED: ${msg}`);
    return;
  }

  // labels (the staging formula can refine the category after the real run)
  for (const c of toApply) {
    c.cat = !c.rec ? "new" : !c.curLabel ? "blank" : String(col(c.row, "Has Substitute?") ?? "false").toLowerCase() === "true" ? "sub" : "instructor";
    c.kindLabel = KIND_LABEL[c.cat];
    c.from = c.rec ? (c.hasFinal && c.cat === "sub" ? `Sub ${c.curLabel}` : c.curLabel) : "";
    c.to = c.mtekName;
  }
  // Heartbeat: every studio in scope gets a message every run, so the channel shows the check really ran
  // (no message = the automation did not run). Studios with nothing to apply get it right away.
  const checkedBy = new Map();
  for (const r of rows) {
    const l = String(col(r, "Location") ?? "").trim();
    checkedBy.set(l, (checkedBy.get(l) || 0) + 1);
  }
  const attentionBlocks = (studio) => {
    const byName = new Map();
    for (const c of [...needsAttention, ...toApply].filter((x) => x.location === studio && x.unmatched?.length)) {
      for (const n of c.unmatched) byName.set(n, [...(byName.get(n) || []), c]);
    }
    if (!byName.size) return [];
    const lines = [...byName].map(([n, list]) => {
      list.sort((a, b) => `${a.ymd} ${a.time}`.localeCompare(`${b.ymd} ${b.time}`));
      return `• *${mrk(n)}*: ${plural(list.length, "class", "classes")} not updated (first: ${mrk(prettyWhen(list[0].ymd, list[0].time))})`;
    });
    return [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `⚠️ *Instructor not found in Airtable*\n${lines.join("\n")}\nUpdate their *MTEK Name* in <${ACTIVE_INSTRUCTORS_URL}|Active Employees - Instructors> so it matches MTEK exactly. These classes are then updated automatically at the next run.`,
        },
      },
    ];
  };
  const postHeartbeat = async (studio, un = []) => {
    const channel = channelByStudio.get(norm(studio)) || "";
    const checked = checkedBy.get(studio) || 0;
    // changes that could not be confirmed are not the studio's business (they are retried, then left to be done by hand)
    const attn = attentionBlocks(studio);
    const text = attn.length
      ? `✅ Checked *${plural(checked, "class", "classes")}* (today to ${capFor(studio)}). *No other changes needed.*`
      : `✅ Checked *${plural(checked, "class", "classes")}* (today to ${capFor(studio)}): MTEK and Airtable match. *No changes needed.*`;
    const blocks = [
      { type: "header", text: { type: "plain_text", text: `📅 Schedule auto-check · ${studio}`.slice(0, 150), emoji: true } },
      { type: "section", text: { type: "mrkdwn", text } },
      ...attn,
      { type: "context", elements: [{ type: "mrkdwn", text: `Ran ${today}. Runs every Sun / Mon / Wed / Fri. If you do not see a message from this check, tell Jonathan.` }] },
    ];
    try {
      await slackPost(channel, `Schedule auto-check – ${studio} – ${un.length ? "not confirmed" : "no changes needed"}`, blocks);
      log(`Heartbeat for ${studio}${channel ? "" : " (no Slack channel set)"}: ${un.length ? "not confirmed" : "no changes needed"}`);
    } catch (e) {
      log(`Heartbeat for ${studio} failed: ${e.message}`);
    }
    await sleep(1100);
  };
  const studiosWithChanges = new Set(toApply.map((c) => c.location));
  for (const studio of studiosInScope) if (!studiosWithChanges.has(studio)) await postHeartbeat(studio);

  let applied = [];
  let unconfirmed = [];
  if (toApply.length && !DRY_RUN) {
    ({ applied, unconfirmed } = await applyViaStaging(toApply, { col, empById, empByMtek, today }));
  }

  // 6. Remember + email ------------------------------------------------------------------------------
  if (!DRY_RUN) {
    for (const c of applied) {
      state.applied[c.key] = { to: c.mtekName, ymd: c.ymd, cat: c.cat, on: today };
      delete state.tries[c.key];
    }
    for (const c of unconfirmed) {
      const t = state.tries[c.key];
      state.tries[c.key] = { to: c.mtekName, ymd: c.ymd, n: (t && norm(t.to) === norm(c.mtekName) ? t.n : 0) + 1 };
    }
    for (const c of stuck.filter((x) => x.firstReport)) state.tries[c.key].told = true;
    saveState(state, today);
  }

  // dry run: nothing was applied, so show what WOULD be posted (to the admin DM only)
  const announce = DRY_RUN ? toApply : applied;
  const byStudio = new Map();
  for (const c of announce) byStudio.set(c.location, [...(byStudio.get(c.location) || []), c]);
  for (const [studio, items] of byStudio) {
    items.sort((a, b) => `${a.ymd} ${a.time}`.localeCompare(`${b.ymd} ${b.time}`));
    const channel = channelByStudio.get(norm(studio)) || "";
    const where = channel ? (/^[CG]/.test(channel) ? `<#${channel}>` : `#${channel}`) : "_no Slack channel set for this studio_";
    if (DRY_RUN) {
      const v = await postStudio(ADMIN_USER, studio, items, { dry: true }, [{ type: "context", elements: [{ type: "mrkdwn", text: `🧪 *DRY RUN* – this is what would be posted in ${where}` }] }]);
      log(`DM'd the dry-run preview for ${studio} (${items.length}, ${v} layout)`);
    } else if (channel) {
      const v = await postStudio(channel, studio, items, {}, [], attentionBlocks(studio));
      log(`Posted to Slack for ${studio} (${items.length}, ${v} layout)`);
    } else {
      await postStudio(ADMIN_USER, studio, items, {}, [{ type: "context", elements: [{ type: "mrkdwn", text: `⚠️ No Slack channel for ${studio}: sent to you instead.` }] }]);
      log(`No Slack channel for ${studio}: sent to admin`);
    }
    await sleep(1100); // Slack: ~1 message per second per channel
  }

  for (const studio of studiosWithChanges) if (!byStudio.has(studio)) await postHeartbeat(studio, unconfirmed.filter((c) => c.location === studio));
  const stuckNew = stuck.filter((c) => c.firstReport);
  if (needsAttention.length || unconfirmed.length || leftAlone.length || stuckNew.length) {
    const li = (arr, f) => arr.map((c) => `• ${mrk(describe(c))}${f ? ` — ${mrk(f(c))}` : ""}`).join("\n");
    const parts = [
      needsAttention.length ? `*Not applied (no matching employee):*\n${li(needsAttention, (c) => c.why)}` : "",
      unconfirmed.length ? `*Applied but not confirmed in All Classes yet* (see the run history of the Airtable automation "Fix schedule (NEW)"):\n${li(unconfirmed)}` : "",
      stuckNew.length ? `*Could not be applied automatically* (ticked twice, never showed up in All Classes; not retried; do these by hand in ALL CLASSES):\n${li(stuckNew)}` : "",
      leftAlone.length ? `*Left alone* (auto-corrected before, then put back by hand):\n${li(leftAlone)}` : "",
    ].filter(Boolean);
    await dmAdmin("Subs auto-correct: items that need a look", [
      { type: "header", text: { type: "plain_text", text: `${DRY_RUN ? "🧪 DRY RUN · " : ""}Subs auto-correct: items to look at`, emoji: true } },
      ...parts.map((t) => ({ type: "section", text: { type: "mrkdwn", text: t.slice(0, 2900) } })),
    ]);
  }
  if (DRY_RUN && byStudio.size === 0 && needsAttention.length + leftAlone.length === 0) {
    await dmAdmin("🧪 Subs auto-correct DRY RUN: nothing to change in the next 60 days.");
  }
  log("Done.");
}

/**
 * Staging table -> tick Fix -> wait for the Airtable automation "Fix schedule (NEW)" -> verify in All Classes.
 * Returns the confirmed changes (applied) and the ones that did not show up in time (unconfirmed).
 */
async function applyViaStaging(allChanges, { col, empById, empByMtek, today }) {
  // Duo classes (two instructors in MTEK): the Airtable automation cannot add the second instructor to a class that
  // already has one, so both employees are written straight into the class (Instructor, or FINAL Sub when MTEK says sub).
  const isDuo = (c) => c.rec && c.mtekName.includes(",") && !c.unmatched;
  const duo = allChanges.filter(isDuo);
  const toApply = allChanges.filter((c) => !isDuo(c));
  const duoDone = [];
  for (let i = 0; i < duo.length; i += 10) {
    const chunk = duo.slice(i, i + 10);
    const records = [];
    for (const c of chunk) {
      const ids = c.mtekName.split(",").map((w) => (empByMtek.get(norm(w.trim())) || [])[0]).filter(Boolean);
      if (!ids.length) continue;
      c.cat = c.cat === "sub" ? "sub" : c.curLabel ? "instructor" : "blank";
      c.kindLabel = KIND_LABEL[c.cat] || c.cat;
      c.from = c.rec ? (c.hasFinal && c.cat === "sub" ? `Sub ${c.curLabel}` : c.curLabel) : "";
      c.to = c.mtekName;
      records.push({ id: c.rec.id, fields: { [c.cat === "sub" ? "FINAL Sub" : "Instructor"]: ids } });
      duoDone.push(c);
    }
    if (records.length) await at("PATCH", `${BASE_ID}/${CLASSES_TABLE}`, { typecast: false, records });
    await sleep(250);
  }
  const num = (v) => (v == null || v === "" ? undefined : String(v));

  // empty the staging table (the Schedule changes page does the same at the start of every check)
  const old = await atListAll(STAGING_TABLE, { "fields[]": ["Fix"] });
  for (let i = 0; i < old.length; i += 10) {
    await at("DELETE", `${BASE_ID}/${STAGING_TABLE}`, null, { "records[]": old.slice(i, i + 10).map((r) => r.id) });
    await sleep(220);
  }

  // one staging row per change, 10 per call
  const created = [];
  for (let i = 0; i < toApply.length; i += 10) {
    const chunk = toApply.slice(i, i + 10);
    const records = chunk.map((c) => {
      const r = c.row;
      const fields = {
        room: c.location,
        date: ddmmyyyy(c.ymd),
        time: c.time,
        "Class Day Of Week": String(col(r, "Class Day of Week") ?? "").trim() || undefined,
        "Instructor name (FROM MTEK)": c.mtekName,
        "Is substitute?": String(col(r, "Has Substitute?") ?? "false").toLowerCase(),
        Classroom: num(col(r, "Classroom")),
        "Class Tags": num(col(r, "Class Tags")),
        "Class is Free?": num(col(r, "Class Is Free?")),
        class: c.type,
        "Class Category": num(col(r, "Class Category")),
        "Pending Standard Reservations": num(col(r, "Pending Standard Reservations")),
        "Pending Standby Reservations": num(col(r, "Pending Standby Reservations")),
        "Pending Waitlist Reservations": num(col(r, "Pending Waitlist Reservations")),
        "Checked In Reservations": num(col(r, "Checked In Reservations")),
        "Late Cancelled Reservations": num(col(r, "Late Cancelled Reservations")),
        "No Showed Reservations": num(col(r, "No Showed Reservations")),
        "Admin Holds": num(col(r, "Admin Holds")),
        "Unavailable Holds": num(col(r, "Unavailable Holds")),
        "Layout Capacity": num(col(r, "Layout Capacity")),
        "Actual Capacity": num(col(r, "Actual Capacity")),
        "% Utilization": num(col(r, "% Utilization")),
        "DATE TIME FORMAT": "DD/MM/YYYY",
      };
      if (c.classId) fields["Class ID"] = Number(c.classId);
      if (c.rec) fields["All Classes Link"] = [c.rec.id];
      for (const k of Object.keys(fields)) if (fields[k] === undefined) delete fields[k];
      return { fields };
    });
    const res = await at("POST", `${BASE_ID}/${STAGING_TABLE}`, { typecast: true, records });
    (res.records || []).forEach((rec, j) => created.push({ id: rec.id, c: chunk[j] }));
    await sleep(250);
  }

  // let the staging formulas calculate, read "Category (Form)" back (40 per call)
  await sleep(Math.min(12000, 3000 + created.length * 40));
  const cat = new Map();
  for (let i = 0; i < created.length; i += 40) {
    const recs = await atListAll(STAGING_TABLE, { filterByFormula: orIds(created.slice(i, i + 40).map((x) => x.id)), "fields[]": ["Category (Form)"] });
    for (const r of recs) cat.set(r.id, norm(r.fields["Category (Form)"]));
  }
  const ticks = [];
  for (const { id, c } of created) {
    const formula = cat.get(id) || "";
    const k = CATEGORY_MAP[formula] || (c.rec ? "instructor" : "new");
    if (k === "good") continue; // the staging formula says nothing to do
    c.cat = k === "new" && c.rec ? "blank" : k;
    c.kindLabel = KIND_LABEL[c.cat] || c.cat;
    c.from = c.rec ? (c.hasFinal && c.cat === "sub" ? `Sub ${c.curLabel}` : c.curLabel) : "";
    c.to = c.mtekName;
    ticks.push(id);
    c.stagingId = id;
  }
  const real = created.filter((x) => ticks.includes(x.id)).map((x) => x.c).concat(duoDone);

  // tick Fix: the Airtable automation "Fix schedule (NEW)" runs once per row
  for (let i = 0; i < ticks.length; i += 10) {
    await at("PATCH", `${BASE_ID}/${STAGING_TABLE}`, { typecast: false, records: ticks.slice(i, i + 10).map((id) => ({ id, fields: { Fix: true } })) });
    await sleep(400);
  }
  log(`Ticked Fix on ${ticks.length} staging rows; waiting for the automation …`);

  // verify in All Classes, batched (40 records per call), up to ~12 minutes
  const fields = ["Instructor", "FINAL Sub", "Class ID", "Class!"];
  const isDone = (c, rec) => {
    if (!rec) return false;
    if (c.cat === "new") return true;
    const ids = c.cat === "sub" ? rec.fields["FINAL Sub"] || [] : rec.fields["Instructor"] || [];
    const have = new Set(ids.map((id) => empById.get(id)).filter(Boolean).flatMap((e) => [norm(e.mtek), norm(e.primary)]).filter(Boolean));
    const wanted = c.mtekName.split(",").map((s) => norm(s)).filter(Boolean);
    return ids.length === wanted.length && wanted.every((w) => have.has(w));
  };
  const lookup = async (list) => {
    const out = new Map();
    const existing = list.filter((c) => c.rec);
    for (let i = 0; i < existing.length; i += 40) {
      const recs = await atListAll(CLASSES_TABLE, { filterByFormula: orIds(existing.slice(i, i + 40).map((c) => c.rec.id)), "fields[]": fields });
      for (const r of recs) out.set(r.id, r);
    }
    // new classes: find them with the SAME studio+date+time+class key the comparison uses (the Class ID is
    // often not carried over by the automation, and the class name can differ, so an exact Class ID match missed them)
    const added = list.filter((c) => !c.rec);
    if (added.length) {
      const rooms = [...new Set(added.map((c) => c.location))].map((r) => `{room}='${q(r)}'`);
      const ymds = added.map((c) => c.ymd).sort();
      const recs = await atListAll(CLASSES_TABLE, {
        filterByFormula: `AND(IS_AFTER({date 2},'${addDays(ymds[0], -2)}'),IS_BEFORE({date 2},'${addDays(ymds[ymds.length - 1], 2)}'),OR(${rooms.join(",")}))`,
        "fields[]": [...fields, "date 2", "time 2", "room", "class"],
      });
      const byK = new Map();
      for (const r of recs) {
        const f = r.fields;
        const t = parseTitle(Array.isArray(f["Class!"]) ? f["Class!"][0] : f["Class!"]);
        let k = null;
        if (t) k = keyOf(t.room, t.ymd, t.hhmm, t.cls);
        else if (f["date 2"] && f["room"] && f["class"]) k = keyOf(f["room"], ymdFromDate(new Date(f["date 2"])), hhmm(f["time 2"]), f["class"]);
        if (k && !byK.has(k)) byK.set(k, r);
      }
      for (const c of added) if (byK.has(c.key)) out.set(`new:${c.key}`, byK.get(c.key));
    }
    return out;
  };
  let pending = real.slice();
  const confirmed = [];
  const confirmedRecs = new Map();
  const deadline = Date.now() + 12 * 60000;
  while (pending.length && Date.now() < deadline) {
    await sleep(pending.some((c) => !c.rec) ? 60000 : 30000);
    const found = await lookup(pending);
    const still = [];
    for (const c of pending) {
      const rec = c.rec ? found.get(c.rec.id) : found.get(`new:${c.key}`);
      if (isDone(c, rec)) {
        c.recId = rec.id;
        confirmed.push(c);
        confirmedRecs.set(c.key, rec);
      } else still.push(c);
    }
    pending = still;
    log(`Verified ${confirmed.length} of ${real.length}`);
  }

  // new classes: fill the MTEK Class ID when the automation did not carry it over
  for (const c of confirmed.filter((x) => x.cat === "new" && x.classId)) {
    const rec = confirmedRecs.get(c.key);
    if (rec && rec.fields["Class ID"] == null) {
      try {
        await at("PATCH", `${BASE_ID}/${CLASSES_TABLE}/${rec.id}`, { typecast: false, fields: { "Class ID": Number(c.classId) } });
      } catch {
        /* the nightly Resolve Class ID job fills it */
      }
    }
  }
  return { applied: confirmed, unconfirmed: pending };
}

run().catch(async (err) => {
  console.error(err);
  try {
    await dmAdmin(`Subs auto-correct FAILED: ${String(err.message).slice(0, 500)}`);
  } catch (e) {
    console.error("Could not send the failure DM:", e.message);
  }
  process.exit(1);
});
