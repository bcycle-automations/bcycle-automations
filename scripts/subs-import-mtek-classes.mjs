#!/usr/bin/env node
/**
 * Subs - Import classes from MTEK (Class Session Utilization report -> Airtable "All Classes").
 *
 * Replaces the manual "download the MTEK report and paste it into the
 * '1. IMPORT HERE MTEK' view" step. Started from the Softr page
 * /subs-import-mtek, which creates a row in the "Import Runs" table and
 * calls a Make webhook -> GitHub repository_dispatch -> this script.
 *
 * Modes (field "Mode" on the run record):
 *   Dry run  - fetch + validate + show what would be created. Writes nothing to All Classes.
 *   Import   - same validation, then creates the new classes in All Classes.
 *
 * Rules (agreed with Jonathan 2026-10-10):
 *   - Existing classes are never updated. A class that already exists (same MTEK Class ID,
 *     or - when the existing row has no Class ID yet - same room + date + time + class) is
 *     skipped and counted as "already in Airtable".
 *   - Records are created with typecast=false so a select value that is not an existing option
 *     is rejected instead of silently creating a new option. Rows with such values are
 *     reported under "Needs fixing" and are NOT imported.
 *   - Instructors are linked by the existing Airtable automations (Update Instructor /
 *     Assign Duo Instructors) from "Zingfit Official Name" - we only report the unmatched ones.
 *
 * Env: AIRTABLE_TOKEN, AIRTABLE_BASE_ID (Instructors - Subs), AIRTABLE_RUN_RECORD_ID,
 *      MTEK_API_TOKEN, optional MTEK_BASE_URL, GITHUB_RUN_URL.
 */

import { fetchMtekReport } from "./lib/mtek-report.mjs";

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const BASE_ID = process.env.AIRTABLE_BASE_ID || "applRovmu340OSBSg";
const RUNS_TABLE = process.env.AIRTABLE_RUNS_TABLE_ID || "tblGRf0nyYyCo1bXT";
const CLASSES_TABLE = process.env.AIRTABLE_CLASSES_TABLE_ID || "tblhoowrSqrGUwg6d";
const EMPLOYEES_TABLE = process.env.AIRTABLE_EMPLOYEES_TABLE_ID || "tblVNgEoR1oQQETdF";
const RUN_ID = process.env.AIRTABLE_RUN_RECORD_ID;
const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com").replace(/\/+$/, "");
const MTEK_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const TZ = "America/Toronto";
const MAX_RANGE_DAYS = 93;
const MTEK_REPORT_ID = "294";
const MTEK_REPORT_SLUG = "utilization";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ------------------------------------------------------------------ */
/* Airtable                                                            */
/* ------------------------------------------------------------------ */

async function at(method, path, body, query) {
  const url = new URL(`https://api.airtable.com/v0/${path}`);
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
      const err = new Error(`Airtable ${method} ${path} -> ${res.status}: ${msg}`);
      err.status = res.status;
      throw err;
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

/* ------------------------------------------------------------------ */
/* Run record: status, steps and notes visible on the Softr page       */
/* ------------------------------------------------------------------ */

const notes = [];
const state = { lastPatchAt: 0 };

function stamp() {
  return new Date().toLocaleTimeString("en-CA", { timeZone: TZ, hour12: false });
}

async function patchRun(fields) {
  try {
    await at("PATCH", `${BASE_ID}/${RUNS_TABLE}/${RUN_ID}`, { fields });
  } catch (e) {
    console.warn(`Could not update run record: ${e.message}`);
  }
}

async function note(icon, text, extra = {}) {
  const line = `${stamp()} ${icon} ${text}`;
  notes.push(line);
  console.log(line);
  await patchRun({ Notes: notes.join("\n"), ...extra });
}
const step = (text, extra) => note("▶", text, { "Current step": text, ...extra });

/* ------------------------------------------------------------------ */
/* Date helpers (America/Toronto)                                      */
/* ------------------------------------------------------------------ */

function ymdFromDate(d) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
}

// UTC ISO of local midnight in Toronto for a YYYY-MM-DD (matches how existing "date 2" values are stored).
function torontoMidnightIso(ymd) {
  const probe = new Date(`${ymd}T05:00:00Z`);
  const part = new Intl.DateTimeFormat("en-US", { timeZone: TZ, timeZoneName: "shortOffset" })
    .formatToParts(probe)
    .find((p) => p.type === "timeZoneName").value; // "GMT-4"
  const hrs = Number(part.replace("GMT", "") || 0);
  return new Date(new Date(`${ymd}T00:00:00Z`).getTime() - hrs * 3600000).toISOString();
}

function addDays(ymd, n) {
  const d = new Date(`${ymd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
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

const norm = (s) => String(s ?? "").trim().toLowerCase();
const keyOf = (room, ymd, time, cls) => `${norm(room)}|${ymd}|${time}|${norm(cls)}`;

function prettyClass(ymd, time, room, cls) {
  const [y, m, d] = ymd.split("-");
  return `${y.slice(2)}-${m}-${d} ${time} ${room} ${cls}`;
}

/* ------------------------------------------------------------------ */
/* MTEK                                                                */
/* ------------------------------------------------------------------ */

async function mtekGet(path, params) {
  const url = new URL(`${MTEK_BASE_URL}${path}`);
  for (const [k, v] of Object.entries(params || {})) url.searchParams.set(k, v);
  for (let attempt = 1; attempt <= 5; attempt++) {
    const res = await fetch(url, { headers: { Authorization: `Bearer ${MTEK_TOKEN}`, Accept: "application/vnd.api+json" } });
    if (res.status === 429) {
      await sleep(Number(res.headers.get("retry-after") || 2) * 1000);
      continue;
    }
    if (!res.ok) throw new Error(`MTEK ${path} -> ${res.status}: ${(await res.text()).slice(0, 200)}`);
    return res.json();
  }
  throw new Error(`MTEK ${path}: rate limited`);
}

async function fetchClassSessions(minYmd, maxYmd) {
  const minIso = new Date(new Date(torontoMidnightIso(minYmd)).getTime() - 3 * 3600000).toISOString();
  const maxIso = new Date(new Date(torontoMidnightIso(addDays(maxYmd, 1))).getTime() + 3 * 3600000).toISOString();
  const sessions = [];
  for (let page = 1; page <= 200; page++) {
    const json = await mtekGet("/api/class_sessions", {
      page_size: "100",
      page: String(page),
      min_datetime: minIso,
      max_datetime: maxIso,
    });
    sessions.push(...(json.data || []));
    const pages = json?.meta?.pagination?.pages || 1;
    if (page >= pages) break;
    await sleep(150);
  }
  return sessions;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

function requireEnv() {
  const missing = [];
  if (!AIRTABLE_TOKEN) missing.push("AIRTABLE_TOKEN");
  if (!RUN_ID) missing.push("AIRTABLE_RUN_RECORD_ID");
  if (!MTEK_TOKEN) missing.push("MTEK_API_TOKEN");
  if (missing.length) throw new Error(`Missing env: ${missing.join(", ")}`);
}

function choiceSet(table, fieldName) {
  const f = table.fields.find((x) => x.name === fieldName);
  if (!f) throw new Error(`Field "${fieldName}" not found in All Classes`);
  return new Set((f.options?.choices || []).map((c) => c.name));
}

async function main() {
  requireEnv();
  const run = (await at("GET", `${BASE_ID}/${RUNS_TABLE}/${RUN_ID}`)).fields;
  const mode = run["Mode"] === "Import" ? "Import" : "Dry run";
  const minYmd = run["Min date"];
  const maxYmd = run["Max date"];
  const studiosRaw = String(run["Studios"] || "All").trim();
  const allStudios = /^all$/i.test(studiosRaw) || studiosRaw === "";
  const studios = allStudios ? [] : studiosRaw.split(",").map((s) => s.trim()).filter(Boolean);
  const studiosLabel = allStudios ? "All studios" : studios.join(", ");

  await patchRun({
    Status: "Running",
    "Started at": new Date().toISOString(),
    "Finished at": null,
    Notes: "",
    "Preview JSON": "",
    "GitHub run": process.env.GITHUB_RUN_URL || null,
    "Classes found": null, "Already in Airtable": null, "To create": null, Created: null, Failed: null,
    "Needs fixing": null, "No instructor match": null, "First class": null, "Last class": null,
  });
  await note("ℹ️", `${mode} for ${studiosLabel}, ${minYmd} to ${maxYmd}`);

  // 1. Inputs -------------------------------------------------------------
  await step("1/7 Checking your inputs");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(minYmd || "") || !/^\d{4}-\d{2}-\d{2}$/.test(maxYmd || "")) {
    throw new Error("Min date and Max date are required.");
  }
  if (minYmd > maxYmd) throw new Error("Min date is after Max date.");
  const rangeDays = Math.round((new Date(maxYmd) - new Date(minYmd)) / 86400000) + 1;
  if (rangeDays > MAX_RANGE_DAYS) throw new Error(`Date range is ${rangeDays} days; the maximum is ${MAX_RANGE_DAYS}. Split it into smaller imports.`);
  await note("✅", `Inputs OK (${rangeDays} day${rangeDays === 1 ? "" : "s"})`);

  // 2. Airtable schema (select options) -----------------------------------
  await step("2/7 Reading All Classes field options");
  const meta = await at("GET", `meta/bases/${BASE_ID}/tables`);
  const table = (meta.tables || []).find((t) => t.id === CLASSES_TABLE);
  if (!table) throw new Error("All Classes table not found in schema");
  const roomChoices = choiceSet(table, "room");
  const classChoices = choiceSet(table, "class");
  const subChoices = choiceSet(table, "Is Substitute?");
  const dayChoices = choiceSet(table, "Class Day Of Week");
  const classIdField = table.fields.find((f) => f.name === "Class ID");
  if (!classIdField) throw new Error('Field "Class ID" not found');

  // 3. MTEK report ----------------------------------------------------------
  await step("3/7 Fetching the MTEK Class Session Utilization report");
  const report = await fetchMtekReport({
    baseUrl: MTEK_BASE_URL,
    token: MTEK_TOKEN,
    reportId: MTEK_REPORT_ID,
    slug: MTEK_REPORT_SLUG,
    pageSize: 500,
    dateParams: { min_start_date_day: minYmd, max_start_date_day: maxYmd },
    pollTimeoutMs: 8 * 60000,
  });
  const H = Object.fromEntries(report.headers.map((h, i) => [h, i]));
  for (const h of ["Location", "Class Date", "Class Time", "Instructors", "Class Type"]) {
    if (!(h in H)) throw new Error(`MTEK report is missing the "${h}" column (got: ${report.headers.join(", ")})`);
  }
  const col = (row, name) => (name in H ? row[H[name]] : undefined);
  const allRows = report.rows;
  const locations = [...new Set(allRows.map((r) => String(col(r, "Location") ?? "").trim()))].sort();
  await note("✅", `MTEK returned ${allRows.length} classes in ${locations.length} location(s): ${locations.join(", ") || "none"}`);

  let rows = allRows;
  if (!allStudios) {
    const wanted = new Set(studios.map(norm));
    rows = allRows.filter((r) => wanted.has(norm(col(r, "Location"))));
    const unknown = studios.filter((s) => !locations.some((l) => norm(l) === norm(s)));
    if (unknown.length) await note("⚠️", `No classes returned for: ${unknown.join(", ")} (check the studio name)`);
    await note("✅", `${rows.length} of ${allRows.length} classes are in the selected studio(s)`);
  }
  if (rows.length === 0) {
    await patchRun({
      Status: "Completed with warnings", "Classes found": 0, "To create": 0, Created: 0,
      "Already in Airtable": 0, "Needs fixing": 0, "Finished at": new Date().toISOString(),
      "Current step": "Nothing to import",
    });
    await note("⚠️", "MTEK returned no classes for this selection, nothing to import.");
    await patchRun({ Notes: notes.join("\n"), "Current step": "Nothing to import" });
    return;
  }

  // 4. MTEK class IDs ---------------------------------------------------------
  await step("4/7 Looking up MTEK Class IDs");
  const sessions = await fetchClassSessions(minYmd, maxYmd);
  const sessionByKey = new Map();
  for (const s of sessions) {
    const a = s.attributes || {};
    if (!a.start_date || !a.start_time) continue;
    const k = keyOf(a.location_display, a.start_date, hhmm(a.start_time), a.class_type_display);
    (sessionByKey.get(k) || sessionByKey.set(k, []).get(k)).push({ id: String(s.id), classroom: a.classroom_display });
  }
  await note("✅", `${sessions.length} MTEK class sessions loaded for Class IDs`);

  // 5. What is already in Airtable -------------------------------------------
  await step("5/7 Checking which classes already exist in Airtable");
  const existing = await atListAll(CLASSES_TABLE, {
    filterByFormula: `AND(IS_AFTER({date 2},'${addDays(minYmd, -2)}'),IS_BEFORE({date 2},'${addDays(maxYmd, 2)}'))`,
    "fields[]": ["Class ID", "Class!", "date 2", "time 2", "room", "class"],
  });
  const existingIds = new Set();
  const existingKeyCount = new Map();
  for (const r of existing) {
    const f = r.fields;
    if (f["Class ID"] != null) existingIds.add(String(f["Class ID"]));
    const t = parseTitle(Array.isArray(f["Class!"]) ? f["Class!"][0] : f["Class!"]);
    let k = null;
    if (t) k = keyOf(t.room, t.ymd, t.hhmm, t.cls);
    else if (f["date 2"] && f["room"] && f["class"]) k = keyOf(f["room"], ymdFromDate(new Date(f["date 2"])), hhmm(f["time 2"]), f["class"]);
    if (k) existingKeyCount.set(k, (existingKeyCount.get(k) || 0) + 1);
  }
  await note("✅", `${existing.length} classes already in Airtable around these dates`);

  // Active employees' MTEK names (for the instructor pre-check)
  const employees = await atListAll(EMPLOYEES_TABLE, { "fields[]": ["MTEK Name"] });
  const mtekNames = new Set(employees.map((e) => norm(e.fields["MTEK Name"])).filter(Boolean));

  // 6. Validate each row --------------------------------------------------------
  await step("6/7 Validating every class");
  const problems = new Map(); // reason -> [examples]
  const addProblem = (reason, example) => {
    if (!problems.has(reason)) problems.set(reason, []);
    problems.get(reason).push(example);
  };
  const toCreate = []; // { fields, label, instructors, noMatch }
  const preview = [];
  let alreadyCount = 0;
  let fixCount = 0;
  let noInstructorCount = 0;
  const unmatchedNames = new Map();
  const seenKeys = new Map();
  const perStudio = new Map();
  const times = [];

  for (const row of rows) {
    const location = String(col(row, "Location") ?? "").trim();
    const ymd = String(col(row, "Class Date") ?? "").trim();
    const time = hhmm(col(row, "Class Time"));
    const type = String(col(row, "Class Type") ?? "").trim();
    const instr = String(col(row, "Instructors") ?? "").trim();
    const label = prettyClass(ymd, time, location, type);
    const fail = (reason) => {
      fixCount++;
      addProblem(reason, label);
      preview.push({ t: label, s: "fix", i: instr, n: reason });
    };

    if (!location || !/^\d{4}-\d{2}-\d{2}$/.test(ymd) || !time || !type) {
      fail("Row is missing a location, date, time or class type");
      continue;
    }
    if (ymd < minYmd || ymd > maxYmd) {
      fail(`Class date outside the requested range (${ymd})`);
      continue;
    }
    const sessionKey = keyOf(location, ymd, time, type);
    const dupKey = sessionKey;

    // Select values must already exist (typecast is OFF).
    if (!roomChoices.has(location)) { fail(`Location "${location}" is not an option of the "room" field`); continue; }
    if (!classChoices.has(type)) { fail(`Class type "${type}" is not an option of the "class" field`); continue; }
    const subValue = String(col(row, "Has Substitute?") ?? "false").toLowerCase();
    if (!subChoices.has(subValue)) { fail(`"Is Substitute?" value "${subValue}" is not an option`); continue; }
    const rawDay = String(col(row, "Class Day of Week") ?? "");
    const dayCandidates = [rawDay, rawDay.trim(), rawDay.trim().padEnd(9, " "), rawDay.trim().padEnd(8, " ")];
    const dayValue = dayCandidates.find((c) => c && dayChoices.has(c));
    if (rawDay.trim() && !dayValue) { fail(`Day of week "${rawDay.trim()}" is not an option`); continue; }

    // Class ID from MTEK
    const candidates = sessionByKey.get(sessionKey) || [];
    let classId = null;
    if (candidates.length === 1) classId = candidates[0].id;
    else if (candidates.length > 1) {
      const cr = norm(col(row, "Classroom"));
      classId = (candidates.find((c) => norm(c.classroom) === cr) || {}).id || null;
    }

    // Duplicate check: Class ID first, then room+date+time+class for rows that have no ID yet.
    const batchCount = (seenKeys.get(dupKey) || 0) + 1;
    seenKeys.set(dupKey, batchCount);
    let isDuplicate = false;
    let dupReason = "";
    if (classId && existingIds.has(classId)) {
      isDuplicate = true;
      dupReason = `Class ID ${classId} already in Airtable`;
    } else if ((existingKeyCount.get(dupKey) || 0) >= batchCount) {
      isDuplicate = true;
      dupReason = "Same room/date/time/class already in Airtable";
    }
    if (isDuplicate) {
      alreadyCount++;
      preview.push({ t: label, s: "exists", i: instr, n: dupReason });
      continue;
    }
    if (classId) existingIds.add(classId); // never create the same Class ID twice in one run

    // Instructor pre-check (the Airtable automations do the real linking on create)
    let noMatch = false;
    if (instr) {
      const parts = instr.split(",").map((s) => s.trim()).filter(Boolean);
      const missing = parts.filter((p) => !mtekNames.has(norm(p)));
      if (missing.length) {
        noMatch = true;
        noInstructorCount++;
        for (const m of missing) unmatchedNames.set(m, (unmatchedNames.get(m) || 0) + 1);
      }
    } else {
      noMatch = true;
      noInstructorCount++;
      unmatchedNames.set("(no instructor in MTEK)", (unmatchedNames.get("(no instructor in MTEK)") || 0) + 1);
    }

    const num = (v) => (v == null || v === "" ? "" : String(v));
    const fields = {
      room: location,
      "date 2": torontoMidnightIso(ymd),
      "time 2": time,
      "Zingfit Official Name": instr,
      "Is Substitute?": subValue,
      Classroom: num(col(row, "Classroom")),
      "Class Tags": num(col(row, "Class Tags")),
      "Class if Free": num(col(row, "Class Is Free?")),
      class: type,
      "Class Category": num(col(row, "Class Category")),
      "Pending Standard Reservations": num(col(row, "Pending Standard Reservations")),
      "Pending Standby Reservations": num(col(row, "Pending Standby Reservations")),
      "Pending Waitlist Reservations": num(col(row, "Pending Waitlist Reservations")),
      "Checked In Reservations": num(col(row, "Checked In Reservations")),
      "Late Cancelled Reservations": num(col(row, "Late Cancelled Reservations")),
      "No Showed Reservations": num(col(row, "No Showed Reservations")),
      "Admin Holds": num(col(row, "Admin Holds")),
      "Unavailable Holds": num(col(row, "Unavailable Holds")),
      "Layout Capacity": num(col(row, "Layout Capacity")),
      "Actual Capacity": num(col(row, "Actual Capacity")),
      "% Utilization": num(col(row, "% Utilization")),
    };
    if (dayValue) fields["Class Day Of Week"] = dayValue;
    if (classId) fields["Class ID"] = Number(classId);
    // Airtable rejects "" for some types; drop empty strings to leave the cell blank.
    for (const k of Object.keys(fields)) if (fields[k] === "") delete fields[k];

    toCreate.push({ fields, label, noMatch, instr });
    preview.push({ t: label, s: "new", i: instr, m: noMatch ? 0 : 1, id: classId || "" });
    perStudio.set(location, (perStudio.get(location) || 0) + 1);
    times.push(`${ymd} ${time}`);
  }

  times.sort();
  const first = times[0] ? `${times[0]}` : "";
  const last = times.length ? times[times.length - 1] : "";
  const withoutId = toCreate.filter((c) => c.fields["Class ID"] == null).length;

  await note("✅", `${rows.length} classes checked: ${toCreate.length} new, ${alreadyCount} already in Airtable (skipped), ${fixCount} need fixing`);
  if (toCreate.length) {
    await note("ℹ️", `New classes run from ${first} to ${last}. By studio: ${[...perStudio].map(([k, v]) => `${k} ${v}`).join(", ")}`);
  }
  if (withoutId) await note("⚠️", `${withoutId} new class(es) have no MTEK Class ID yet (it will be filled by the nightly Resolve Class ID job)`);
  for (const [reason, examples] of problems) {
    await note("❌", `Needs fixing - ${reason}: ${examples.length} class(es), e.g. ${examples.slice(0, 5).join("; ")}`);
  }
  if (noInstructorCount) {
    const top = [...unmatchedNames].sort((a, b) => b[1] - a[1]).slice(0, 12).map(([n, c]) => `${n} (${c})`).join(", ");
    await note("⚠️", `${noInstructorCount} new class(es) have an instructor name with no match in Active Employees "MTEK Name": ${top}. They will appear on the Check Instructors page.`);
  }

  const common = {
    "Classes found": rows.length,
    "Already in Airtable": alreadyCount,
    "To create": toCreate.length,
    "Needs fixing": fixCount,
    "No instructor match": noInstructorCount,
    "First class": first,
    "Last class": last,
    "Preview JSON": JSON.stringify(preview.slice(0, 700)),
  };

  // Dry run ends here.
  if (mode === "Dry run") {
    await step("7/7 Dry run finished - nothing was written to Airtable", common);
    await note("✅", `DRY RUN: ${toCreate.length} class(es) would be created, ${alreadyCount} skipped as duplicates, ${fixCount} need fixing.`);
    await patchRun({ ...common, Status: "Ready (dry run done)", "Finished at": new Date().toISOString(), Notes: notes.join("\n") });
    return;
  }

  // 7. Import ------------------------------------------------------------------
  await step(`7/7 Creating ${toCreate.length} class(es) in Airtable`, common);
  const createdIds = [];
  let failed = 0;
  const failedLabels = [];
  for (let i = 0; i < toCreate.length; i += 10) {
    const chunk = toCreate.slice(i, i + 10);
    try {
      const res = await at("POST", `${BASE_ID}/${CLASSES_TABLE}`, { typecast: false, records: chunk.map((c) => ({ fields: c.fields })) });
      createdIds.push(...(res.records || []).map((r) => r.id));
    } catch (batchErr) {
      // isolate the bad record(s)
      for (const c of chunk) {
        try {
          const res = await at("POST", `${BASE_ID}/${CLASSES_TABLE}`, { typecast: false, records: [{ fields: c.fields }] });
          createdIds.push(...(res.records || []).map((r) => r.id));
        } catch (e) {
          failed++;
          failedLabels.push(`${c.label} (${e.message.replace(/^Airtable POST [^:]+: /, "").slice(0, 120)})`);
        }
        await sleep(220);
      }
    }
    await patchRun({ "Current step": `Creating classes: ${createdIds.length + failed}/${toCreate.length}`, Created: createdIds.length, Failed: failed });
    await sleep(250);
  }
  await note("✅", `Created ${createdIds.length} of ${toCreate.length} class(es)`);
  for (const f of failedLabels.slice(0, 15)) await note("❌", `Not created: ${f}`);

  // Verify what actually landed + wait for the instructor automations.
  await patchRun({ "Current step": "Verifying the import" });
  await sleep(20000);
  let verified = 0;
  let noInstructorNow = 0;
  const noInstructorLabels = [];
  for (let i = 0; i < createdIds.length; i += 40) {
    const ids = createdIds.slice(i, i + 40);
    const recs = await atListAll(CLASSES_TABLE, {
      filterByFormula: `OR(${ids.map((id) => `RECORD_ID()='${id}'`).join(",")})`,
      "fields[]": ["Class!", "Instructor", "Zingfit Official Name"],
    });
    verified += recs.length;
    for (const r of recs) {
      if (!r.fields["Instructor"] || r.fields["Instructor"].length === 0) {
        noInstructorNow++;
        noInstructorLabels.push(`${Array.isArray(r.fields["Class!"]) ? r.fields["Class!"][0] : r.fields["Class!"]} [${r.fields["Zingfit Official Name"] || "-"}]`);
      }
    }
  }
  await note(verified === createdIds.length ? "✅" : "❌", `Verified ${verified} of ${createdIds.length} created class(es) are in Airtable`);
  if (noInstructorNow) {
    await note("⚠️", `${noInstructorNow} imported class(es) have no instructor linked yet - fix them on the Check Instructors page: ${noInstructorLabels.slice(0, 8).join("; ")}${noInstructorLabels.length > 8 ? "; ..." : ""}`);
  } else if (createdIds.length) {
    await note("✅", "All imported classes have an instructor linked");
  }

  const allGood = failed === 0 && fixCount === 0 && verified === createdIds.length && noInstructorNow === 0;
  const summary =
    `Imported ${createdIds.length} of ${rows.length} class(es) found in MTEK for ${studiosLabel}, ${minYmd} to ${maxYmd}. ` +
    `${alreadyCount} already existed (skipped), ${fixCount} need fixing, ${failed} failed, ${noInstructorNow} without instructor.`;
  await note(allGood ? "🎉" : "⚠️", summary);
  await patchRun({
    ...common,
    Created: createdIds.length,
    Failed: failed,
    "No instructor match": noInstructorNow,
    Status: allGood ? "Completed" : "Completed with warnings",
    "Current step": allGood ? "Done" : "Done - see notes",
    "Finished at": new Date().toISOString(),
    Notes: notes.join("\n"),
  });
}

main().catch(async (err) => {
  console.error(err);
  if (AIRTABLE_TOKEN && RUN_ID) {
    notes.push(`${stamp()} ❌ ${err.message}`);
    await patchRun({
      Status: "Failed",
      "Current step": "Failed",
      Notes: notes.join("\n"),
      "Finished at": new Date().toISOString(),
    });
  }
  process.exit(1);
});
