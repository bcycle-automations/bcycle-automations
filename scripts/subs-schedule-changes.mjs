/**
 * Subs - Schedule changes. Replaces the manual paste steps (views 1-3) of the table
 * "Fix post subs entered (16th of each month) NEW" (STAGING table) - the Airtable automation
 * "Fix schedule (NEW)" stays the thing that actually changes All Classes, so its run history
 * remains the audit log.
 *
 * Called from scripts/subs-import-mtek-classes.mjs when the Import Runs row has
 *
 *   Mode = "Check changes"
 *     1. empties the staging table (the old process also deleted the rows before each run)
 *     2. pulls the MTEK report (API, dates always DD/MM/YYYY) and compares it with All Classes
 *     3. writes ONE staging row per class that is not already correct, with the same columns the
 *        paste used to fill ("All Classes Link" = the existing class, DATE TIME FORMAT = DD/MM/YYYY)
 *        so the staging table's own formula "Category (Form)" decides what the automation will do
 *     4. returns the list to the Softr page (nothing in All Classes is touched)
 *
 *   Mode = "Apply changes"
 *     ticks the "Fix" box on the staging rows the person selected -> the automation runs.
 *     Then waits and verifies in All Classes that each change really happened.
 *
 * Classes where Airtable already has a sub covering the regular instructor (MTEK still shows the
 * regular instructor) are shown as information only - no staging row is created for them.
 */

import { fetchMtekReport } from "./lib/mtek-report.mjs";

const STAGING_TABLE = process.env.AIRTABLE_STAGING_TABLE_ID || "tblTutoySkMiu7TvD";
const MAX_RANGE_DAYS = 93;
const MTEK_REPORT_ID = "294";
const MTEK_REPORT_SLUG = "utilization";

const norm = (s) => String(s ?? "").trim().toLowerCase();
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
function prettyClass(ymd, time, room, cls) {
  const m = /^\d{4}-(\d{2})-(\d{2})$/.exec(ymd || "");
  const t = /^(\d{2}):(\d{2})$/.exec(time || "");
  if (!m || !t) return [ymd, time, room, cls].filter(Boolean).join(" ");
  let h = Number(t[1]);
  const ap = h >= 12 ? "pm" : "am";
  h = h % 12 || 12;
  return `${MONTHS[Number(m[1]) - 1]} ${Number(m[2])}, ${h}:${t[2]}${ap} · ${room} · ${cls}`;
}
const keyOf = (room, ymd, time, cls) => `${norm(room)}|${ymd}|${time}|${norm(cls)}`;
const ddmmyyyy = (ymd) => `${ymd.slice(8, 10)}/${ymd.slice(5, 7)}/${ymd.slice(0, 4)}`;

const CATEGORY_MAP = {
  "new instructor on timeslot": "instructor",
  "sub not assigned in airtable": "sub",
  "new timeslot added to schedule": "new",
  "all good": "good",
};

export async function runChanges(ctx) {
  if (ctx.run["Mode"] === "Apply changes") return applyChanges(ctx);

  const { at, atListAll, patchRun, note, step, notes, run, BASE_ID, CLASSES_TABLE, EMPLOYEES_TABLE,
    MTEK_BASE_URL, MTEK_TOKEN, fetchClassSessions, parseTitle, hhmm, ymdFromDate, addDays } = ctx;

  const minYmd = run["Min date"];
  const maxYmd = run["Max date"];
  const studiosRaw = String(run["Studios"] || "All").trim();
  const allStudios = /^all$/i.test(studiosRaw) || studiosRaw === "";
  const studios = allStudios ? [] : studiosRaw.split(",").map((s) => s.trim()).filter(Boolean);
  const studiosLabel = allStudios ? "All studios" : studios.join(", ");

  await patchRun({
    Status: "Running", "Started at": new Date().toISOString(), "Finished at": null, Notes: "", "Preview JSON": "",
    "Classes found": null, "Already in Airtable": null, "To create": null, Created: null, Failed: null,
    "Needs fixing": null, "No instructor match": null, "First class": null, "Last class": null,
  });

  // 1. Inputs
  await step("Step 1 of 6: Checking your dates and studios…");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(minYmd || "") || !/^\d{4}-\d{2}-\d{2}$/.test(maxYmd || "")) throw new Error("From date and To date are required.");
  if (minYmd > maxYmd) throw new Error("The From date is after the To date.");
  const rangeDays = Math.round((new Date(maxYmd) - new Date(minYmd)) / 86400000) + 1;
  if (rangeDays > MAX_RANGE_DAYS) throw new Error(`The date range is ${rangeDays} days; the maximum is ${MAX_RANGE_DAYS}. Split it into smaller ranges.`);

  // 2. Employees
  await step("Step 2 of 6: Reading the employees in Airtable…");
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
    if (mtek) {
      const k = norm(mtek);
      if (!empByMtek.has(k)) empByMtek.set(k, []);
      empByMtek.get(k).push(e.id);
    }
  }

  // 3. MTEK
  await step("Step 3 of 6: Getting the classes from MTEK (this can take a minute)…");
  const report = await fetchMtekReport({
    baseUrl: MTEK_BASE_URL, token: MTEK_TOKEN, reportId: MTEK_REPORT_ID, slug: MTEK_REPORT_SLUG, pageSize: 500,
    dateParams: { min_start_date_day: minYmd, max_start_date_day: maxYmd }, pollTimeoutMs: 8 * 60000,
  });
  const H = Object.fromEntries(report.headers.map((h, i) => [h, i]));
  for (const h of ["Location", "Class Date", "Class Time", "Instructors", "Class Type"]) {
    if (!(h in H)) throw new Error(`The MTEK report is missing the "${h}" column.`);
  }
  const col = (row, name) => (name in H ? row[H[name]] : undefined);
  let rows = report.rows;
  if (!allStudios) {
    const wanted = new Set(studios.map(norm));
    rows = rows.filter((r) => wanted.has(norm(col(r, "Location"))));
  }
  const roomBooked = rows.filter((r) => norm(col(r, "Class Type")) === "room booked").length;
  rows = rows.filter((r) => norm(col(r, "Class Type")) !== "room booked");

  // 4. Class IDs
  await step("Step 4 of 6: Matching MTEK class numbers…");
  const sessions = await fetchClassSessions(minYmd, maxYmd);
  const sessionByKey = new Map();
  for (const s of sessions) {
    const a = s.attributes || {};
    if (!a.start_date || !a.start_time) continue;
    const k = keyOf(a.location_display, a.start_date, hhmm(a.start_time), a.class_type_display);
    if (!sessionByKey.has(k)) sessionByKey.set(k, []);
    sessionByKey.get(k).push({ id: String(s.id), classroom: a.classroom_display });
  }

  // 5. Airtable classes
  await step("Step 5 of 6: Comparing with what is in Airtable…");
  const existing = await atListAll(CLASSES_TABLE, {
    filterByFormula: `AND(IS_AFTER({date 2},'${addDays(minYmd, -2)}'),IS_BEFORE({date 2},'${addDays(maxYmd, 2)}'))`,
    "fields[]": ["Class ID", "Class!", "date 2", "time 2", "room", "class", "Instructor", "FINAL Sub", "Zingfit Official Name"],
  });
  const byId = new Map();
  const byKey = new Map();
  for (const r of existing) {
    const f = r.fields;
    if (f["Class ID"] != null) byId.set(String(f["Class ID"]), r);
    const t = parseTitle(Array.isArray(f["Class!"]) ? f["Class!"][0] : f["Class!"]);
    let k = null;
    if (t) k = keyOf(t.room, t.ymd, t.hhmm, t.cls);
    else if (f["date 2"] && f["room"] && f["class"]) k = keyOf(f["room"], ymdFromDate(new Date(f["date 2"])), hhmm(f["time 2"]), f["class"]);
    if (k) {
      if (!byKey.has(k)) byKey.set(k, []);
      byKey.get(k).push(r);
    }
  }

  const usedRecs = new Set();
  const stage = []; // classes that need a staging row
  const mtekAhead = [];
  let good = 0;
  let noMtekInstructor = 0;

  for (const row of rows) {
    const location = String(col(row, "Location") ?? "").trim();
    const ymd = String(col(row, "Class Date") ?? "").trim();
    const time = hhmm(col(row, "Class Time"));
    const type = String(col(row, "Class Type") ?? "").trim();
    const mtekName = String(col(row, "Instructors") ?? "").trim();
    const isSub = String(col(row, "Has Substitute?") ?? "false").toLowerCase() === "true";
    const label = prettyClass(ymd, time, location, type);
    if (!location || !/^\d{4}-\d{2}-\d{2}$/.test(ymd) || !time || !type) continue;
    if (ymd < minYmd || ymd > maxYmd) continue;
    if (!mtekName) {
      noMtekInstructor++;
      continue; // same as the old view: rows without an instructor are never looked at
    }

    const sessionKey = keyOf(location, ymd, time, type);
    const cands = sessionByKey.get(sessionKey) || [];
    let classId = null;
    if (cands.length === 1) classId = cands[0].id;
    else if (cands.length > 1) {
      const cr = norm(col(row, "Classroom"));
      classId = (cands.find((c) => norm(c.classroom) === cr) || {}).id || null;
    }
    let rec = classId ? byId.get(classId) : null;
    if (rec && usedRecs.has(rec.id)) rec = null;
    if (!rec) rec = (byKey.get(sessionKey) || []).find((r) => !usedRecs.has(r.id)) || null;

    if (!rec) {
      stage.push({ row, location, ymd, time, type, mtekName, isSub, label, classId, rec: null });
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
    // Airtable has a sub and MTEK still shows the regular instructor: Airtable is ahead of MTEK.
    const instrEmps = instrIds.map((id) => empById.get(id)).filter(Boolean);
    const instrSet = new Set(instrEmps.flatMap((e) => [norm(e.mtek), norm(e.primary)]).filter(Boolean));
    if (finalIds.length > 0 && instrEmps.length > 0 && instrEmps.length === wanted.length && wanted.every((w) => instrSet.has(w))) {
      const subLabel = finalIds.map((id) => empById.get(id)).filter(Boolean).map((e) => e.mtek || e.primary).join(", ");
      mtekAhead.push({
        rid: `info:${rec.id}`, t: label, c: "mtek", info: true,
        from: `Instructor ${instrEmps.map((e) => e.mtek || e.primary).join(", ")} · Sub ${subLabel}`, to: mtekName, bad: "",
      });
      continue;
    }
    const curLabel = curEmps.map((e) => e.mtek || e.primary).join(", ");
    stage.push({ row, location, ymd, time, type, mtekName, isSub, label, classId, rec, curLabel, hasFinal: finalIds.length > 0 });
  }

  // 6. Staging table: empty it, then one row per class that needs attention
  await step("Step 6 of 6: Preparing the list of changes…");
  const old = await atListAll(STAGING_TABLE, { "fields[]": ["Fix"] });
  for (let i = 0; i < old.length; i += 10) {
    await at("DELETE", `${BASE_ID}/${STAGING_TABLE}`, null, { "records[]": old.slice(i, i + 10).map((r) => r.id) });
    await sleep(220);
  }

  const num = (v) => (v == null || v === "" ? undefined : String(v));
  const created = [];
  const toCreate = stage.map((c) => {
    const r = c.row;
    const subVal = String(col(r, "Has Substitute?") ?? "false").toLowerCase();
    const dayVal = String(col(r, "Class Day of Week") ?? "");
    const fields = {
      room: c.location,
      date: ddmmyyyy(c.ymd),
      time: c.time,
      "Class Day Of Week": dayVal.trim() ? dayVal : undefined,
      "Instructor name (FROM MTEK)": c.mtekName,
      "Is substitute?": subVal,
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
    return { c, fields };
  });
  for (let i = 0; i < toCreate.length; i += 10) {
    const chunk = toCreate.slice(i, i + 10);
    const res = await at("POST", `${BASE_ID}/${STAGING_TABLE}`, { typecast: true, records: chunk.map((x) => ({ fields: x.fields })) });
    (res.records || []).forEach((rec, j) => created.push({ id: rec.id, c: chunk[j].c }));
    await sleep(250);
  }

  // let the staging formulas (Class!, Category (Form)) calculate, then read them back
  await sleep(Math.min(12000, 3000 + created.length * 40));
  const readBack = new Map();
  for (let i = 0; i < created.length; i += 40) {
    const ids = created.slice(i, i + 40).map((x) => x.id);
    const recs = await atListAll(STAGING_TABLE, {
      filterByFormula: `OR(${ids.map((id) => `RECORD_ID()='${id}'`).join(",")})`,
      "fields[]": ["Category (Form)", "Class!"],
    });
    for (const r of recs) readBack.set(r.id, r.fields);
  }

  const changes = created.map(({ id, c }) => {
    const formula = String(readBack.get(id)?.["Category (Form)"] ?? "");
    let cat = CATEGORY_MAP[norm(formula)] || (c.rec ? "instructor" : "new");
    if (cat === "new" && c.rec) cat = "blank"; // class exists but has nobody: the automation fills it in
    const names = c.mtekName.split(",").map((x) => x.trim()).filter(Boolean);
    const missing = names.filter((w) => !(empByMtek.get(norm(w)) || []).length);
    let warn = "";
    if (missing.length && cat !== "good") {
      warn = cat === "new"
        ? `No employee has the MTEK Name "${missing.join('", "')}" yet, so the class will be added without an instructor`
        : `No employee has the MTEK Name "${missing.join('", "')}", so the instructor cannot be set`;
    }
    return {
      rid: id, t: c.label, c: cat, from: c.rec ? (c.hasFinal ? `Sub ${c.curLabel}` : c.curLabel) : "", to: c.mtekName,
      bad: "", warn, cls: c.rec ? c.rec.id : "", cid: c.classId || "", ymd: c.ymd, time: c.time, loc: c.location, type: c.type, formula,
    };
  });

  const actionable = changes.filter((c) => c.c !== "good");
  const byCat = (k) => actionable.filter((x) => x.c === k).length;
  const warnRows = actionable.filter((c) => c.warn);

  await note("📅", `MTEK has ${plural(rows.length, "class", "classes")} for ${studiosLabel}, ${minYmd} to ${maxYmd}.`);
  await note("✅", `${plural(good, "class is", "classes are")} already correct in Airtable.`);
  if (actionable.length) {
    await note("🔄", `${plural(actionable.length, "change", "changes")} found:`);
    if (byCat("instructor")) await note("   ", `- ${plural(byCat("instructor"), "class has", "classes have")} a new instructor in MTEK.`);
    if (byCat("sub")) await note("   ", `- ${plural(byCat("sub"), "class has", "classes have")} a substitute in MTEK that is not assigned in Airtable.`);
    if (byCat("blank")) await note("   ", `- ${plural(byCat("blank"), "class has", "classes have")} no instructor in Airtable yet but MTEK has one.`);
    if (byCat("new")) await note("   ", `- ${plural(byCat("new"), "class is", "classes are")} in MTEK but not in Airtable yet (new time slots).`);
  } else {
    await note("✅", "No changes needed.");
  }
  if (mtekAhead.length) {
    await note("ℹ️", `${plural(mtekAhead.length, "class has", "classes have")} a substitute assigned in Airtable that MTEK does not show yet. Nothing to change in Airtable — update MTEK if needed.`);
  }
  if (warnRows.length) {
    await note("⚠️", `${plural(warnRows.length, "change has", "changes have")} an instructor name that does not match an employee's MTEK Name. Fix it in Active Employees (or pick the instructor on Check Instructors afterwards).`);
  }
  if (noMtekInstructor) await note("•", `${plural(noMtekInstructor, "class has", "classes have")} no instructor in MTEK yet, so nothing to compare.`);
  if (roomBooked) await note("•", `${plural(roomBooked, "room booking (ROOM BOOKED) was", "room bookings (ROOM BOOKED) were")} skipped.`);
  await note("ℹ️", `The changes are loaded in the Airtable table "Fix post subs entered (16th of each month) NEW". Applying them runs the Airtable automation "Fix schedule (NEW)", so its run history is the audit log.`);

  const times = actionable.filter((c) => c.ymd).map((c) => `${c.ymd} ${c.time}`).sort();
  await note("✅", actionable.length ? `Nothing has been changed yet. Tick the changes you want and click "Apply".` : "Nothing to apply.");
  await patchRun({
    "Classes found": rows.length,
    "Already in Airtable": good,
    "To create": actionable.length,
    "Needs fixing": 0,
    "No instructor match": warnRows.length,
    "First class": times[0] ? `${times[0]}` : "",
    "Last class": times.length ? times[times.length - 1] : "",
    "Preview JSON": JSON.stringify([...actionable, ...mtekAhead].slice(0, 700)),
    Status: "Ready (dry run done)",
    "Current step": actionable.length ? "Check finished. Nothing was changed yet." : "Check finished. Nothing to change.",
    "Finished at": new Date().toISOString(), Notes: notes.join("\n"),
  });
}

/** Mode "Apply changes": tick Fix on the selected staging rows, then verify the automation did its job. */
async function applyChanges(ctx) {
  const { at, atListAll, patchRun, note, step, notes, run, BASE_ID, CLASSES_TABLE, RUNS_TABLE } = ctx;
  await patchRun({ Status: "Running", "Started at": new Date().toISOString(), "Finished at": null, Notes: "", Created: null, Failed: null });

  let selected;
  try {
    selected = [...new Set(JSON.parse(run["Selected changes"] || "[]"))].filter((x) => /^rec/.test(String(x)));
  } catch {
    selected = [];
  }
  if (selected.length === 0) throw new Error("No changes were selected. Tick the changes you want to apply and try again.");

  // the check this apply belongs to (same studios + dates): its Preview JSON says what we expect to happen
  await step("Step 1 of 3: Loading the selected changes…");
  const checks = await atListAll(RUNS_TABLE, {
    filterByFormula: `AND({Mode}='Check changes',{Status}='Ready (dry run done)')`,
    "fields[]": ["Preview JSON", "Studios", "Min date", "Max date", "Started at"],
    "sort[0][field]": "Started at",
    "sort[0][direction]": "desc",
    pageSize: 20,
  });
  const check = checks.find(
    (c) => String(c.fields["Studios"] || "") === String(run["Studios"] || "") && c.fields["Min date"] === run["Min date"] && c.fields["Max date"] === run["Max date"],
  );
  let expected = [];
  try {
    expected = JSON.parse(check?.fields["Preview JSON"] || "[]");
  } catch {
    expected = [];
  }
  const expectedById = new Map(expected.map((e) => [e.rid, e]));

  // still in the staging table and not ticked yet?
  const live = [];
  for (let i = 0; i < selected.length; i += 40) {
    const ids = selected.slice(i, i + 40);
    const recs = await atListAll(STAGING_TABLE, {
      filterByFormula: `OR(${ids.map((id) => `RECORD_ID()='${id}'`).join(",")})`,
      "fields[]": ["Fix", "Category (Form)", "Class!"],
    });
    live.push(...recs);
  }
  const liveById = new Map(live.map((r) => [r.id, r]));
  const missing = selected.filter((id) => !liveById.has(id));
  const already = live.filter((r) => r.fields["Fix"]).map((r) => r.id);
  const todo = live.filter((r) => !r.fields["Fix"] && norm(r.fields["Category (Form)"]) !== "all good").map((r) => r.id);
  if (missing.length) await note("⚠️", `${plural(missing.length, "selected change is", "selected changes are")} no longer in the list (a newer check replaced it) and was skipped.`);
  if (already.length) await note("•", `${plural(already.length, "change was", "changes were")} already applied and was skipped.`);
  if (todo.length === 0) throw new Error("Nothing to apply: the selected changes are no longer in the list. Run “Check for changes” again.");

  // tick the box -> the Airtable automation "Fix schedule (NEW)" runs once per row
  await step(`Step 2 of 3: Ticking Fix on ${plural(todo.length, "row", "rows")} (the Airtable automation applies them)…`);
  const ticked = [];
  for (let i = 0; i < todo.length; i += 10) {
    const chunk = todo.slice(i, i + 10);
    await at("PATCH", `${BASE_ID}/${STAGING_TABLE}`, { typecast: false, records: chunk.map((id) => ({ id, fields: { Fix: true } })) });
    ticked.push(...chunk);
    await patchRun({ "Current step": `Ticking Fix… ${ticked.length} of ${todo.length}` });
    await sleep(400);
  }
  await note("✅", `Ticked Fix on ${plural(ticked.length, "row", "rows")}. The Airtable automation "Fix schedule (NEW)" is applying them.`);

  // verify the effect in All Classes (the automation needs a few seconds per row)
  await step("Step 3 of 3: Waiting for the automation and double-checking All Classes…");
  const want = ticked.map((id) => expectedById.get(id)).filter(Boolean);
  const unknown = ticked.length - want.length;
  let pending = want.slice();
  const deadline = Date.now() + 4 * 60000;
  const q = (s) => String(s).replace(/'/g, "");
  const classOf = async (e) => {
    const fields = ["Instructor", "FINAL Sub", "Zingfit Official Name"];
    if (e.cls) return (await atListAll(CLASSES_TABLE, { filterByFormula: `RECORD_ID()='${e.cls}'`, "fields[]": fields }))[0] || null;
    if (e.cid) {
      const r = (await atListAll(CLASSES_TABLE, { filterByFormula: `{Class ID}=${Number(e.cid)}`, "fields[]": fields }))[0];
      if (r) return r;
    }
    const recs = await atListAll(CLASSES_TABLE, {
      filterByFormula: `AND(IS_SAME({date 2},'${e.ymd}','day'),{room}='${q(e.loc)}',{class}='${q(e.type)}',{time 2}='${e.time}')`,
      "fields[]": fields,
    });
    return recs[0] || null;
  };
  const done = [];
  while (pending.length && Date.now() < deadline) {
    await sleep(15000);
    const still = [];
    for (const e of pending) {
      let rec = null;
      try {
        rec = await classOf(e);
      } catch {
        rec = null;
      }
      let ok = false;
      if (rec) {
        if (e.c === "new") ok = true;
        else if (e.c === "sub") ok = (rec.fields["FINAL Sub"] || []).length > 0;
        else ok = (rec.fields["Instructor"] || []).length > 0;
      }
      if (ok) done.push({ e, rec });
      else still.push(e);
    }
    pending = still;
    await patchRun({ "Current step": `Double-checking All Classes… ${done.length} of ${want.length} confirmed`, Created: done.length });
  }

  // new classes: the instructor is linked by the "Update Instructor" automation a moment after creation
  const newDone = done.filter((d) => d.e.c === "new");
  const noInstr = newDone.filter((d) => !(d.rec.fields["Instructor"] || []).length);
  await note(pending.length ? "⚠️" : "✅", pending.length
    ? `${done.length} of ${want.length} changes are confirmed in All Classes. ${pending.length} not confirmed yet:`
    : `Double-checked: all ${done.length} changes are in All Classes.`);
  for (const e of pending.slice(0, 10)) await note("   ", `- ${e.t} — open the run history of "Fix schedule (NEW)" in Airtable to see why.`);
  if (unknown) await note("•", `${plural(unknown, "row was", "rows were")} ticked but could not be double-checked automatically.`);
  if (noInstr.length) await note("⚠️", `${plural(noInstr.length, "added class has", "added classes have")} no instructor yet. Pick it on the Check Instructors page.`);

  const allGood = pending.length === 0 && noInstr.length === 0;
  await note(allGood ? "🎉" : "⚠️", allGood
    ? `All done: ${plural(ticked.length, "change", "changes")} applied by the Airtable automation.`
    : `Finished with things to look at: ${done.length} confirmed, ${pending.length} not confirmed.`);
  await patchRun({
    Created: done.length, Failed: pending.length,
    Status: allGood ? "Completed" : "Completed with warnings",
    "Current step": allGood ? "Done" : "Done - see the notes below",
    "Finished at": new Date().toISOString(), Notes: notes.join("\n"),
  });
}
