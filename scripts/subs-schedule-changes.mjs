/**
 * Subs - Schedule changes (replaces the 4 views of "Fix post subs entered (16th of each month) NEW"
 * and the Airtable automation "Fix schedule (NEW)").
 *
 * Called from scripts/subs-import-mtek-classes.mjs when the Import Runs row has
 *   Mode = "Check changes"  -> compare MTEK with Airtable and list the differences (writes nothing to All Classes)
 *   Mode = "Apply changes"  -> re-check, then apply only the changes whose All Classes record id is in
 *                              the run's "Selected changes" (JSON array).
 *
 * Same logic as the old Category (Form) formula + automation, for classes that ALREADY exist:
 *   - class has no instructor/sub in Airtable ........ "blank"       -> set Instructor + Zingfit Official Name (+ Run Points)
 *   - MTEK instructor = Airtable instructor-or-sub ... "good"        -> nothing
 *   - different and MTEK says it is a substitute ..... "sub"         -> set FINAL Sub
 *   - different otherwise ............................ "instructor"  -> set Instructor (+ Run Points)
 * - class is not in Airtable at all ............... "new"         -> create the class (same fields as the Import page,
 *                                                                     Run Points on); the "Update Instructor" automation links the instructor
 * Nothing is ever deleted or cancelled here.
 *
 * Differences from the old automation (safer): a change is NOT applied when no Active Employees
 * record has that MTEK Name (the old automation would have blanked the link), and nothing is
 * written until the person ticks it.
 */

import { fetchMtekReport } from "./lib/mtek-report.mjs";

const MAX_RANGE_DAYS = 93;
const MTEK_REPORT_ID = "294";
const MTEK_REPORT_SLUG = "utilization";

const norm = (s) => String(s ?? "").trim().toLowerCase();
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
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

// UTC ISO of local midnight in Toronto (how existing "date 2" values are stored)
function torontoMidnightIso(ymd) {
  const probe = new Date(`${ymd}T05:00:00Z`);
  const part = new Intl.DateTimeFormat("en-US", { timeZone: "America/Toronto", timeZoneName: "shortOffset" })
    .formatToParts(probe)
    .find((p) => p.type === "timeZoneName").value;
  const hrs = Number(part.replace("GMT", "") || 0);
  return new Date(new Date(`${ymd}T00:00:00Z`).getTime() - hrs * 3600000).toISOString();
}

export async function runChanges(ctx) {
  const { at, atListAll, patchRun, note, step, notes, run, BASE_ID, CLASSES_TABLE, EMPLOYEES_TABLE, RUN_ID,
    MTEK_BASE_URL, MTEK_TOKEN, fetchClassSessions, parseTitle, hhmm, ymdFromDate, addDays } = ctx;

  const apply = run["Mode"] === "Apply changes";
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
  await step("Step 1 of 5: Checking your dates and studios…");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(minYmd || "") || !/^\d{4}-\d{2}-\d{2}$/.test(maxYmd || "")) throw new Error("From date and To date are required.");
  if (minYmd > maxYmd) throw new Error("The From date is after the To date.");
  const rangeDays = Math.round((new Date(maxYmd) - new Date(minYmd)) / 86400000) + 1;
  if (rangeDays > MAX_RANGE_DAYS) throw new Error(`The date range is ${rangeDays} days; the maximum is ${MAX_RANGE_DAYS}. Split it into smaller ranges.`);

  let selected = null;
  if (apply) {
    try {
      selected = new Set(JSON.parse(run["Selected changes"] || "[]"));
    } catch {
      selected = new Set();
    }
    if (selected.size === 0) throw new Error("No changes were selected. Tick the changes you want to apply and try again.");
  }

  // 2. Employees
  await step("Step 2 of 5: Reading the employees in Airtable…");
  const meta = await at("GET", `meta/bases/${BASE_ID}/tables`);
  const empTable = (meta.tables || []).find((t) => t.id === EMPLOYEES_TABLE);
  const primaryName = empTable?.fields?.find((f) => f.id === empTable.primaryFieldId)?.name;
  const classesMeta = (meta.tables || []).find((t) => t.id === CLASSES_TABLE);
  const choicesOf = (name) => new Set(((classesMeta?.fields || []).find((f) => f.name === name)?.options?.choices || []).map((c) => c.name));
  const subChoices = choicesOf("Is Substitute?");
  const dayChoices = choicesOf("Class Day Of Week");
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
  await step("Step 3 of 5: Getting the classes from MTEK (this can take a minute)…");
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
  await step("Step 4 of 5: Matching MTEK class numbers…");
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
  await step("Step 5 of 5: Comparing with what is in Airtable…");
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
  const changes = [];
  let good = 0;
  const mtekAhead = [];
  const newRows = new Map();
  const newSeq = new Map();
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
      const seq = (newSeq.get(sessionKey) || 0) + 1;
      newSeq.set(sessionKey, seq);
      const rid = `new:${sessionKey}:${seq}`;
      const missingNames = mtekName.split(",").map((x) => x.trim()).filter((w) => w && !(empByMtek.get(norm(w)) || []).length);
      newRows.set(rid, { row, location, ymd, time, type, mtekName, isSub, classId });
      changes.push({
        rid, t: label, c: "new", from: "", to: mtekName, emp: [], bad: "", cid: classId || "", sub: isSub,
        warn: missingNames.length ? `No employee has the MTEK Name "${missingNames.join('", "')}" yet, so the class will be added without an instructor` : "",
      });
      continue;
    }
    usedRecs.add(rec.id);

    // current instructor-or-sub (FINAL Sub wins, like the "Instructor or sub" formula)
    const finalIds = rec.fields["FINAL Sub"] || [];
    const instrIds = rec.fields["Instructor"] || [];
    const curIds = finalIds.length ? finalIds : instrIds;
    const curEmps = curIds.map((id) => empById.get(id)).filter(Boolean);
    const curLabel = curEmps.map((e) => e.mtek || e.primary).join(", ");

    const wanted = mtekName.split(",").map((s) => norm(s)).filter(Boolean);
    const curSet = new Set(curEmps.flatMap((e) => [norm(e.mtek), norm(e.primary)]).filter(Boolean));
    const sameAsMtek = curIds.length > 0 && wanted.every((w) => curSet.has(w)) && curEmps.length === wanted.length;

    if (sameAsMtek) {
      good++;
      continue;
    }

    // Airtable has a sub (FINAL Sub) covering the regular instructor, and MTEK still shows the regular
    // instructor: Airtable is ahead of MTEK. Nothing to change in Airtable - MTEK has to be updated.
    const instrEmps = instrIds.map((id) => empById.get(id)).filter(Boolean);
    const instrSet = new Set(instrEmps.flatMap((e) => [norm(e.mtek), norm(e.primary)]).filter(Boolean));
    const mtekIsRegularInstructor = instrEmps.length > 0 && instrEmps.length === wanted.length && wanted.every((w) => instrSet.has(w));
    if (finalIds.length > 0 && mtekIsRegularInstructor) {
      const subLabel = finalIds.map((id) => empById.get(id)).filter(Boolean).map((e) => e.mtek || e.primary).join(", ");
      mtekAhead.push({ rid: rec.id, t: label, c: "mtek", info: true, from: `Instructor ${instrEmps.map((e) => e.mtek || e.primary).join(", ")} · Sub ${subLabel}`, to: mtekName, emp: [], bad: "", cid: classId || "", sub: isSub });
      continue;
    }
    let cat = curIds.length === 0 ? "blank" : isSub ? "sub" : "instructor";

    // the employee(s) MTEK says teach it
    const empIds = [];
    let problem = "";
    for (const w of mtekName.split(",").map((s) => s.trim()).filter(Boolean)) {
      const m = empByMtek.get(norm(w)) || [];
      if (m.length === 0) problem = `No employee in Airtable has the MTEK Name "${w}"`;
      else if (m.length > 1) problem = `More than one employee has the MTEK Name "${w}"`;
      else empIds.push(m[0]);
    }
    changes.push({ rid: rec.id, t: label, c: cat, from: finalIds.length ? `Sub ${curLabel}` : curLabel, to: mtekName, emp: problem ? [] : empIds, bad: problem, cid: classId || "", sub: isSub });
  }

  const applicable = changes.filter((c) => !c.bad);
  const blocked = changes.filter((c) => c.bad);
  const byCat = (c) => changes.filter((x) => x.c === c).length;

  // Summary (plain language)
  await note("📅", `MTEK has ${plural(rows.length, "class", "classes")} for ${studiosLabel}, ${minYmd} to ${maxYmd}.`);
  await note("✅", `${plural(good, "class is", "classes are")} already correct in Airtable.`);
  if (applicable.length) {
    await note("🔄", `${plural(applicable.length, "change", "changes")} found:`);
    if (byCat("instructor")) await note("   ", `- ${plural(byCat("instructor"), "class has", "classes have")} a new instructor in MTEK.`);
    if (byCat("sub")) await note("   ", `- ${plural(byCat("sub"), "class has", "classes have")} a substitute in MTEK that is not assigned in Airtable.`);
    if (byCat("blank")) await note("   ", `- ${plural(byCat("blank"), "class has", "classes have")} no instructor in Airtable yet but MTEK has one.`);
    if (byCat("new")) await note("   ", `- ${plural(byCat("new"), "class is", "classes are")} in MTEK but not in Airtable yet (new time slots). ${byCat("new") === 1 ? "It" : "They"} will be added.`);
  } else {
    await note("✅", "No changes needed.");
  }
  if (mtekAhead.length) {
    await note("ℹ️", `${plural(mtekAhead.length, "class has", "classes have")} a substitute assigned in Airtable that MTEK does not show yet. Nothing to change in Airtable — update MTEK if needed.`);
  }
  if (blocked.length) {
    await note("❌", `${plural(blocked.length, "change cannot", "changes cannot")} be applied because the MTEK name does not match an employee. Fix the MTEK Name in Active Employees, then check again:`);
    for (const b of blocked.slice(0, 8)) await note("   ", `- ${b.t}: ${b.bad}`);
  }
  const warnNew = changes.filter((c) => c.c === "new" && c.warn);
  if (warnNew.length) {
    await note("⚠️", `${plural(warnNew.length, "new class has", "new classes have")} an instructor name that does not match an employee yet. ${warnNew.length === 1 ? "It" : "They"} will be added without an instructor and show on Check Instructors.`);
  }
  if (noMtekInstructor) await note("•", `${plural(noMtekInstructor, "class has", "classes have")} no instructor in MTEK yet, so nothing to compare.`);
  if (roomBooked) await note("•", `${plural(roomBooked, "room booking (ROOM BOOKED) was", "room bookings (ROOM BOOKED) were")} skipped.`);

  const preview = [...changes, ...mtekAhead].map((c) => ({ ...c })).slice(0, 700);
  const common = {
    "Classes found": rows.length,
    "Already in Airtable": good,
    "To create": applicable.length,
    "Needs fixing": blocked.length,
    "No instructor match": warnNew.length,
  };

  if (!apply) {
    await note("✅", applicable.length
      ? `Nothing has been changed yet. Tick the changes you want and click "Apply".`
      : "Nothing to apply.");
    await patchRun({
      ...common, "Preview JSON": JSON.stringify(preview),
      Status: "Ready (dry run done)",
      "Current step": applicable.length ? "Check finished. Nothing was changed yet." : "Check finished. Nothing to change.",
      "Finished at": new Date().toISOString(), Notes: notes.join("\n"),
    });
    return;
  }

  // Apply ---------------------------------------------------------------------
  const todo = applicable.filter((c) => selected.has(c.rid));
  const skippedStale = [...selected].filter((rid) => !applicable.some((c) => c.rid === rid));
  await step(`Applying ${plural(todo.length, "change", "changes")}…`, common);
  if (skippedStale.length) {
    await note("⚠️", `${plural(skippedStale.length, "selected change is", "selected changes are")} no longer needed or can no longer be applied (the data changed since the check) and ${skippedStale.length === 1 ? "was" : "were"} skipped.`);
  }

  const patches = todo.filter((c) => c.c !== "new").map((c) => {
    const fields = {};
    if (c.c === "sub") {
      fields["FINAL Sub"] = c.emp.slice(0, 1);
    } else {
      fields["Instructor"] = c.emp;
      fields["Run Points"] = true;
      if (c.c === "blank") fields["Zingfit Official Name"] = c.to;
    }
    return { id: c.rid, fields, c };
  });

  const doneIds = [];
  const failed = [];
  for (let i = 0; i < patches.length; i += 10) {
    const chunk = patches.slice(i, i + 10);
    try {
      await at("PATCH", `${BASE_ID}/${CLASSES_TABLE}`, { typecast: false, records: chunk.map((p) => ({ id: p.id, fields: p.fields })) });
      doneIds.push(...chunk.map((p) => p.id));
    } catch {
      for (const p of chunk) {
        try {
          await at("PATCH", `${BASE_ID}/${CLASSES_TABLE}`, { typecast: false, records: [{ id: p.id, fields: p.fields }] });
          doneIds.push(p.id);
        } catch (e) {
          failed.push(`${p.c.t} (${e.message.replace(/^Airtable PATCH [^:]+: /, "").slice(0, 120)})`);
        }
      }
    }
    await patchRun({ "Current step": `Applying changes… ${doneIds.length + failed.length} of ${patches.length}`, Created: doneIds.length, Failed: failed.length });
  }
  if (patches.length) {
    await note(failed.length ? "⚠️" : "✅", `Applied ${doneIds.length} of ${plural(patches.length, "instructor/sub change", "instructor/sub changes")}.`);
  }
  for (const f of failed.slice(0, 10)) await note("❌", `Could not apply: ${f}`);

  // New classes (same fields as the Import page)
  const toAdd = todo.filter((c) => c.c === "new");
  const createdIds = [];
  const createFailed = [];
  if (toAdd.length) {
    await patchRun({ "Current step": `Adding ${plural(toAdd.length, "new class", "new classes")}…` });
    const num = (v) => (v == null || v === "" ? "" : String(v));
    const built = toAdd.map((c) => {
      const n = newRows.get(c.rid);
      const r = n.row;
      const subValue = String(col(r, "Has Substitute?") ?? "false").toLowerCase();
      const rawDay = String(col(r, "Class Day of Week") ?? "");
      const dayValue = [rawDay, rawDay.trim(), rawDay.trim().padEnd(9, " "), rawDay.trim().padEnd(8, " ")].find((x) => x && dayChoices.has(x)) || rawDay;
      const fields = {
        room: n.location,
        "date 2": torontoMidnightIso(n.ymd),
        "time 2": n.time,
        "Zingfit Official Name": n.mtekName,
        "Is Substitute?": subChoices.has(subValue) || subValue ? subValue : "",
        Classroom: num(col(r, "Classroom")),
        "Class Tags": num(col(r, "Class Tags")),
        "Class if Free": num(col(r, "Class Is Free?")),
        class: n.type,
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
        "Run Points": true,
      };
      if (dayValue.trim()) fields["Class Day Of Week"] = dayValue;
      if (n.classId) fields["Class ID"] = Number(n.classId);
      for (const k of Object.keys(fields)) if (fields[k] === "") delete fields[k];
      return { c, fields };
    });
    for (let i = 0; i < built.length; i += 10) {
      const chunk = built.slice(i, i + 10);
      try {
        const res = await at("POST", `${BASE_ID}/${CLASSES_TABLE}`, { typecast: true, records: chunk.map((b) => ({ fields: b.fields })) });
        createdIds.push(...(res.records || []).map((r) => r.id));
      } catch {
        for (const b of chunk) {
          try {
            const res = await at("POST", `${BASE_ID}/${CLASSES_TABLE}`, { typecast: true, records: [{ fields: b.fields }] });
            createdIds.push(...(res.records || []).map((r) => r.id));
          } catch (e) {
            createFailed.push(`${b.c.t} (${e.message.replace(/^Airtable POST [^:]+: /, "").slice(0, 120)})`);
          }
        }
      }
      await patchRun({ "Current step": `Adding new classes… ${createdIds.length + createFailed.length} of ${toAdd.length}`, Created: doneIds.length + createdIds.length, Failed: failed.length + createFailed.length });
    }
    await note(createFailed.length ? "⚠️" : "✅", `Added ${createdIds.length} of ${plural(toAdd.length, "new class", "new classes")} to Airtable.`);
    for (const f of createFailed.slice(0, 10)) await note("❌", `Could not add: ${f}`);
  }

  // Verify
  await patchRun({ "Current step": "Double-checking in Airtable…" });
  let verified = 0;
  for (let i = 0; i < doneIds.length; i += 40) {
    const ids = doneIds.slice(i, i + 40);
    const recs = await atListAll(CLASSES_TABLE, {
      filterByFormula: `OR(${ids.map((id) => `RECORD_ID()='${id}'`).join(",")})`,
      "fields[]": ["Instructor", "FINAL Sub"],
    });
    for (const r of recs) {
      const p = patches.find((x) => x.id === r.id);
      if (!p) continue;
      const field = p.c.c === "sub" ? "FINAL Sub" : "Instructor";
      const got = (r.fields[field] || []).slice().sort().join(",");
      const want = p.fields[field].slice().sort().join(",");
      if (got === want) verified++;
    }
  }
  let createdVerified = 0;
  let createdNoInstructor = 0;
  if (createdIds.length) {
    await patchRun({ "Current step": "Double-checking the new classes (waiting for instructors to link)…" });
    await new Promise((r) => setTimeout(r, 20000));
    for (let i = 0; i < createdIds.length; i += 40) {
      const ids = createdIds.slice(i, i + 40);
      const recs = await atListAll(CLASSES_TABLE, {
        filterByFormula: `OR(${ids.map((id) => `RECORD_ID()='${id}'`).join(",")})`,
        "fields[]": ["Instructor"],
      });
      createdVerified += recs.length;
      createdNoInstructor += recs.filter((r) => !r.fields["Instructor"] || r.fields["Instructor"].length === 0).length;
    }
    await note(createdVerified === createdIds.length ? "✅" : "❌", createdVerified === createdIds.length
      ? `Double-checked: all ${createdIds.length} new classes are in Airtable.`
      : `Only ${createdVerified} of ${createdIds.length} new classes could be found in Airtable. Please check the All Classes table.`);
    if (createdNoInstructor) await note("⚠️", `${plural(createdNoInstructor, "new class has", "new classes have")} no instructor yet. Pick it on the Check Instructors page.`);
    else await note("✅", "Every new class has an instructor.");
  }
  if (doneIds.length) {
    await note(verified === doneIds.length ? "✅" : "❌", verified === doneIds.length
      ? `Double-checked: all ${doneIds.length} changes are in Airtable.`
      : `Only ${verified} of ${doneIds.length} changes could be confirmed in Airtable. Please check the All Classes table.`);
  }
  const totalDone = doneIds.length + createdIds.length;
  const notApplied = applicable.length - totalDone;
  const allGood = failed.length === 0 && createFailed.length === 0 && verified === doneIds.length && createdVerified === createdIds.length && createdNoInstructor === 0;
  await note(allGood ? "🎉" : "⚠️", allGood
    ? `All done: ${plural(totalDone, "change", "changes")} applied.${notApplied > 0 ? ` ${notApplied} other change${notApplied === 1 ? " was" : "s were"} left for later.` : ""}`
    : `Finished with things to look at: ${totalDone} applied, ${failed.length + createFailed.length} failed, ${createdNoInstructor} new classes without an instructor.`);
  await patchRun({
    ...common, Created: totalDone, Failed: failed.length + createFailed.length,
    Status: allGood ? "Completed" : "Completed with warnings",
    "Current step": allGood ? "Done" : "Done - see the notes below",
    "Finished at": new Date().toISOString(), Notes: notes.join("\n"),
  });
}
