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
 * Classes MTEK has that are not in Airtable are only counted ("new class"): they are added with the
 * "Import classes from MTEK" page. Nothing is ever deleted or cancelled here.
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
  let newClasses = 0;
  let noMtekInstructor = 0;
  const newClassLabels = [];

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
      newClasses++;
      if (newClassLabels.length < 8) newClassLabels.push(label);
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
  if (newClasses) {
    await note("➕", `${plural(newClasses, "class is", "classes are")} in MTEK but not in Airtable yet. Add ${newClasses === 1 ? "it" : "them"} with the "Import classes from MTEK" page. They are not handled here.`);
  }
  if (noMtekInstructor) await note("•", `${plural(noMtekInstructor, "class has", "classes have")} no instructor in MTEK yet, so nothing to compare.`);
  if (roomBooked) await note("•", `${plural(roomBooked, "room booking (ROOM BOOKED) was", "room bookings (ROOM BOOKED) were")} skipped.`);

  const preview = [...changes, ...mtekAhead].map((c) => ({ ...c })).slice(0, 700);
  const common = {
    "Classes found": rows.length,
    "Already in Airtable": good,
    "To create": applicable.length,
    "Needs fixing": blocked.length,
    "No instructor match": newClasses,
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

  const patches = todo.map((c) => {
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
  await note(failed.length ? "⚠️" : "✅", `Applied ${doneIds.length} of ${plural(patches.length, "selected change", "selected changes")}.`);
  for (const f of failed.slice(0, 10)) await note("❌", `Could not apply: ${f}`);

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
  if (doneIds.length) {
    await note(verified === doneIds.length ? "✅" : "❌", verified === doneIds.length
      ? `Double-checked: all ${doneIds.length} changes are in Airtable.`
      : `Only ${verified} of ${doneIds.length} changes could be confirmed in Airtable. Please check the All Classes table.`);
  }
  const notApplied = applicable.length - doneIds.length;
  const allGood = failed.length === 0 && verified === doneIds.length;
  await note(allGood ? "🎉" : "⚠️", allGood
    ? `All done: ${plural(doneIds.length, "change", "changes")} applied.${notApplied > 0 ? ` ${notApplied} other change${notApplied === 1 ? " was" : "s were"} left for later.` : ""}`
    : `Finished with things to look at: ${doneIds.length} applied, ${failed.length} failed.`);
  await patchRun({
    ...common, Created: doneIds.length, Failed: failed.length,
    Status: allGood ? "Completed" : "Completed with warnings",
    "Current step": allGood ? "Done" : "Done - see the notes below",
    "Finished at": new Date().toISOString(), Notes: notes.join("\n"),
  });
}
