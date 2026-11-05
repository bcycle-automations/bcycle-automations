// Fills HR Employees "MTEK Display Name" for active SPINCO instructors that lack one,
// from SPINCO MTEK /api/employees (employee_public_profiles.schedule_display_name).
// Why: SPINCO rating CSVs carry only the MTEK schedule display name; without it in HR
// the Feedback Manager automations (A003/A013) can't link ratings to the instructor,
// so they never show in Softr "My Ratings".
// Only fills EMPTY fields. Matches by email, falls back to full name. Skips (and reports)
// anything ambiguous or whose display name would collide with another instructor's.
// DRY_RUN defaults to true.

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const HR_BASE_ID = process.env.HR_BASE_ID || "appiwfeujJzUZPPBx";
const HR_EMPLOYEES_TABLE = process.env.HR_EMPLOYEES_TABLE_ID || "tbl0FzJ2s4Mk5jWIi";
const MTEK_BASE = (process.env.MTEK_SPINCO_BASE_URL || "https://spinco.marianatek.com").replace(/\/+$/, "");
const MTEK_TOKEN = (process.env.MTEK_SPINCO_API_TOKEN || "").trim();
const DRY_RUN = String(process.env.DRY_RUN ?? "true").toLowerCase() !== "false";

// HR field IDs
const F = {
  name: "fldfx5XqnufDFx3il",
  email: "fldtZABqLYuLRJef5",
  status: "fldIB88QIEbEafwf8",
  position: "fldZ3AFAp5jbzT028",
  company: "fldZLi1ZGQ779haCV",
  display: "fldejCNliS2edfhRi",
};

if (!AIRTABLE_TOKEN || !MTEK_TOKEN) {
  console.error("Missing AIRTABLE_TOKEN and/or MTEK_SPINCO_API_TOKEN");
  process.exit(1);
}

const norm = (s) => String(s || "").trim().toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
const flat = (v) => (Array.isArray(v) ? v.flatMap(flat) : v && typeof v === "object" ? [v.name ?? ""] : [v ?? ""]);

async function atFetch(url, opts = {}) {
  const r = await fetch(url, { ...opts, headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}`, "Content-Type": "application/json", ...(opts.headers || {}) } });
  if (!r.ok) throw new Error(`Airtable ${r.status}: ${await r.text()}`);
  return r.json();
}

async function loadHr() {
  const rows = [];
  let offset;
  do {
    const p = new URLSearchParams({ returnFieldsByFieldId: "true", pageSize: "100" });
    Object.values(F).forEach((f) => p.append("fields[]", f));
    if (offset) p.set("offset", offset);
    const j = await atFetch(`https://api.airtable.com/v0/${HR_BASE_ID}/${HR_EMPLOYEES_TABLE}?${p}`);
    rows.push(...j.records);
    offset = j.offset;
  } while (offset);
  return rows;
}

async function loadMtekEmployees() {
  const out = [];
  let url = `${MTEK_BASE}/api/employees?include=user,public_profile&page_size=100`;
  while (url) {
    const r = await fetch(url, { headers: { Authorization: `Bearer ${MTEK_TOKEN}`, Accept: "application/json" } });
    if (!r.ok) throw new Error(`MTEK ${r.status}: ${await r.text()}`);
    const j = await r.json();
    const inc = new Map((j.included || []).map((i) => [`${i.type}:${i.id}`, i]));
    for (const e of j.results || j.data || []) {
      const rel = e.relationships || {};
      const user = inc.get(`${rel.user?.data?.type}:${rel.user?.data?.id}`);
      const prof = inc.get(`${rel.public_profile?.data?.type}:${rel.public_profile?.data?.id}`);
      const ua = user?.attributes || e.user || {};
      const pa = prof?.attributes || e.public_profile || {};
      out.push({
        id: e.id,
        email: norm(ua.email || e.attributes?.email),
        fullName: norm(`${ua.first_name || ""} ${ua.last_name || ""}`),
        display: (pa.schedule_display_name || "").trim(),
      });
    }
    url = j.links?.next || j.next || null;
  }
  return out;
}

const hr = await loadHr();
const isActive = (r) => flat(r.fields[F.status]).some((x) => norm(x) === "active");
const isSpinco = (r) => flat(r.fields[F.company]).some((x) => norm(x) === "spinco");
const isInstructor = (r) => flat(r.fields[F.position]).some((x) => /instructor/i.test(x));
const candidates = hr.filter((r) => isActive(r) && isSpinco(r) && isInstructor(r));
const missing = candidates.filter((r) => !String(r.fields[F.display] || "").trim());
console.log(`HR: ${candidates.length} active SPINCO instructors, ${missing.length} missing MTEK Display Name`);
if (!missing.length) process.exit(0);

const mtek = await loadMtekEmployees();
console.log(`MTEK: ${mtek.length} employees`);
const byEmail = new Map(mtek.filter((m) => m.email).map((m) => [m.email, m]));
const byName = new Map();
for (const m of mtek) byName.set(m.fullName, [...(byName.get(m.fullName) || []), m]);

const taken = new Map(); // display name -> count among HR candidates (existing + planned)
candidates.forEach((r) => { const d = norm(r.fields[F.display]); if (d) taken.set(d, (taken.get(d) || 0) + 1); });

const updates = [];
const problems = [];
for (const r of missing) {
  const nm = r.fields[F.name];
  const email = norm(r.fields[F.email]);
  let m = byEmail.get(email);
  if (!m) { const l = byName.get(norm(nm)) || []; if (l.length === 1) m = l[0]; }
  if (!m) { problems.push(`${nm}: no MTEK employee found (email ${email || "none"})`); continue; }
  if (!m.display) { problems.push(`${nm}: MTEK profile has no schedule display name`); continue; }
  const d = norm(m.display);
  if (taken.get(d)) { problems.push(`${nm}: display name "${m.display}" already used by another instructor — set manually`); continue; }
  taken.set(d, 1);
  updates.push({ id: r.id, fields: { [F.display]: m.display }, _label: `${nm} -> ${m.display}` });
}

updates.forEach((u) => console.log(`${DRY_RUN ? "[dry-run] would set" : "setting"} ${u._label}`));
problems.forEach((p) => console.log(`::warning::${p}`));

if (!DRY_RUN) {
  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10).map(({ id, fields }) => ({ id, fields }));
    await atFetch(`https://api.airtable.com/v0/${HR_BASE_ID}/${HR_EMPLOYEES_TABLE}`, {
      method: "PATCH",
      body: JSON.stringify({ records: chunk, typecast: false }),
    });
  }
}
console.log(`Done. ${updates.length} ${DRY_RUN ? "proposed" : "updated"}, ${problems.length} need manual attention.`);
