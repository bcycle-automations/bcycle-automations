#!/usr/bin/env node
// Read-only: lists SPINCO MTEK locations (id + name) so the Studios table can be filled in.
const base = (process.env.MTEK_SPINCO_BASE_URL || 'https://spinco.marianatek.com').replace(/\/+$/, '');
const token = process.env.MTEK_SPINCO_API_TOKEN;
if (!token) throw new Error('Missing MTEK_SPINCO_API_TOKEN');
const filter = (process.env.NAME_FILTER || 'summerhill|montr|downtown').toLowerCase();
const re = new RegExp(filter);
let page = 1, pages = 1;
const rows = [];
while (page <= pages) {
  const res = await fetch(`${base}/api/locations?page=${page}&page_size=100`, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/vnd.api+json' },
  });
  if (!res.ok) throw new Error(`MTEK ${res.status}`);
  const j = await res.json();
  pages = Number(j?.meta?.pagination?.pages) || 1;
  for (const d of j.data || []) rows.push({ id: d.id, name: d.attributes?.name || d.attributes?.display_name || '' });
  page += 1;
}
console.log(`${rows.length} locations total`);
for (const r of rows.filter((r) => re.test(r.name.toLowerCase()))) console.log(`${r.id}\t${r.name}`);
