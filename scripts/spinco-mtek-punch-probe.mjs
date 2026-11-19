#!/usr/bin/env node
// Read-only probe: how many time_clock_shifts does SPINCO's MTEK hold? Counts only (public logs).
const base = (process.env.MTEK_SPINCO_BASE_URL || 'https://spinco.marianatek.com').replace(/\/+$/, '');
const token = process.env.MTEK_SPINCO_API_TOKEN;
if (!token) throw new Error('Missing MTEK_SPINCO_API_TOKEN');
async function count(label, params) {
  const url = new URL('/api/time_clock_shifts', base);
  Object.entries({ page_size: 1, ...params }).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/vnd.api+json' } });
  const text = await res.text();
  let n = '?';
  try { n = JSON.parse(text)?.meta?.pagination?.count; } catch {}
  console.log(`${label}: HTTP ${res.status}, count=${n}`);
}
await count('all time, no filter', {});
await count('Nov 2026, no location', { min_start_datetime: '2026-11-01T00:00:00Z', max_start_datetime: '2026-11-19T00:00:00Z' });
for (const [name, id] of [['Toronto Downtown', 48783], ['Toronto Summerhill', 48784], ['Montreal', 48785]]) {
  await count(`${name} (any time)`, { location: id });
}

// Latest punch date per studio (date only).
for (const [name, id] of [['Toronto Downtown', 48783], ['Toronto Summerhill', 48784], ['Montreal', 48785]]) {
  const url = new URL('/api/time_clock_shifts', base);
  url.searchParams.set('location', id); url.searchParams.set('page_size', 1); url.searchParams.set('ordering', '-start_datetime');
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/vnd.api+json' } });
  let d = '?'; try { d = String((await res.json())?.data?.[0]?.attributes?.start_datetime || 'none').slice(0, 10); } catch {}
  console.log(`${name}: latest punch ${d}`);
}
