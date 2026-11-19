#!/usr/bin/env node
// SPINCO MTEK time-punch test helper. MODE=inspect | create | delete.
// Logs are public: only ids, counts and attribute NAMES are printed, never names/emails.
const base = (process.env.MTEK_SPINCO_BASE_URL || 'https://spinco.marianatek.com').replace(/\/+$/, '');
const token = process.env.MTEK_SPINCO_API_TOKEN;
if (!token) throw new Error('Missing MTEK_SPINCO_API_TOKEN');
const H = { Authorization: `Bearer ${token}`, Accept: 'application/vnd.api+json', 'Content-Type': 'application/vnd.api+json' };
const mode = process.env.MODE || 'inspect';
const loc = process.env.LOCATION_ID || '48785';

async function api(method, path, body) {
  const res = await fetch(`${base}${path}`, { method, headers: H, body: body ? JSON.stringify(body) : undefined });
  const text = await res.text();
  let json = null; try { json = text ? JSON.parse(text) : null; } catch {}
  return { status: res.status, json, text };
}

if (mode === 'inspect') {
  const r = await api('GET', `/api/time_clock_shifts?location=${loc}&page_size=1&ordering=-start_datetime&include=employee,shift_type`);
  const d = r.json?.data?.[0];
  console.log('GET status', r.status);
  console.log('attribute keys:', Object.keys(d?.attributes || {}).join(', '));
  console.log('relationship keys:', Object.keys(d?.relationships || {}).join(', '));
  console.log('type:', d?.type, '| employee id:', d?.relationships?.employee?.data?.id, '| shift_type id:', d?.relationships?.shift_type?.data?.id);
  const st = await api('GET', '/api/shift_types?page_size=100');
  console.log('shift types:', (st.json?.data || []).map((x) => `${x.id}=${x.attributes?.name}`).join(' | '));
  const opt = await api('OPTIONS', '/api/time_clock_shifts');
  console.log('OPTIONS status', opt.status, 'allow methods:', JSON.stringify(opt.json?.data?.actions ? Object.keys(opt.json.data.actions) : opt.text.slice(0, 200)));
}
