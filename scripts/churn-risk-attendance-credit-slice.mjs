// Prototype / validation run for the churn-risk project. Read-only: pulls the
// existing "customers-details" report (id 336) and scores two categories —
// Attendance Decay and Credit Exhaustion — using fields MTEK already
// computes (Recency/Frequency Score, Retention Segment, Membership Status,
// Total Remaining Credits). Does not write anywhere; prints counts + samples
// so the categories can be sanity-checked before wiring into a real table.

import { fetchMtekReport } from "./lib/mtek-report.mjs";

const MTEK_BASE_URL = process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com";
const MTEK_API_TOKEN = process.env.MTEK_API_TOKEN;

function parseStatuses(raw) {
  if (!raw) return [];
  return [...new Set(String(raw).split(",").map((s) => s.trim()).filter(Boolean))];
}

function col(headers, name) {
  const i = headers.indexOf(name);
  if (i === -1) throw new Error(`Column not found: ${name}`);
  return i;
}

async function main() {
  if (!MTEK_API_TOKEN) throw new Error("MTEK_API_TOKEN is required");

  const { headers, rows } = await fetchMtekReport({
    baseUrl: MTEK_BASE_URL,
    token: MTEK_API_TOKEN,
    reportId: "336",
    slug: "customers-details",
    pageSize: 500,
  });

  const c = {
    customerId: col(headers, "Customer ID"),
    email: col(headers, "Email"),
    name: col(headers, "Full Name"),
    membershipStatus: col(headers, "Membership Status"),
    recency: col(headers, "Recency Score"),
    frequency: col(headers, "Frequency Score"),
    retention: col(headers, "Retention Segment"),
    lastCheckIn: col(headers, "Last Check-In"),
    remainingCredits: col(headers, "Total Remaining Credits"),
  };

  const attendanceDecay = { critical: [], watch: [] };
  let attendanceOk = 0;
  let excludedFrozen = 0;
  let excludedPaymentFailure = 0;

  const creditExhaustion = { critical: [], watch: [] };
  let noMembershipCount = 0;

  for (const row of rows) {
    const statuses = parseStatuses(row[c.membershipStatus]);
    const hasActive = statuses.includes("active");
    const isFrozen = statuses.includes("frozen");
    const isPaymentFailure = statuses.includes("payment_failure");
    const recency = Number(row[c.recency]);
    const frequency = Number(row[c.frequency]);
    const retention = row[c.retention];
    const credits = row[c.remainingCredits] === "" || row[c.remainingCredits] == null
      ? null
      : Number(row[c.remainingCredits]);

    const base = () => ({
      customerId: row[c.customerId],
      email: row[c.email],
      name: row[c.name],
      recencyScore: recency,
      retentionSegment: retention,
      lastCheckIn: row[c.lastCheckIn],
    });

    if (hasActive) {
      if (isFrozen) {
        excludedFrozen++;
        continue;
      }
      if (isPaymentFailure) {
        excludedPaymentFailure++; // handled by the existing Payment Failures flow
        continue;
      }
      if (recency <= 2 && (retention === "Lapsing" || retention === "Lost")) {
        attendanceDecay.critical.push(base());
      } else if (recency === 3) {
        attendanceDecay.watch.push(base());
      } else {
        attendanceOk++;
      }
      continue;
    }

    if (!statuses.length) {
      noMembershipCount++;
      // Only worth flagging if they had a real usage pattern before going quiet
      if (frequency >= 2) {
        if (credits === 0 && (retention === "Lapsing" || retention === "Lost")) {
          creditExhaustion.critical.push({ ...base(), remainingCredits: credits, frequencyScore: frequency });
        } else if (credits !== null && credits > 0 && credits <= 2 && retention === "Lapsing") {
          creditExhaustion.watch.push({ ...base(), remainingCredits: credits, frequencyScore: frequency });
        }
      }
    }
  }

  console.log("=== ATTENDANCE DECAY (active membership, not frozen, not payment_failure) ===");
  console.log("critical:", attendanceDecay.critical.length);
  console.log("watch:", attendanceDecay.watch.length);
  console.log("ok:", attendanceOk);
  console.log("excluded (frozen):", excludedFrozen);
  console.log("excluded (payment_failure — own flow):", excludedPaymentFailure);
  console.log("sample critical (first 10):", JSON.stringify(attendanceDecay.critical.slice(0, 10), null, 2));
  console.log("sample watch (first 5):", JSON.stringify(attendanceDecay.watch.slice(0, 5), null, 2));

  console.log("\n=== CREDIT EXHAUSTION (no current membership, credit-based history) ===");
  console.log("no-membership pool:", noMembershipCount);
  console.log("critical:", creditExhaustion.critical.length);
  console.log("watch:", creditExhaustion.watch.length);
  console.log("sample critical (first 10):", JSON.stringify(creditExhaustion.critical.slice(0, 10), null, 2));
  console.log("sample watch (first 5):", JSON.stringify(creditExhaustion.watch.slice(0, 5), null, 2));
}

main();
