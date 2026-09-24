// scripts/dry-run-birthday-eligibility.mjs
//
// Read-only: reconstructs a past week's birthday-matching window, runs the
// real findBirthdayMatches + getMembershipSegment logic against it (same
// functions the live automation uses — no duplicated/drifting copy), and
// reports counts under multiple "last attended within N years" thresholds
// side by side (Jess asked to compare 3 years vs the current 5 years,
// 2026-09-24). Never creates a cart, never checks out, never sends an
// email, never writes to Airtable.
//
// Also classifies each eligible person by whether they currently hold ANY
// active/frozen membership (not just unlimited) vs. none — a proxy for
// Bijan's revenue question: membership holders pay a flat fee regardless
// of this one extra class, so a free credit doesn't cost anything
// incremental; someone with no active membership (credit-pack/drop-in
// only) would likely have paid for that class, so it's closer to a real
// cost. This is a proxy, not a certainty — flagged clearly in the output.
//
// ANCHOR_DATE (YYYY-MM-DD, defaults to 2026-09-21 — the most recent real
// scheduled Monday run at the time this was written) is the first day of
// the 7-day window to re-check, matching getTargetBirthdayDates()' own
// window shape (anchor through anchor+6).
//
// YEARS_THRESHOLDS (comma-separated, defaults to "3,5") — which "attended
// within N years" cutoffs to compare.

import {
  findBirthdayMatches,
  getMembershipSegment,
  getLastCheckInDate,
} from "./birthday-credit-email.mjs";
import { fetchAllPages } from "./lib/mtek.mjs";

const MTEK_BASE_URL = "https://bcycle.marianatek.com/api";

function getMTechHeaders() {
  return {
    Authorization: `Bearer ${process.env.MTEK_API_TOKEN}`,
    Accept: "application/vnd.api+json",
  };
}

function addCalendarDays(dateString, daysToAdd) {
  const [year, month, day] = dateString.split("-").map(Number);
  const result = new Date(Date.UTC(year, month - 1, day + daysToAdd, 12, 0, 0));
  return result.toISOString().slice(0, 10);
}

function addCalendarMonths(dateString, monthsToAdd) {
  const [year, month, day] = dateString.split("-").map(Number);
  const result = new Date(Date.UTC(year, month - 1 + monthsToAdd, day, 12, 0, 0));
  return result.toISOString().slice(0, 10);
}

// "Never attended" eligibility window is unchanged (4 weeks to 6 months) —
// this dry run only compares the "attended before" years-threshold, per
// what Jess actually asked to see.
function isEligibleNeverAttended(match, today) {
  if (!match.dateJoined) return false;
  const joinedDate = match.dateJoined.slice(0, 10);
  const sixMonthsAgo = addCalendarMonths(today, -6);
  const fourWeeksAgo = addCalendarDays(today, -28);
  return joinedDate >= sixMonthsAgo && joinedDate <= fourWeeksAgo;
}

// Any active/frozen membership_instance at all, regardless of whether it's
// unlimited — broader than getMembershipSegment's unlimited-only check.
async function hasAnyActiveMembership(userId) {
  const instances = await fetchAllPages(
    `${MTEK_BASE_URL}/membership_instances/`,
    { user: userId, page_size: "100" },
    getMTechHeaders()
  );

  return instances.some((instance) =>
    ["active", "frozen"].includes(instance.attributes?.status)
  );
}

async function main() {
  const anchorDate = process.env.ANCHOR_DATE?.trim() || "2026-09-21";
  const targetDates = Array.from({ length: 7 }, (_, offset) => addCalendarDays(anchorDate, offset));
  const yearsThresholds = (process.env.YEARS_THRESHOLDS?.trim() || "3,5")
    .split(",")
    .map((s) => Number(s.trim()));

  console.log("==========================================");
  console.log("DRY RUN — no emails sent, no credits granted, no Airtable writes");
  console.log(`Window: ${targetDates[0].slice(5)} through ${targetDates[6].slice(5)}`);
  console.log(`Comparing "attended within N years" thresholds: ${yearsThresholds.join(", ")}`);
  console.log("==========================================");

  const matches = await findBirthdayMatches(targetDates);
  console.log(`Total birthday matches found: ${matches.length}`);

  const today = targetDates[0];

  // Per-match: has this person ever attended, and if so, when was their
  // last check-in (fetched once, reused across every threshold).
  const enriched = [];
  for (const match of matches) {
    if (match.completedClassCount === 0) {
      enriched.push({ ...match, lastCheckIn: null });
    } else {
      const lastCheckIn = await getLastCheckInDate(match.id);
      enriched.push({ ...match, lastCheckIn });
    }
  }

  const results = {};

  for (const years of yearsThresholds) {
    const cutoff = addCalendarMonths(today, -years * 12);
    const eligible = [];

    for (const match of enriched) {
      const isEligible =
        match.completedClassCount === 0
          ? isEligibleNeverAttended(match, today)
          : Boolean(match.lastCheckIn) && match.lastCheckIn.slice(0, 10) >= cutoff;

      if (isEligible) eligible.push(match);
    }

    // Classify eligible people by membership status + segment (extra MTEK
    // calls only for the eligible set, not the whole 1000+ match pool).
    let unlimitedCount = 0;
    let hasOtherMembershipCount = 0;
    let noMembershipCount = 0;
    let expectedVisitsThatWeek = 0; // see note below

    for (const match of eligible) {
      const segment = await getMembershipSegment(match.id);
      if (segment === "unlimited") {
        unlimitedCount += 1;
        continue;
      }
      const hasMembership = await hasAnyActiveMembership(match.id);
      if (hasMembership) {
        hasOtherMembershipCount += 1;
        continue;
      }

      noMembershipCount += 1;

      // Not everyone with no active membership would have visited that
      // specific week anyway — assuming 100% of them is a worst-case, not a
      // realistic one (per Jonathan, 2026-09-24). Estimate each person's
      // probability of visiting in any given week from their own lifetime
      // visit rate (completedClassCount / weeks since date_joined, both
      // already fetched with the birthday match — no extra API calls),
      // capped at 1.0 since a rate above "once a week" doesn't raise the
      // odds of visiting in a *specific* week beyond certain. This is a
      // lifetime average, not a recent-activity-weighted rate — a real
      // simplification, flagged clearly in the output.
      if (match.dateJoined) {
        const weeksSinceJoined = Math.max(
          (new Date(today).getTime() - new Date(match.dateJoined).getTime()) /
            (7 * 24 * 60 * 60 * 1000),
          1
        );
        const visitProbability = Math.min(match.completedClassCount / weeksSinceJoined, 1);
        expectedVisitsThatWeek += visitProbability;
      }
    }

    results[years] = {
      eligibleCount: eligible.length,
      unlimitedCount,
      hasOtherMembershipCount,
      noMembershipCount,
      expectedVisitsThatWeek,
    };
  }

  const AVG_REVENUE_PER_CLASS = 61.21; // BigQuery: avg per single-class-credit-equivalent, last 180 days
  const MEDIAN_REVENUE_PER_CLASS = 31.04; // same query, median

  console.log("==========================================");
  console.log("RESULTS BY THRESHOLD");
  for (const years of yearsThresholds) {
    const r = results[years];
    const roundedExpected = Math.round(r.expectedVisitsThatWeek * 10) / 10;
    console.log(`--- Attended within ${years} year(s) ---`);
    console.log(`  Total eligible: ${r.eligibleCount}`);
    console.log(`  - Unlimited members (guest pass, no incremental cost): ${r.unlimitedCount}`);
    console.log(
      `  - Other active membership (flat fee, no incremental cost): ${r.hasOtherMembershipCount}`
    );
    console.log(
      `  - No active membership (credit-pack/drop-in — closest to a real cost): ${r.noMembershipCount}`
    );
    console.log(
      `    Worst case (100% would've come that week): $${(r.noMembershipCount * AVG_REVENUE_PER_CLASS).toFixed(2)} (avg) / $${(r.noMembershipCount * MEDIAN_REVENUE_PER_CLASS).toFixed(2)} (median)`
    );
    console.log(
      `    Realistic (weighted by each person's own lifetime visit rate): ~${roundedExpected} expected visits that week ` +
        `-> $${(r.expectedVisitsThatWeek * AVG_REVENUE_PER_CLASS).toFixed(2)} (avg) / $${(r.expectedVisitsThatWeek * MEDIAN_REVENUE_PER_CLASS).toFixed(2)} (median)`
    );
  }
  console.log("==========================================");
}

main().catch((error) => {
  console.error("Dry run failed:");
  console.error(error);
  process.exitCode = 1;
});
