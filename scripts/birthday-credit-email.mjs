// scripts/birthday-credit-email.mjs
//
// Weekly automation: finds clients whose birthday falls within the next 7
// days, grants them a free class credit in MTEK, and emails them. Two
// segments, decided by whether the client has an active-or-frozen unlimited
// membership (Emilie created both products on 2026-07-28):
//   - Unlimited (active/frozen): "Passe invité d'anniversaire" (shareable,
//     since unlimited members can bring a guest) — product 16603 / variant
//     16604 / credit id 2323 ("Class Packages").
//   - Everyone else: "Pass d'anniversaire" (not shareable) — product 16601 /
//     variant 16602 / credit id 2324 ("Passe Flex").
// Both are $0 variants configured in MTEK with a 2-week relative expiration.
//
// Eligibility (added 2026-09-23): a birthday match alone isn't enough — see
// isEligibleForBirthdayCredit(). Must have checked into a class within the
// last 5 years, or, if they've never completed one, have created their
// profile within the last 6 months. This isn't treated as marketing (opt-in
// status is intentionally not checked): it's a transactional free-class
// grant to an engaged or recent account, per Jonathan.
//
// MTEK's /users/ endpoint only supports an *exact* birth_date=YYYY-MM-DD
// filter (no month/day-only filter exists — confirmed by testing
// birth_date__month/__day/__contains/__endswith, all silently ignored), so
// finding "birthday in 7 days" means looping over every plausible birth year
// and querying each one exactly. Confirmed via Mariana Tek support
// (Nov/Dec 2025 emails) that the real rate limit is a 1300-request bucket
// refilling 650/sec — ~101 sequential requests per run is nowhere near that,
// so no need to split the birth-year range across multiple days.
//
// LIVE_MODE went live 2026-09-22, after Jess (marketing) approved the
// content/format (Option 1: English first — free-class terms dropped from
// the English copy and starred — straight into French with no gap, French
// keeps the terms + footer, matching "*" on the French terms sentence).
// When true, every real match found each run is processed (not just the
// first), and both the email and the credit grant go to the real customer
// — see recipientEmail / creditTargetUserId below. If this ever needs to go
// back to a safe dry-run, flip LIVE_MODE back to false: every match found
// still gets logged to the console either way, so nothing about matching
// depends on this flag — only where the email/credit actually land does.
//
// The credit is granted via a real MTEK checkout (POST /carts/ -> POST
// /cart_lines/ -> POST /checkouts/), not a direct POST /credit_transactions/.
// The direct approach works but MTEK always names the resulting credit
// "Complimentary <credit type>" (e.g. "Complimentary Passe Flex"), ignoring
// whatever `credit_name` is sent — confirmed live. Going through checkout
// with the real $0 product/variant produces a credit_transaction whose
// credit_name is the actual product name ("Pass d'anniversaire" /
// "Passe invité d'anniversaire"), since only checkout ties the transaction
// to the product rather than just the underlying credit type. Also
// confirmed: creating a cart for a user can silently merge in any
// pre-existing OPEN cart they already have (found a real unrelated $31 item
// this way during testing) — grantBirthdayCreditViaCheckout() refuses to
// check out unless the cart total is exactly $0 after adding our line.
//
// fulfillment_partner is per-studio, not universal — in live mode each
// customer's own home_location (from MTEK) is mapped to their studio's
// partner id via LOCATION_TO_PARTNER, falling back to Vieux-Port with a
// logged warning if that's ever missing/unrecognized, rather than failing
// their credit outright.

import { fetchJsonWithRateLimit, fetchAllPages } from "./lib/mtek.mjs";

const LIVE_MODE = true;

const MTEK_BASE_URL = "https://bcycle.marianatek.com/api";
const GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0";
const AIRTABLE_BASE_URL = "https://api.airtable.com/v0";
const EMAIL_LOGS_TABLE_ID = "tbloAdBJHSygcndbA"; // "Email Logs"
const TIME_ZONE = "America/Toronto";

const MIN_BIRTH_YEAR = 1920;
const MAX_BIRTH_YEAR = 2020;

const UNLIMITED_MEMBERSHIP_PATTERN = /unlimited|illimit/i;
const ACTIVE_MEMBERSHIP_STATUSES = new Set(["active", "frozen"]);

// fulfillment_partner is per-studio, not universal (confirmed live):
// VieuxPort=41364, CentreVille=41362, Rockland=41363, Westmount=41365.
// Test account (jonathan@bcyclespin.com) is home-studio Vieux-Port, so this
// is what non-live/manual test runs use.
const TEST_PARTNER_ID = "41364";

// Maps each MTEK location id (relationships.home_location on /users/) to
// its fulfillment_partner id, confirmed live from real orders at each
// studio. Real customers do have home_location populated (confirmed on
// several real matches during testing) — used in LIVE_MODE to route the
// checkout to the customer's own studio instead of always Vieux-Port.
const LOCATION_TO_PARTNER = {
  "48719": "41364", // Vieux-Port
  "48717": "41362", // Centre-Ville
  "48718": "41363", // Rockland
  "48720": "41365", // Westmount
};

// child_products ids are the same underlying ids as the product_variants
// ids (16602/16604) — just referenced under a different type name when
// building a cart line.
const PRODUCTS = {
  unlimited: {
    productId: "16603",
    variantId: "16604",
    creditId: "2323",
    name: "Passe invité d'anniversaire",
  },
  standard: {
    productId: "16601",
    variantId: "16602",
    creditId: "2324",
    name: "Pass d'anniversaire",
  },
};

const REQUIRED_ENVIRONMENT_VARIABLES = [
  "MTEK_API_TOKEN",
  "M365_CLIENT_ID",
  "M365_CLIENT_SECRET",
  "M365_TENANT_ID",
  "M365_SENDER_UPN",
  "AIRTABLE_TOKEN",
  "CUSTOMER_BASE_ID",
];

async function main() {
  validateEnvironmentVariables();

  const testEmail = cleanString(process.env.TEST_EMAIL) || "jonathan@bcyclespin.com";
  const manuallySelectedDate = cleanString(process.env.TARGET_BIRTHDAY_DATE);

  // TARGET_BIRTHDAY_DATE overrides to a single specific date, for testing
  // against a known match. Otherwise, use the real 7-day window.
  const targetBirthdayDates = manuallySelectedDate
    ? [manuallySelectedDate]
    : getTargetBirthdayDates();

  console.log("==========================================");
  console.log("Birthday credit email automation started");
  console.log(`Mode: ${LIVE_MODE ? "LIVE" : "TEST"}`);
  console.log(
    `Target birthday window (month/day): ${targetBirthdayDates
      .map((d) => d.slice(5))
      .join(", ")}`
  );
  if (!LIVE_MODE) {
    console.log(
      `TEST MODE: emails + credit grants are redirected to ${testEmail}'s account. ` +
        "No real customer will be emailed or credited."
    );
  }
  console.log("==========================================");

  const matches = await findBirthdayMatches(targetBirthdayDates);

  console.log(`Found ${matches.length} client(s) with a birthday in the next 7 days.`);
  matches.forEach((match, index) => {
    console.log(`  ${index + 1}. ${match.email || "(no email)"} (user ${match.id})`);
  });

  if (matches.length === 0) {
    console.log("No qualifying birthdays found for this run.");
    return;
  }

  // Birthday matching alone is too broad (any non-archived account with an
  // email) — filter to people who've actually engaged: checked into a class
  // within the last 5 years, or, if they've never completed one, created
  // their profile within the last 6 months.
  const eligibleMatches = [];
  for (const match of matches) {
    if (await isEligibleForBirthdayCredit(match)) {
      eligibleMatches.push(match);
    }
  }

  console.log(
    `${eligibleMatches.length} of ${matches.length} match(es) are eligible ` +
      "(active within 5 years, or profile created within 6 months if never attended)."
  );

  if (eligibleMatches.length === 0) {
    console.log("No eligible birthdays found for this run.");
    return;
  }

  // Live: process every eligible match. Non-live (manual/dry-run only): just
  // the first, mirroring the isTestMode ? transactions.slice(0, 1) : ...
  // precedent in credit-expiry-email.mjs.
  const matchesToProcess = LIVE_MODE ? eligibleMatches : eligibleMatches.slice(0, 1);

  if (!LIVE_MODE && eligibleMatches.length > 1) {
    console.log(
      `TEST MODE: only processing the first match (${matchesToProcess[0].email}); ` +
        `${eligibleMatches.length - 1} other eligible match(es) found this run were skipped.`
    );
  }

  const microsoftAccessToken = await getMicrosoftAccessToken();

  // In test mode, the credit is granted to this account instead of the real
  // customer's — looked up live so it always matches whatever TEST_EMAIL is
  // configured to.
  const testUser = LIVE_MODE ? null : await findUserByEmail(testEmail);

  if (!LIVE_MODE && !testUser) {
    throw new Error(`Could not find a MTEK user for TEST_EMAIL=${testEmail}.`);
  }

  let successCount = 0;
  let failureCount = 0;

  for (const match of matchesToProcess) {
    try {
      if (!match.email && LIVE_MODE) {
        throw new Error(`Client ${match.id} does not have an email address.`);
      }

      const segment = await getMembershipSegment(match.id);
      const product = PRODUCTS[segment];

      const recipientEmail = LIVE_MODE ? match.email : testEmail;
      const creditTargetUserId = LIVE_MODE ? match.id : testUser.id;

      // Route the checkout to the real customer's own studio in live mode
      // (fulfillment_partner is per-studio — confirmed live). Falls back to
      // Vieux-Port with a warning if home_location is missing/unrecognized,
      // rather than failing that customer's credit entirely.
      let partnerId = TEST_PARTNER_ID;
      if (LIVE_MODE) {
        partnerId = LOCATION_TO_PARTNER[match.homeLocationId];
        if (!partnerId) {
          console.warn(
            `  Warning: no known partner for home_location=${match.homeLocationId} ` +
              `(user ${match.id}) — falling back to Vieux-Port.`
          );
          partnerId = TEST_PARTNER_ID;
        }
      }

      console.log(
        `Processing ${match.email} (segment: ${segment}, product: ${product.name})` +
          (LIVE_MODE ? "" : ` — redirecting to ${recipientEmail} / MTEK user ${creditTargetUserId}`)
      );

      const order = await grantBirthdayCreditViaCheckout({
        userId: creditTargetUserId,
        product,
        partnerId,
      });

      console.log(`  Granted via order ${order.attributes.number} (${order.id}).`);

      const emailHtml = buildEmailHtml({
        firstName: match.firstName || "there",
        segment,
        product,
      });

      const subjectPrefix = LIVE_MODE ? "" : "[TEST] ";
      const subject = `${subjectPrefix}Joyeux anniversaire / Happy Birthday — ${product.name}`;

      await sendMicrosoftEmail({
        accessToken: microsoftAccessToken,
        recipientEmail,
        subject,
        html: emailHtml,
      });

      try {
        // "Birthday email" is a pre-existing choice on the Email Logs Type
        // field (singleSelect) — the Airtable token can't create new choice
        // options, so test vs. live isn't distinguished here (it is in the
        // MTEK credit_transaction `note` field and in the workflow logs).
        await logEmailToAirtable({
          email: match.email,
          type: "Birthday email",
        });
      } catch (logError) {
        console.error(
          `Warning: email/credit succeeded but failed to log to Airtable for ${match.email}:`,
          logError.message
        );
      }

      successCount += 1;
      console.log(`Done: ${match.email} (${segment}).`);
    } catch (error) {
      failureCount += 1;
      console.error(`Failed to process client ${match.id}:`, error.message);
    }
  }

  console.log("==========================================");
  console.log("Automation completed");
  console.log(`Successful: ${successCount}`);
  console.log(`Failed: ${failureCount}`);
  console.log("==========================================");

  if (failureCount > 0) {
    process.exitCode = 1;
  }
}

function validateEnvironmentVariables() {
  const missingVariables = REQUIRED_ENVIRONMENT_VARIABLES.filter(
    (variableName) => !cleanString(process.env[variableName])
  );

  if (missingVariables.length > 0) {
    throw new Error(`Missing required environment variables: ${missingVariables.join(", ")}`);
  }
}

function cleanString(value) {
  return typeof value === "string" ? value.trim() : "";
}

// Runs weekly (Monday cron), so "birthday in the next 7 days" has to mean a
// 7-day window (today through today+6), not a single exact date — matching
// only "exactly 7 days from today" would only ever catch people born on a
// Monday, since that's the only weekday that lands exactly 7 days after a
// Monday run. This window gives 0-6 days' notice depending on which day of
// the week the birthday falls, with no gaps or overlaps between weekly runs.
function getTargetBirthdayDates() {
  const todayInToronto = getDateInTimeZone(new Date(), TIME_ZONE);
  const dates = [];

  for (let offset = 0; offset <= 6; offset++) {
    dates.push(addCalendarDays(todayInToronto, offset));
  }

  return dates;
}

function getDateInTimeZone(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });

  const parts = formatter.formatToParts(date);
  const values = Object.fromEntries(
    parts.filter((part) => part.type !== "literal").map((part) => [part.type, part.value])
  );

  return `${values.year}-${values.month}-${values.day}`;
}

function addCalendarDays(dateString, daysToAdd) {
  const [year, month, day] = dateString.split("-").map(Number);
  const result = new Date(Date.UTC(year, month - 1, day + daysToAdd, 12, 0, 0));
  return result.toISOString().slice(0, 10);
}

// Negative monthsToAdd moves back in time (e.g. -60 for "5 years ago").
// Date.UTC naturally rolls the year when month goes out of 0-11 range.
function addCalendarMonths(dateString, monthsToAdd) {
  const [year, month, day] = dateString.split("-").map(Number);
  const result = new Date(Date.UTC(year, month - 1 + monthsToAdd, day, 12, 0, 0));
  return result.toISOString().slice(0, 10);
}

function getMTechHeaders() {
  return {
    Authorization: `Bearer ${process.env.MTEK_API_TOKEN}`,
    Accept: "application/vnd.api+json",
  };
}

// For every date in the window, loops every birth year in range querying
// the exact birth_date=YYYY-MM-DD match, and collects every real
// (non-archived, has-email) match. Dedupes by user id in case someone
// somehow matches more than one date in the window (shouldn't happen since
// each date has a distinct month/day, but cheap insurance).
async function findBirthdayMatches(targetDates) {
  const matches = [];
  const seenUserIds = new Set();

  for (const targetDate of targetDates) {
    const monthDay = targetDate.slice(5); // "MM-DD"

    for (let year = MIN_BIRTH_YEAR; year <= MAX_BIRTH_YEAR; year++) {
      const birthDate = `${year}-${monthDay}`;

      const users = await fetchAllPages(
        `${MTEK_BASE_URL}/users/`,
        { birth_date: birthDate, page_size: "100" },
        getMTechHeaders()
      );

      for (const user of users) {
        const attrs = user.attributes || {};

        if (attrs.archived_at) continue;
        if (!attrs.email) continue;
        if (seenUserIds.has(user.id)) continue;

        seenUserIds.add(user.id);
        matches.push({
          id: user.id,
          email: attrs.email,
          firstName: attrs.first_name,
          lastName: attrs.last_name,
          birthDate: attrs.birth_date,
          upcomingBirthdayDate: targetDate,
          homeLocationId: user.relationships?.home_location?.data?.id || null,
          completedClassCount: attrs.completed_class_count || 0,
          dateJoined: attrs.date_joined || null,
        });
      }
    }
  }

  return matches;
}

// Eligibility rule (added 2026-09-23, per Jonathan): birthday matching alone
// is too broad — it would include anyone with a non-archived account,
// regardless of engagement. Two cases:
//   - Has completed at least one class: must have actually checked into a
//     class within the last 5 years.
//   - Never completed a class (completedClassCount === 0): their profile
//     must have been created within the last 6 months — otherwise it's a
//     years-old account that signed up and never came back.
// "Checked in" uses reservations status=check in (confirmed live: count
// under this filter exactly matches the user's completed_class_count), not
// just any reservation (which would also include cancellations/no-shows).
async function isEligibleForBirthdayCredit(match) {
  const todayInToronto = getDateInTimeZone(new Date(), TIME_ZONE);

  if (match.completedClassCount === 0) {
    const sixMonthsAgo = addCalendarMonths(todayInToronto, -6);
    return Boolean(match.dateJoined) && match.dateJoined.slice(0, 10) >= sixMonthsAgo;
  }

  const fiveYearsAgo = addCalendarMonths(todayInToronto, -60);
  const lastCheckIn = await getLastCheckInDate(match.id);
  return Boolean(lastCheckIn) && lastCheckIn.slice(0, 10) >= fiveYearsAgo;
}

// Fetches a small page (not just 1) of the user's most recent check-ins and
// takes the true max client-side, as cheap insurance against the API's
// default ordering ever being anything other than newest-first — confirmed
// live that it is (verified page_size=1's result against the true max
// across a 393-reservation history), but this costs almost nothing extra.
async function getLastCheckInDate(userId) {
  const url = new URL(`${MTEK_BASE_URL}/reservations/`);
  url.searchParams.set("user", userId);
  url.searchParams.set("status", "check in");
  url.searchParams.set("page_size", "5");

  const body = await fetchJsonWithRateLimit(url, { headers: getMTechHeaders() });
  const checkInDates = (body?.data || [])
    .map((reservation) => reservation.attributes?.check_in_date)
    .filter(Boolean);

  if (checkInDates.length === 0) return null;

  return checkInDates.sort().reverse()[0];
}

async function findUserByEmail(email) {
  const url = new URL(`${MTEK_BASE_URL}/users/`);
  url.searchParams.set("email", email);

  const body = await fetchJsonWithRateLimit(url, { headers: getMTechHeaders() });
  const users = body?.data || [];

  if (users.length === 0) return null;

  return { id: users[0].id, email: users[0].attributes?.email };
}

// "Has unlimited" = any membership_instance with status active/frozen whose
// membership_name matches /unlimited|illimit/i. There's no structured
// boolean for this on the /memberships/ catalog, so text matching against
// the instance's own membership_name is the only reliable signal (same
// convention as the bike-rental `LIKE '%1 bike%'` check used elsewhere for
// b.cycle's BigQuery data).
async function getMembershipSegment(userId) {
  const instances = await fetchAllPages(
    `${MTEK_BASE_URL}/membership_instances/`,
    { user: userId, page_size: "100" },
    getMTechHeaders()
  );

  const hasUnlimited = instances.some((instance) => {
    const attrs = instance.attributes || {};
    return (
      ACTIVE_MEMBERSHIP_STATUSES.has(attrs.status) &&
      UNLIMITED_MEMBERSHIP_PATTERN.test(attrs.membership_name || "")
    );
  });

  return hasUnlimited ? "unlimited" : "standard";
}

// Grants the birthday credit via a real $0 checkout so the resulting
// credit_transaction carries the real product name. Three steps:
//   1. Create a cart for the user (can silently merge with any pre-existing
//      open cart they already have).
//   2. Add the product's variant as a cart line ($0 price).
//   3. Refuse to proceed unless the cart total is exactly $0 (guards against
//      the merge-in case above), then POST /checkouts/ to complete it.
// Returns the completed order.
async function grantBirthdayCreditViaCheckout({ userId, product, partnerId }) {
  const cart = await fetchJsonWithRateLimit(`${MTEK_BASE_URL}/carts/`, {
    method: "POST",
    headers: { ...getMTechHeaders(), "Content-Type": "application/vnd.api+json" },
    body: JSON.stringify({
      data: {
        type: "carts",
        relationships: {
          user: { data: { type: "users", id: String(userId) } },
          fulfillment_partner: { data: { type: "partners", id: partnerId } },
        },
      },
    }),
  });

  const cartId = cart.data.id;

  await fetchJsonWithRateLimit(`${MTEK_BASE_URL}/cart_lines/`, {
    method: "POST",
    headers: { ...getMTechHeaders(), "Content-Type": "application/vnd.api+json" },
    body: JSON.stringify({
      data: {
        type: "cart_lines",
        attributes: { quantity: 1 },
        relationships: {
          cart: { data: { type: "carts", id: cartId } },
          product: { data: { type: "child_products", id: product.variantId } },
          partner: { data: { type: "partners", id: partnerId } },
        },
      },
    }),
  });

  const cartCheck = await fetchJsonWithRateLimit(`${MTEK_BASE_URL}/carts/${cartId}/`, {
    headers: getMTechHeaders(),
  });

  const cartTotal = Number(cartCheck.data.attributes.total);

  if (cartTotal !== 0) {
    throw new Error(
      `Refusing to check out cart ${cartId}: expected a $0 total, got $${cartTotal}. ` +
        "This usually means a pre-existing unrelated cart item merged in — check the account manually."
    );
  }

  const order = await fetchJsonWithRateLimit(`${MTEK_BASE_URL}/checkouts/`, {
    method: "POST",
    headers: { ...getMTechHeaders(), "Content-Type": "application/vnd.api+json" },
    body: JSON.stringify({
      data: {
        type: "checkouts",
        relationships: {
          cart: { data: { type: "carts", id: cartId } },
        },
      },
    }),
  });

  return order.data;
}

// Real content from Jess's "Birthday Email - Automated Messaging 2026.docx",
// split by segment (standard/unlimited) and language (fr/en). giftHtml uses
// literal <strong> for the two bolded phrases from that doc — safe, static
// markup, not user input. Footer text was only given in the doc's one worked
// HTML example (English/standard) — the other three footers are adapted from
// that same line, not verbatim from Jess. Jess approved the Option 1
// layout/format (2026-09-21), not a re-confirmation of this specific footer
// wording — real customers will now see it, so worth a final check with her.
export const EMAIL_CONTENT = {
  standard: {
    fr: {
      headlineLines: ["C’EST TA SEMAINE", "DE FÊTE!"],
      intro: "Sors les confettis… c’est TOI qu’on célèbre!",
      giftHtml:
        "Pour souligner ton anniversaire, nous avons ajouté <strong>1 cours gratuit</strong> à ton compte, à utiliser <strong>au cours des 14 prochains jours</strong>.",
      celebrate:
        "Il existe mille façons de célébrer un anniversaire. Nous, on pense qu’une bonne dose d’endorphines, une playlist incroyable, quelques « high-fives » et une communauté en or, c’est un excellent point de départ.",
      closingSuffix:
        "! On te souhaite une année remplie de bonheur, de force, et d’une foule de belles raisons de célébrer.",
      closingPrefix: "Joyeux anniversaire, ",
      buttonText: "RÉSERVER MON COURS D’ANNIVERSAIRE",
      footer:
        "Ton cours d’anniversaire est automatiquement ajouté à ton compte et expire dans 14 jours.",
    },
    en: {
      headlineLines: ["IT’S YOUR", "BIRTHDAY WEEK!"],
      intro: "Cue the confetti… It’s time to celebrate YOU.",
      giftHtml:
        "As our birthday gift to you, we’ve added <strong>1 complimentary class</strong> to your account <strong>valid for the next 14 days</strong>!",
      celebrate:
        "There are plenty of ways to celebrate a birthday. We happen to think endorphins, great playlists, high-fives, and an incredible community are a pretty great place to start ;)",
      closingPrefix: "Happy Birthday, ",
      closingSuffix:
        "! We hope this year brings you strength, joy, and plenty of reasons to celebrate.",
      buttonText: "BOOK MY BIRTHDAY CLASS!",
      footer:
        "Your birthday class is automatically added to your account and expires in 14 days.",
    },
  },
  unlimited: {
    fr: {
      headlineLines: ["C’EST TA SEMAINE", "DE FÊTE!"],
      intro: "Sors les confettis… c’est TOI qu’on célèbre!",
      giftHtml:
        "Pour souligner ton anniversaire, nous avons ajouté <strong>1 cours invité gratuit</strong> à ton compte, à utiliser <strong>au cours des 14 prochains jours</strong>. En tant que membre avec un Accès Illimité, tu peux l’utiliser pour inviter un(e) ami(e) avec qui le célébrer.",
      celebrate:
        "Il existe mille façons de célébrer un anniversaire. Nous, on pense qu’une bonne dose d’endorphines, une playlist incroyable, quelques « high-fives » et une communauté en or, c’est un excellent point de départ.",
      closingPrefix: "Joyeux anniversaire, ",
      closingSuffix:
        "! On te souhaite une année remplie de bonheur, de force, et d’une foule de belles raisons de célébrer.",
      buttonText: "RÉSERVER MON COURS D’ANNIVERSAIRE",
      footer:
        "Ton cours invité d’anniversaire est automatiquement ajouté à ton compte et expire dans 14 jours.",
    },
    en: {
      headlineLines: ["IT’S YOUR", "BIRTHDAY WEEK!"],
      intro: "Cue the confetti… It’s time to celebrate YOU.",
      giftHtml:
        "As our birthday gift to you, we’ve added <strong>1 complimentary guest class</strong> to your account, <strong>valid for the next 14 days</strong>! As an Unlimited Access Member, you can use it to bring a friend along and celebrate your birthday together.",
      celebrate:
        "There are plenty of ways to celebrate a birthday. We happen to think endorphins, great playlists, high-fives, and an incredible community are a pretty great place to start ;)",
      closingPrefix: "Happy Birthday, ",
      closingSuffix:
        "! We hope this year brings you strength, joy, and plenty of reasons to celebrate.",
      buttonText: "BOOK MY BIRTHDAY CLASS!",
      footer:
        "Your birthday guest class is automatically added to your account and expires in 14 days.",
    },
  },
};

export const BRAND = {
  pageBackground: "#f0f1f5",
  cardBackground: "#f6f6ec",
  band: "#09403f",
  headline: "#adc5c0",
  button: "#adc5c0",
};

export const BOOKING_URL = "https://www.bcyclespin.com";

// Renders one language block (header band, body paragraphs + button, and
// optionally the footer band) — reproduces the structure/styling of Jess's
// worked HTML example, including the Outlook VML button fallback.
// `includeGift`/`includeFooter` let the English half skip the free-class
// terms ("the condition") per Jess's approved format (2026-09-21: Option 1
// — English first, condition dropped from English and starred, straight
// into French with no gap, French keeps the condition + footer).
// `starGiftSentence` appends a footnote "*" to the end of the gift/condition
// sentence itself — the French half carries this so it visually pairs with
// the "*" on the English closing line (confirmed with Jon 2026-09-22: the
// star belongs at the end of the condition, not just as a generic aside).
function renderLanguageSection(
  content,
  firstName,
  { includeGift, includeFooter, starClosing, starGiftSentence }
) {
  const bodyParagraphs = [content.intro];
  if (includeGift) {
    bodyParagraphs.push(content.giftHtml + (starGiftSentence ? " *" : ""));
  }
  bodyParagraphs.push(content.celebrate);

  const paragraphHtml = bodyParagraphs
    .map(
      (text) =>
        `<tr><td dir="ltr" style="color:#000000;font-size:16px;font-family:Helvetica, Arial, sans-serif;text-align:center;padding:0 24px 16px;line-height:1.4">${text}</td></tr>`
    )
    .join("");

  const closingHtml = `<tr><td dir="ltr" style="color:#000000;font-size:16px;font-family:Helvetica, Arial, sans-serif;text-align:center;padding:0 24px 16px;line-height:1.4">${escapeHtml(
    content.closingPrefix
  )}${escapeHtml(firstName)}${escapeHtml(content.closingSuffix)}${
    starClosing ? " *" : ""
  }</td></tr>`;

  const footerHtml = includeFooter
    ? `<table border="0" cellpadding="0" cellspacing="0" align="center" width="100%" style="border-collapse:separate;table-layout:fixed;background-color:${BRAND.band}">
        <tbody><tr><td style="padding:20px;text-align:center">
          <span style="color:#ffffff;font-size:11px;font-family:Helvetica, Arial, sans-serif;line-height:14px">${escapeHtml(
            content.footer
          )}</span>
        </td></tr></tbody>
      </table>`
    : "";

  return `
<table border="0" cellpadding="0" cellspacing="0" align="center" width="100%" style="border-collapse:separate;table-layout:fixed;background-color:${BRAND.band}">
  <tbody><tr><td style="text-align:center;padding:20px 24px">
    <div style="color:${BRAND.headline};font-size:38px;font-weight:900;font-family:Helvetica, Arial, sans-serif;line-height:1.2">${content.headlineLines
      .map(escapeHtml)
      .join("<br>")}</div>
  </td></tr></tbody>
</table>
<table align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation">
  <tbody>
    ${paragraphHtml}
    ${closingHtml}
    <tr><td style="padding:8px 24px 24px">
      <table cellpadding="0" cellspacing="0" border="0" style="width:100%"><tbody><tr><td align="center">
        <!--[if mso]>
        <v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word" href="${BOOKING_URL}" style="height:48px;width:284px;v-text-anchor:middle;" arcsize="52%" fillcolor="${BRAND.button}">
        <v:stroke dashstyle="Solid" weight="0px" color="${BRAND.button}"/>
        <w:anchorlock/>
        <center style="color:#ffffff;font-family:sans-serif;font-size:16px;font-weight:700">${escapeHtml(
          content.buttonText
        )}</center>
        </v:roundrect>
        <![endif]-->
        <!--[if !mso]><!-->
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="width:100%;max-width:284px;margin:0 auto;border-collapse:separate;border-spacing:0">
          <tbody><tr><td bgcolor="${BRAND.button}" style="background-color:${BRAND.button};border-radius:25px">
            <a href="${BOOKING_URL}" target="_blank" rel="noopener" style="color:#ffffff;text-decoration:none;display:block;padding:13px 8px;text-align:center;font-family:Helvetica, Arial, sans-serif;font-size:16px;font-weight:700;line-height:22px">${escapeHtml(
              content.buttonText
            )}</a>
          </td></tr></tbody>
        </table>
        <!--<![endif]-->
      </td></tr></tbody></table>
    </td></tr>
  </tbody>
</table>
${footerHtml}`;
}

export function buildEmailHtml({ firstName, segment }) {
  const content = EMAIL_CONTENT[segment];
  const displayName = firstName || "there";

  const testBanner = LIVE_MODE
    ? ""
    : `<tr><td style="padding:12px 24px;background-color:#ffffff;border-bottom:2px solid #000000;font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#000000;text-align:center">TEST EMAIL — segment: ${escapeHtml(
        segment
      )}</td></tr>`;

  const enSection = renderLanguageSection(content.en, displayName, {
    includeGift: false,
    includeFooter: false,
    starClosing: true,
    starGiftSentence: false,
  });

  const frSection = renderLanguageSection(content.fr, displayName, {
    includeGift: true,
    includeFooter: true,
    starClosing: false,
    starGiftSentence: true,
  });

  return `<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Joyeux anniversaire / Happy Birthday</title>
</head>
<body style="width:100%;-webkit-text-size-adjust:100%;background-color:${BRAND.pageBackground};margin:0;padding:0">
<table width="100%" border="0" cellpadding="0" cellspacing="0" bgcolor="${BRAND.pageBackground}" style="background-color:${BRAND.pageBackground}">
<tbody><tr><td>
<table align="center" width="600" border="0" cellpadding="0" cellspacing="0" role="presentation" style="max-width:600px;margin:0 auto;background-color:${BRAND.cardBackground};width:600px">
<tbody>
${testBanner}
<tr><td>${enSection}</td></tr>
<tr><td>${frSection}</td></tr>
</tbody>
</table>
</td></tr></tbody>
</table>
</body>
</html>`;
}

export function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

export async function getMicrosoftAccessToken() {
  const tokenUrl = `https://login.microsoftonline.com/${encodeURIComponent(
    process.env.M365_TENANT_ID
  )}/oauth2/v2.0/token`;

  const formBody = new URLSearchParams({
    client_id: process.env.M365_CLIENT_ID,
    client_secret: process.env.M365_CLIENT_SECRET,
    scope: "https://graph.microsoft.com/.default",
    grant_type: "client_credentials",
  });

  const response = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: formBody.toString(),
  });

  const responseText = await response.text();
  let body;

  try {
    body = JSON.parse(responseText);
  } catch {
    body = responseText;
  }

  if (!response.ok) {
    throw new Error(
      `Microsoft token request failed: ${response.status} ${
        typeof body === "string" ? body : JSON.stringify(body)
      }`
    );
  }

  if (!body.access_token) {
    throw new Error("Microsoft token response did not contain an access token.");
  }

  return body.access_token;
}

export async function sendMicrosoftEmail({ accessToken, recipientEmail, subject, html }) {
  const sender = process.env.M365_SENDER_UPN;
  const url = `${GRAPH_BASE_URL}/users/${encodeURIComponent(sender)}/sendMail`;

  const body = {
    message: {
      subject,
      body: { contentType: "HTML", content: html },
      toRecipients: [{ emailAddress: { address: recipientEmail } }],
    },
    saveToSentItems: true,
  };

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const responseText = await response.text();
    throw new Error(`Microsoft sendMail failed: ${response.status} ${responseText}`);
  }
}

async function logEmailToAirtable({ email, type }) {
  const url = `${AIRTABLE_BASE_URL}/${process.env.CUSTOMER_BASE_ID}/${EMAIL_LOGS_TABLE_ID}`;

  const body = {
    fields: {
      Email: email,
      Type: type,
    },
  };

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const responseText = await response.text();
    throw new Error(`Airtable Email Logs insert failed: ${response.status} ${responseText}`);
  }
}

// Only auto-run when executed directly (`node scripts/birthday-credit-email.mjs`),
// not when imported — lets scripts/preview-birthday-emails.mjs reuse
// buildEmailHtml/getMicrosoftAccessToken/sendMicrosoftEmail without
// triggering the real birthday-matching + credit-granting flow.
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error("Automation failed:");
    console.error(error);
    process.exitCode = 1;
  });
}
