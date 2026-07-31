// scripts/birthday-credit-email.mjs
//
// Weekly automation: finds clients whose birthday falls exactly 7 days from
// today, grants them a free class credit in MTEK, and emails them. Two
// segments, decided by whether the client has an active-or-frozen unlimited
// membership (Emilie created both products on 2026-07-28):
//   - Unlimited (active/frozen): "Passe invité d'anniversaire" (shareable,
//     since unlimited members can bring a guest) — product 16603 / variant
//     16604 / credit id 2323 ("Class Packages").
//   - Everyone else: "Pass d'anniversaire" (not shareable) — product 16601 /
//     variant 16602 / credit id 2324 ("Passe Flex").
// Both are $0 variants configured in MTEK with a 2-week relative expiration.
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
// LIVE_MODE is hardcoded false: this script has never sent a real customer
// an email or a real credit. In this mode, only the FIRST real match found
// each run is processed, and both the email and the credit grant are
// redirected to a test account (default jonathan@bcyclespin.com) instead of
// the real customer — see recipientEmail / creditTargetUserId below. Flip
// LIVE_MODE to true (and remove the redirect) only once Jess's final HTML
// templates are ready and you've verified a test run end to end.

import { fetchJsonWithRateLimit, fetchAllPages } from "./lib/mtek.mjs";

const LIVE_MODE = false;

const MTEK_BASE_URL = "https://bcycle.marianatek.com/api";
const GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0";
const AIRTABLE_BASE_URL = "https://api.airtable.com/v0";
const EMAIL_LOGS_TABLE_ID = "tbloAdBJHSygcndbA"; // "Email Logs"
const TIME_ZONE = "America/Toronto";

const MIN_BIRTH_YEAR = 1920;
const MAX_BIRTH_YEAR = 2020;

const CREDIT_EXPIRY_DAYS = 14; // matches both variants' 2-week relative expiration

const UNLIMITED_MEMBERSHIP_PATTERN = /unlimited|illimit/i;
const ACTIVE_MEMBERSHIP_STATUSES = new Set(["active", "frozen"]);

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

  const targetBirthdayDate = manuallySelectedDate || getTargetBirthdayDate();

  console.log("==========================================");
  console.log("Birthday credit email automation started");
  console.log(`Mode: ${LIVE_MODE ? "LIVE" : "TEST"}`);
  console.log(`Target birthday date (month/day): ${targetBirthdayDate.slice(5)}`);
  if (!LIVE_MODE) {
    console.log(
      `TEST MODE: emails + credit grants are redirected to ${testEmail}'s account. ` +
        "No real customer will be emailed or credited."
    );
  }
  console.log("==========================================");

  const matches = await findBirthdayMatches(targetBirthdayDate);

  console.log(`Found ${matches.length} client(s) with a birthday on ${targetBirthdayDate.slice(5)}.`);
  matches.forEach((match, index) => {
    console.log(`  ${index + 1}. ${match.email || "(no email)"} (user ${match.id})`);
  });

  if (matches.length === 0) {
    console.log("No qualifying birthdays found for this run.");
    return;
  }

  // LIVE_MODE is always false right now — only ever process the first real
  // match, mirroring the existing isTestMode ? transactions.slice(0, 1) : ...
  // precedent in credit-expiry-email.mjs.
  const matchesToProcess = LIVE_MODE ? matches : matches.slice(0, 1);

  if (!LIVE_MODE && matches.length > 1) {
    console.log(
      `TEST MODE: only processing the first match (${matchesToProcess[0].email}); ` +
        `${matches.length - 1} other real match(es) found this run were skipped.`
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

      console.log(
        `Processing ${match.email} (segment: ${segment}, product: ${product.name})` +
          (LIVE_MODE ? "" : ` — redirecting to ${recipientEmail} / MTEK user ${creditTargetUserId}`)
      );

      await grantBirthdayCredit({
        userId: creditTargetUserId,
        product,
        realCustomerEmail: match.email,
      });

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
        await logEmailToAirtable({
          email: match.email,
          type: `Birthday credit email${LIVE_MODE ? "" : " (TEST)"}`,
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

function truncate(value, maxLength) {
  return value.length > maxLength ? value.slice(0, maxLength) : value;
}

function getTargetBirthdayDate() {
  const todayInToronto = getDateInTimeZone(new Date(), TIME_ZONE);
  return addCalendarDays(todayInToronto, 7);
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

function getMTechHeaders() {
  return {
    Authorization: `Bearer ${process.env.MTEK_API_TOKEN}`,
    Accept: "application/vnd.api+json",
  };
}

// Loops every birth year in range, querying the exact birth_date=YYYY-MM-DD
// match for each, and collects every real (non-archived, has-email) match.
async function findBirthdayMatches(targetDate) {
  const monthDay = targetDate.slice(5); // "MM-DD"
  const matches = [];

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

      matches.push({
        id: user.id,
        email: attrs.email,
        firstName: attrs.first_name,
        lastName: attrs.last_name,
        birthDate: attrs.birth_date,
      });
    }
  }

  return matches;
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

async function grantBirthdayCredit({ userId, product, realCustomerEmail }) {
  const issuedAt = new Date();
  const expirationDatetime = new Date(
    issuedAt.getTime() + CREDIT_EXPIRY_DAYS * 24 * 60 * 60 * 1000
  ).toISOString();

  const body = {
    data: {
      type: "credit_transactions",
      attributes: {
        transaction_amount: 1,
        transaction_currency: "CAD",
        transaction_penalty_fee_type: "fee_loss_of_credit",
        expiration_datetime: expirationDatetime,
        credit_name: product.name,
        origination_type: "complimentary",
        // MTEK caps `note` at 100 characters.
        note: truncate(
          LIVE_MODE
            ? "Birthday credit automation"
            : `TEST birthday credit automation — real customer: ${realCustomerEmail}`,
          100
        ),
      },
      relationships: {
        user: { data: { type: "users", id: String(userId) } },
        credit: { data: { type: "credits", id: product.creditId } },
      },
    },
  };

  await fetchJsonWithRateLimit(`${MTEK_BASE_URL}/credit_transactions`, {
    method: "POST",
    headers: {
      ...getMTechHeaders(),
      "Content-Type": "application/vnd.api+json",
    },
    body: JSON.stringify(body),
  });
}

function buildEmailHtml({ firstName, segment, product }) {
  const safeFirstName = escapeHtml(firstName);
  const testBanner = LIVE_MODE
    ? ""
    : `
      <div style="margin-bottom:24px;padding:16px;border:2px solid #000000;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.5;">
        <strong>TEST EMAIL — placeholder content, pending Jess's final HTML templates.</strong><br>
        Segment: ${escapeHtml(segment)}<br>
        Product: ${escapeHtml(product.name)}
      </div>
    `;

  return `
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Joyeux anniversaire / Happy Birthday</title>
</head>
<body style="margin:0;padding:0;background-color:#ffffff;">
  <div style="max-width:680px;margin:0 auto;padding:32px 24px;font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:1.5;color:#000000;">

    ${testBanner}

    <p>Bonjour ${safeFirstName},</p>

    <p>
      Joyeux anniversaire de la part de toute l'équipe b.cycle! Pour souligner
      l'occasion, on t'offre ton cours gratuit : <strong>${escapeHtml(product.name)}</strong>.
      Ce crédit est valide pendant 2 semaines à partir de sa date d'émission.
    </p>

    <p>Au plaisir de célébrer avec toi bientôt en studio!</p>

    <p>L'équipe b.cycle</p>

    <p style="margin:28px 0;">–</p>

    <p>Hi ${safeFirstName},</p>

    <p>
      Happy birthday from the whole b.cycle team! To celebrate, we're giving
      you a free class: <strong>${escapeHtml(product.name)}</strong>. This
      credit is valid for 2 weeks from its issue date.
    </p>

    <p>We look forward to celebrating with you in studio soon!</p>

    <p>The b.cycle Team</p>

  </div>
</body>
</html>
  `;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function getMicrosoftAccessToken() {
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

async function sendMicrosoftEmail({ accessToken, recipientEmail, subject, html }) {
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

main().catch((error) => {
  console.error("Automation failed:");
  console.error(error);
  process.exitCode = 1;
});
