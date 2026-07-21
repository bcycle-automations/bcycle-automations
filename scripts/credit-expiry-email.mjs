const MTEK_BASE_URL = "https://bcycle.marianatek.com/api";
const GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0";
const TIME_ZONE = "America/Toronto";

/*
 * Replace this later with the actual form URL.
 */
const FORM_URL = "https://bcyclespin.com/en/i-have-a-question/";

const REQUIRED_ENVIRONMENT_VARIABLES = [
  "MTEK_API_TOKEN",
  "M365_CLIENT_ID",
  "M365_CLIENT_SECRET",
  "M365_TENANT_ID",
  "M365_SENDER_UPN",
];

async function main() {
  validateEnvironmentVariables();

  const testEmail = cleanString(process.env.TEST_EMAIL);
  const manuallySelectedDate = cleanString(
    process.env.TARGET_EXPIRATION_DATE
  );

  const isTestMode = Boolean(testEmail);

  const targetExpirationDate = getTargetExpirationDate(
    manuallySelectedDate
  );

  console.log("==========================================");
  console.log("Credit expiry automation started");
  console.log(`Mode: ${isTestMode ? "TEST" : "PRODUCTION"}`);
  console.log(`Target date: ${targetExpirationDate}`);

  if (isTestMode) {
    console.log(`Test recipient: ${testEmail}`);
    console.log(
      "Only one email will be sent, and it will be sent to the test address."
    );
  }

  console.log("==========================================");

  const transactions = await getExpiringCreditTransactions(
    targetExpirationDate
  );

  console.log(
    `Found ${transactions.length} transaction(s) with remaining credits.`
  );

  if (transactions.length === 0) {
    console.log("No qualifying credit packages were found.");
    return;
  }

  /*
   * During a test, use only the first qualifying transaction.
   * During a scheduled production run, process every transaction.
   */
  const transactionsToProcess = isTestMode
    ? transactions.slice(0, 1)
    : transactions;

  const microsoftAccessToken =
    await getMicrosoftAccessToken();

  let successfulEmails = 0;
  let failedEmails = 0;

  for (const transaction of transactionsToProcess) {
    try {
      const userId =
        transaction?.relationships?.user?.data?.id;

      if (!userId) {
        throw new Error(
          `Transaction ${transaction.id} does not contain a user ID.`
        );
      }

      const user = await getMTechUser(userId);

      if (!user.email && !isTestMode) {
        throw new Error(
          `Mariana Tek user ${userId} does not have an email address.`
        );
      }

      const recipientEmail = isTestMode
        ? testEmail
        : user.email;

      const creditsRemaining = Number(
        transaction.attributes.remaining_credits_cache
      );

      const expirationDate =
        transaction.attributes.expiration_datetime;

      const emailHtml = buildEmailHtml({
        firstName: user.firstName || "there",
        expirationDate,
        creditsRemaining,
        creditName:
          transaction.attributes.credit_name ||
          "Class Package",
        isTestMode,
        originalCustomerEmail: user.email,
        transactionId: transaction.id,
      });

      const subject = isTestMode
        ? "[TEST] Rappel : votre forfait de cours expire bientôt"
        : "Rappel : votre forfait de cours expire bientôt";

      await sendMicrosoftEmail({
        accessToken: microsoftAccessToken,
        recipientEmail,
        subject,
        html: emailHtml,
      });

      successfulEmails += 1;

      console.log(
        `Email sent successfully for transaction ${transaction.id} to ${recipientEmail}.`
      );
    } catch (error) {
      failedEmails += 1;

      console.error(
        `Failed to process transaction ${transaction.id}:`,
        error.message
      );
    }
  }

  console.log("==========================================");
  console.log("Automation completed");
  console.log(`Successful emails: ${successfulEmails}`);
  console.log(`Failed emails: ${failedEmails}`);
  console.log("==========================================");

  if (failedEmails > 0) {
    process.exitCode = 1;
  }
}

function validateEnvironmentVariables() {
  const missingVariables =
    REQUIRED_ENVIRONMENT_VARIABLES.filter(
      (variableName) =>
        !cleanString(process.env[variableName])
    );

  if (missingVariables.length > 0) {
    throw new Error(
      `Missing required environment variables: ${missingVariables.join(
        ", "
      )}`
    );
  }
}

function cleanString(value) {
  return typeof value === "string"
    ? value.trim()
    : "";
}

function getTargetExpirationDate(manualDate) {
  if (manualDate) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(manualDate)) {
      throw new Error(
        "TARGET_EXPIRATION_DATE must use YYYY-MM-DD format."
      );
    }

    return manualDate;
  }

  const todayInToronto = getDateInTimeZone(
    new Date(),
    TIME_ZONE
  );

  return addCalendarDays(todayInToronto, 30);
}

function getDateInTimeZone(date, timeZone) {
  const formatter = new Intl.DateTimeFormat(
    "en-CA",
    {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }
  );

  const parts = formatter.formatToParts(date);

  const values = Object.fromEntries(
    parts
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value])
  );

  return `${values.year}-${values.month}-${values.day}`;
}

function addCalendarDays(dateString, daysToAdd) {
  const [year, month, day] = dateString
    .split("-")
    .map(Number);

  const result = new Date(
    Date.UTC(
      year,
      month - 1,
      day + daysToAdd,
      12,
      0,
      0
    )
  );

  return result.toISOString().slice(0, 10);
}

async function getExpiringCreditTransactions(
  targetExpirationDate
) {
  const allTransactions = [];

  let page = 1;
  let totalPages = 1;

  do {
    const url = new URL(
      `${MTEK_BASE_URL}/credit_transactions`
    );

    url.searchParams.set("active", "true");
    url.searchParams.set("is_expired", "false");
    url.searchParams.set(
      "expiration_datetime",
      `${targetExpirationDate}T00:00:00`
    );
    url.searchParams.set("page_size", "100");
    url.searchParams.set("page", String(page));

    console.log(
      `Requesting Mariana Tek transactions page ${page}...`
    );

    const body = await fetchJson(url, {
      method: "GET",
      headers: getMTechHeaders(),
    });

    if (!Array.isArray(body?.data)) {
      throw new Error(
        "Mariana Tek credit transactions response did not contain a data array."
      );
    }

    allTransactions.push(...body.data);

    totalPages = Number(
      body?.meta?.pagination?.pages || 1
    );

    console.log(
      `Received page ${page} of ${totalPages}.`
    );

    page += 1;
  } while (page <= totalPages);

  return allTransactions.filter((transaction) => {
    const remainingCredits = Number(
      transaction?.attributes
        ?.remaining_credits_cache || 0
    );

    const isExpired =
      transaction?.attributes?.is_expired === true;

    return remainingCredits > 0 && !isExpired;
  });
}

async function getMTechUser(userId) {
  const url =
    `${MTEK_BASE_URL}/users/` +
    `${encodeURIComponent(userId)}/`;

  const body = await fetchJson(url, {
    method: "GET",
    headers: getMTechHeaders(),
  });

  /*
   * This supports both:
   * 1. A normal user object.
   * 2. A JSON:API response with data.attributes.
   */
  const attributes =
    body?.data?.attributes ||
    body?.attributes ||
    body;

  if (!attributes || typeof attributes !== "object") {
    throw new Error(
      `Unexpected Mariana Tek user response for user ${userId}.`
    );
  }

  return {
    id: String(userId),
    firstName: cleanString(
      attributes.first_name ||
      attributes.firstName
    ),
    lastName: cleanString(
      attributes.last_name ||
      attributes.lastName
    ),
    email: cleanString(attributes.email),
  };
}

function getMTechHeaders() {
  return {
    Authorization:
      `Bearer ${process.env.MTEK_API_TOKEN}`,
    Accept: "application/json",
  };
}

async function getMicrosoftAccessToken() {
  const tokenUrl =
    `https://login.microsoftonline.com/` +
    `${encodeURIComponent(
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
    headers: {
      "Content-Type":
        "application/x-www-form-urlencoded",
    },
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
      `Microsoft token request failed: ` +
      `${response.status} ` +
      `${typeof body === "string"
        ? body
        : JSON.stringify(body)}`
    );
  }

  if (!body.access_token) {
    throw new Error(
      "Microsoft token response did not contain an access token."
    );
  }

  return body.access_token;
}

async function sendMicrosoftEmail({
  accessToken,
  recipientEmail,
  subject,
  html,
}) {
  const sender = process.env.M365_SENDER_UPN;

  const url =
    `${GRAPH_BASE_URL}/users/` +
    `${encodeURIComponent(sender)}/sendMail`;

  const body = {
    message: {
      subject,
      body: {
        contentType: "HTML",
        content: html,
      },
      toRecipients: [
        {
          emailAddress: {
            address: recipientEmail,
          },
        },
      ],
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

    throw new Error(
      `Microsoft sendMail failed: ` +
      `${response.status} ${responseText}`
    );
  }
}

function buildEmailHtml({
  firstName,
  expirationDate,
  creditsRemaining,
  creditName,
  isTestMode,
  originalCustomerEmail,
  transactionId,
}) {
  const safeFirstName = escapeHtml(firstName);

  const englishExpirationDate =
    formatEnglishDate(expirationDate);

  const frenchExpirationDate =
    formatFrenchDate(expirationDate);

  const testBanner = isTestMode
    ? `
      <div style="
        margin-bottom:24px;
        padding:16px;
        border:2px solid #000000;
        font-family:Arial,Helvetica,sans-serif;
        font-size:14px;
        line-height:1.5;
      ">
        <strong>TEST EMAIL</strong><br>
        Mariana Tek transaction: ${escapeHtml(
          transactionId
        )}<br>
        Package: ${escapeHtml(creditName)}<br>
        Original customer email: ${escapeHtml(
          originalCustomerEmail || "No email on account"
        )}
      </div>
    `
    : "";

  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
  >
  <title>
    Rappel : votre forfait de cours expire bientôt
  </title>
</head>

<body style="
  margin:0;
  padding:0;
  background-color:#ffffff;
">
  <div style="
    max-width:680px;
    margin:0 auto;
    padding:32px 24px;
    font-family:Arial,Helvetica,sans-serif;
    font-size:16px;
    line-height:1.5;
    color:#000000;
  ">

    ${testBanner}

    <p>Hi ${safeFirstName},</p>

    <p>
      Just a friendly reminder that your current class
      package is scheduled to expire on
      <strong>${escapeHtml(
        englishExpirationDate
      )}</strong>,
      which is 30 days from today.
    </p>

    <p>
      At the time this email was sent, you had
      <strong>${creditsRemaining}</strong>
      remaining on your package.
    </p>

    <p>
      We wanted to give you plenty of notice so you have
      time to plan ahead and make the most of your remaining
      credits.
    </p>

    <p style="margin-bottom:0;">
      <strong>
        <u>Your package at a glance:</u>
      </strong>
    </p>

    <p style="margin-top:0;">
      <strong>Expiry Date:</strong>
      ${escapeHtml(englishExpirationDate)}
      <br>
      <strong>Credits Remaining:</strong>
      ${creditsRemaining}
    </p>

    <p>
      Now’s the perfect time to book your next visit!
    </p>

    <p>
      If you have any questions about your expiry date or
      your account,
      <a
        href="${escapeHtml(FORM_URL)}"
        style="
          color:#0563c1;
          text-decoration:underline;
        "
      >
        fill out this form here
      </a>
      and our team will get back to you within 2 business
      days. Our team would be happy to help and talk through
      your options!
    </p>

    <p>
      We look forward to seeing you in the studio soon!
    </p>

    <p>The b.cycle Team</p>

    <p style="margin:28px 0;">–</p>

    <p>Bonjour ${safeFirstName},</p>

    <p>
      Petit rappel amical : votre forfait de cours expirera
      le
      <strong>${escapeHtml(
        frenchExpirationDate
      )}</strong>,
      soit dans 30 jours.
    </p>

    <p>
      Au moment de l’envoi de ce courriel, il vous restait
      <strong>${creditsRemaining}</strong>
      crédit(s) à utiliser sur votre forfait.
    </p>

    <p>
      Nous souhaitions vous en aviser à l’avance afin que
      vous puissiez planifier vos prochaines visites et
      profiter pleinement de vos crédits restants avant leur
      expiration.
    </p>

    <p style="margin-bottom:0;">
      <strong>Votre forfait en un coup d’œil :</strong>
    </p>

    <p style="margin-top:0;">
      <strong>Date d’expiration :</strong>
      ${escapeHtml(frenchExpirationDate)}
      <br>
      <strong>Crédits restants :</strong>
      ${creditsRemaining}
    </p>

    <p>
      C’est le moment idéal pour réserver votre prochaine
      séance!
    </p>

    <p>
      Si vous avez des questions concernant la date
      d’expiration de votre forfait ou votre compte,
      <a
        href="${escapeHtml(FORM_URL)}"
        style="
          color:#0563c1;
          text-decoration:underline;
        "
      >
        remplissez ce formulaire
      </a>
      et un membre de notre équipe vous répondra dans un
      délai de 2 jours ouvrables. Nous serons ravis de vous
      aider et de discuter des différentes options qui
      s’offrent à vous.
    </p>

    <p>
      Au plaisir de vous accueillir en studio bientôt!
    </p>

    <p>L’équipe b.cycle</p>

  </div>
</body>
</html>
  `;
}

function formatEnglishDate(isoDate) {
  const dateOnly = isoDate.slice(0, 10);

  const [year, month, day] = dateOnly
    .split("-")
    .map(Number);

  const date = new Date(
    Date.UTC(year, month - 1, day, 12)
  );

  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "UTC",
    year: "numeric",
    month: "long",
    day: "numeric",
  }).format(date);
}

function formatFrenchDate(isoDate) {
  const dateOnly = isoDate.slice(0, 10);

  const [year, month, day] = dateOnly
    .split("-")
    .map(Number);

  const date = new Date(
    Date.UTC(year, month - 1, day, 12)
  );

  return new Intl.DateTimeFormat("fr-CA", {
    timeZone: "UTC",
    year: "numeric",
    month: "long",
    day: "numeric",
  }).format(date);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const responseText = await response.text();

  let body;

  try {
    body = responseText
      ? JSON.parse(responseText)
      : null;
  } catch {
    body = responseText;
  }

  if (!response.ok) {
    throw new Error(
      `Request failed: ${response.status} ` +
      `${response.statusText}. ` +
      `${typeof body === "string"
        ? body
        : JSON.stringify(body)}`
    );
  }

  return body;
}

main().catch((error) => {
  console.error("Automation failed:");
  console.error(error);
  process.exitCode = 1;
});
