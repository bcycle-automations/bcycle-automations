// scripts/preview-birthday-emails.mjs
//
// One-off: sends the two real (non-placeholder) birthday email designs —
// standard and unlimited, each bilingual (fr then en) — to a test address,
// so the content/design can be reviewed before going live. Doesn't touch
// MTEK at all (no birthday matching, no credit granting) — email only.

import {
  buildEmailHtml,
  getMicrosoftAccessToken,
  sendMicrosoftEmail,
} from "./birthday-credit-email.mjs";

const REQUIRED_ENVIRONMENT_VARIABLES = [
  "M365_CLIENT_ID",
  "M365_CLIENT_SECRET",
  "M365_TENANT_ID",
  "M365_SENDER_UPN",
];

async function main() {
  const missing = REQUIRED_ENVIRONMENT_VARIABLES.filter(
    (name) => !process.env[name]?.trim()
  );

  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
  }

  const recipientEmail = process.env.TEST_EMAIL?.trim() || "jonathan@bcyclespin.com";
  const firstName = process.env.PREVIEW_FIRST_NAME?.trim() || "Jonathan";

  const accessToken = await getMicrosoftAccessToken();

  for (const segment of ["standard", "unlimited"]) {
    const html = buildEmailHtml({ firstName, segment });
    const subject =
      segment === "standard"
        ? "[PREVIEW] Birthday Email — Pass d'anniversaire (non-unlimited, FR+EN)"
        : "[PREVIEW] Birthday Email — Passe invité d'anniversaire (unlimited, FR+EN)";

    console.log(`Sending ${segment} preview to ${recipientEmail}...`);

    await sendMicrosoftEmail({
      accessToken,
      recipientEmail,
      subject,
      html,
    });

    console.log(`Sent ${segment} preview.`);
  }

  console.log("Done — both previews sent.");
}

main().catch((error) => {
  console.error("Preview send failed:");
  console.error(error);
  process.exitCode = 1;
});
