// scripts/preview-birthday-email-format-options.mjs
//
// One-off: Jess replied to the [PREVIEW] emails (2026-09-21) saying she
// doesn't like the gap between the French and English sections, and
// proposed two fixes:
//   Option 1 — English first, with the "condition" (the free-class terms
//     sentence) dropped from the English copy and starred (*) so English
//     readers know to continue into French for the actual terms; French
//     immediately follows with no gap, keeping its condition sentence and
//     the footer disclaimer.
//   Option 2 — Drop English entirely; French-only.
// Both built for the "standard" (non-unlimited/Pass d'anniversaire) segment,
// since that's the one she was replying to. Sends both to a test address
// for review — doesn't touch MTEK, doesn't send to Jess directly.

import {
  EMAIL_CONTENT,
  BRAND,
  BOOKING_URL,
  escapeHtml,
  getMicrosoftAccessToken,
  sendMicrosoftEmail,
} from "./birthday-credit-email.mjs";

const REQUIRED_ENVIRONMENT_VARIABLES = [
  "M365_CLIENT_ID",
  "M365_CLIENT_SECRET",
  "M365_TENANT_ID",
  "M365_SENDER_UPN",
];

// Renders one language block. `includeGift`/`includeFooter` let Option 1
// drop the "condition" copy from the English half. `asteriskNote`, if set,
// appends a small starred line pointing readers to keep reading below.
function renderSection(content, firstName, { includeGift, includeFooter, asteriskNote }) {
  const bodyParagraphs = [content.intro];
  if (includeGift) bodyParagraphs.push(content.giftHtml);
  bodyParagraphs.push(content.celebrate);

  const paragraphHtml = bodyParagraphs
    .map(
      (text) =>
        `<tr><td dir="ltr" style="color:#000000;font-size:16px;font-family:Helvetica, Arial, sans-serif;text-align:center;padding:0 24px 16px;line-height:1.4">${text}</td></tr>`
    )
    .join("");

  const closingText = `${content.closingPrefix}${escapeHtml(firstName)}${
    content.closingSuffix
  }${asteriskNote ? " *" : ""}`;

  const closingHtml = `<tr><td dir="ltr" style="color:#000000;font-size:16px;font-family:Helvetica, Arial, sans-serif;text-align:center;padding:0 24px 16px;line-height:1.4">${closingText}</td></tr>`;

  const asteriskHtml = asteriskNote
    ? `<tr><td style="text-align:center;padding:0 24px 16px;font-family:Helvetica, Arial, sans-serif;font-size:12px;color:#555555;font-style:italic">* ${escapeHtml(
        asteriskNote
      )}</td></tr>`
    : "";

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
    ${asteriskHtml}
    <tr><td style="padding:8px 24px 24px">
      <table cellpadding="0" cellspacing="0" border="0" style="width:100%"><tbody><tr><td align="center">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="width:100%;max-width:284px;margin:0 auto;border-collapse:separate;border-spacing:0">
          <tbody><tr><td bgcolor="${BRAND.button}" style="background-color:${BRAND.button};border-radius:25px">
            <a href="${BOOKING_URL}" target="_blank" rel="noopener" style="color:#ffffff;text-decoration:none;display:block;padding:13px 8px;text-align:center;font-family:Helvetica, Arial, sans-serif;font-size:16px;font-weight:700;line-height:22px">${escapeHtml(
              content.buttonText
            )}</a>
          </td></tr></tbody>
        </table>
      </td></tr></tbody></table>
    </td></tr>
  </tbody>
</table>
${footerHtml}`;
}

function wrapEmail(bodyHtml) {
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
<tr><td style="padding:12px 24px;background-color:#ffffff;border-bottom:2px solid #000000;font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#000000;text-align:center">TEST EMAIL — format option preview</td></tr>
${bodyHtml}
</tbody>
</table>
</td></tr></tbody>
</table>
</body>
</html>`;
}

export function buildOption1Html(firstName) {
  const content = EMAIL_CONTENT.standard;

  const enSection = renderSection(content.en, firstName, {
    includeGift: false,
    includeFooter: false,
    asteriskNote: "See the full offer details below",
  });

  const frSection = renderSection(content.fr, firstName, {
    includeGift: true,
    includeFooter: true,
    asteriskNote: null,
  });

  return wrapEmail(`<tr><td>${enSection}</td></tr><tr><td>${frSection}</td></tr>`);
}

export function buildOption2Html(firstName) {
  const content = EMAIL_CONTENT.standard;

  const frSection = renderSection(content.fr, firstName, {
    includeGift: true,
    includeFooter: true,
    asteriskNote: null,
  });

  return wrapEmail(`<tr><td>${frSection}</td></tr>`);
}

async function main() {
  const missing = REQUIRED_ENVIRONMENT_VARIABLES.filter((name) => !process.env[name]?.trim());

  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
  }

  const recipientEmail = process.env.TEST_EMAIL?.trim() || "jonathan@bcyclespin.com";
  const firstName = process.env.PREVIEW_FIRST_NAME?.trim() || "Jonathan";

  const accessToken = await getMicrosoftAccessToken();

  console.log(`Sending Option 1 (English first, starred, no gap) to ${recipientEmail}...`);
  await sendMicrosoftEmail({
    accessToken,
    recipientEmail,
    subject: "[OPTION 1] Birthday Email — English first, condition only in French (*)",
    html: buildOption1Html(firstName),
  });
  console.log("Sent Option 1.");

  console.log(`Sending Option 2 (French only) to ${recipientEmail}...`);
  await sendMicrosoftEmail({
    accessToken,
    recipientEmail,
    subject: "[OPTION 2] Birthday Email — French only",
    html: buildOption2Html(firstName),
  });
  console.log("Sent Option 2.");

  console.log("Done — both format options sent.");
}

main().catch((error) => {
  console.error("Preview send failed:");
  console.error(error);
  process.exitCode = 1;
});
