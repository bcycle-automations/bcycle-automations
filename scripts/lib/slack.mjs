// scripts/lib/slack.mjs
// Posts a message to the #bigquery-updates Slack channel via incoming
// webhook. Silently skips (logs instead) if no webhook is configured yet,
// so testing isn't blocked on Slack setup.

export async function sendSlackMessage(text) {
  const webhookUrl = (process.env.SLACK_BIGQUERY_UPDATES_WEBHOOK_URL || "").trim();
  if (!webhookUrl) {
    console.log(`(Slack notification skipped — SLACK_BIGQUERY_UPDATES_WEBHOOK_URL not set) ${text}`);
    return;
  }

  const res = await fetch(webhookUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Slack webhook failed ${res.status}: ${body}`);
  }
}
