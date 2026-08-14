// scripts/report-low-credit-customers.mjs
// One-off report: customers whose active, non-expired credit package is
// almost finished (remaining_credits_cache <= MAX_REMAINING_CREDITS).
// Prints a table to the workflow log — nothing is sent or written anywhere.

import { fetchAllPages } from "./lib/mtek.mjs";

const MTEK_BASE_URL = (process.env.MTEK_BASE_URL || "https://bcycle.marianatek.com/api").replace(/\/+$/, "");
const MTEK_API_TOKEN = (process.env.MTEK_API_TOKEN || "").trim();
const MAX_REMAINING_CREDITS = Number(process.env.MAX_REMAINING_CREDITS || "2");

if (!MTEK_API_TOKEN) {
  throw new Error("MTEK_API_TOKEN environment variable is required.");
}

function authHeaders() {
  return {
    Authorization: `Bearer ${MTEK_API_TOKEN}`,
    Accept: "application/vnd.api+json",
  };
}

async function getLowCreditTransactions() {
  const url = `${MTEK_BASE_URL}/credit_transactions`;
  const allTransactions = await fetchAllPages(
    url,
    { active: "true", is_expired: "false", page_size: "100" },
    authHeaders()
  );

  return allTransactions.filter((transaction) => {
    const remaining = Number(transaction?.attributes?.remaining_credits_cache || 0);
    const isExpired = transaction?.attributes?.is_expired === true;
    return remaining > 0 && remaining <= MAX_REMAINING_CREDITS && !isExpired;
  });
}

async function getUser(userId, cache) {
  if (cache.has(userId)) return cache.get(userId);

  const url = `${MTEK_BASE_URL}/users/${encodeURIComponent(userId)}/`;
  const res = await fetch(url, { headers: authHeaders() });
  if (!res.ok) {
    cache.set(userId, null);
    return null;
  }
  const body = await res.json();
  const attributes = body?.data?.attributes || body?.attributes || body;

  const user = {
    firstName: attributes?.first_name || attributes?.firstName || "",
    lastName: attributes?.last_name || attributes?.lastName || "",
    email: attributes?.email || "",
  };
  cache.set(userId, user);
  return user;
}

async function main() {
  console.log("==========================================");
  console.log("Low-credit customer report");
  console.log(`Threshold: remaining_credits_cache <= ${MAX_REMAINING_CREDITS}`);
  console.log("==========================================");

  const transactions = await getLowCreditTransactions();
  console.log(`Found ${transactions.length} qualifying transaction(s).\n`);

  const userCache = new Map();
  const rows = [];

  for (const transaction of transactions) {
    const userId = transaction?.relationships?.user?.data?.id;
    if (!userId) continue;

    const user = await getUser(userId, userCache);

    rows.push({
      customer: user ? `${user.firstName} ${user.lastName}`.trim() : `(user ${userId})`,
      email: user?.email || "",
      credit_name: transaction.attributes.credit_name || "",
      remaining_credits: Number(transaction.attributes.remaining_credits_cache || 0),
      expiration: transaction.attributes.expiration_datetime || "",
      transaction_id: transaction.id,
    });
  }

  rows.sort((a, b) => a.remaining_credits - b.remaining_credits);

  for (const row of rows) {
    console.log(
      `${row.remaining_credits} left  |  ${row.customer.padEnd(30)}  |  ${row.email.padEnd(35)}  |  ${row.credit_name}  |  expires ${row.expiration}`
    );
  }

  console.log(`\nTotal customers with a nearly-finished package: ${rows.length}`);
}

main().catch((error) => {
  console.error("Report failed:", error);
  process.exitCode = 1;
});
