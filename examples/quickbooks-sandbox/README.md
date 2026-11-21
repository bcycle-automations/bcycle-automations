# QuickBooks sandbox OAuth example

Run this local app with Node.js 22 or later. It uses Intuit's documented authorization-code flow and a single sandbox company. Tokens are stored in `.tokens/quickbooks.json` with owner-only permissions on macOS and Linux. The app serializes refreshes, saves the latest refresh token atomically, and exposes the UserInfo response without using it to sign users in.

## Set up and connect

1. In the Intuit Developer Portal, open your app's **Development** settings. Add `http://localhost:3000/oauth/callback` under **Redirect URIs** and save. Use this extra redirect for the local app. Intuit's hosted Quick Start uses its own redirect.
2. From the repository root, run these commands. Fill in both Development credentials in the new `.env` file without sharing them or entering them in shell history.

   ```sh
   cd examples/quickbooks-sandbox
   (umask 077; cp -n .env.example .env)
   ${EDITOR:-nano} .env
   node --env-file=.env server.mjs
   ```

3. Open [Connect to QuickBooks](http://localhost:3000/connect) in your browser. Sign in, choose the sandbox company, and approve access. The callback takes you to the company info response.

## Run the examples

```sh
curl -fsS http://localhost:3000/company-info
curl -fsS http://localhost:3000/user-info
curl -fsS -X POST -H 'X-Local-Action: 1' http://localhost:3000/refresh
```

The GET requests refresh a token shortly before expiry and retry once after an upstream 401. The manual refresh prints only the realm and expiry. If refresh returns `invalid_grant`, visit `/connect` for a fresh grant. For an Intuit API error, use the returned operation, HTTP status, error code and trace ID to troubleshoot.

For the optional **USD 1.00 sandbox charge**, use a US sandbox company with Payments access. Create a fresh sandbox Payments token with Intuit's [Tokens API](https://developer.intuit.com/app/developer/qbpayments/docs/api/resources/all-entities/tokens) using sandbox test card data. Put that token in `INTUIT_SANDBOX_PAYMENT_TOKEN` in `.env`, restart the app, then run:

```sh
curl -fsS -X POST -H 'X-Local-Action: 1' http://localhost:3000/test-charge
```

The charge uses Intuit's sandbox host and sends a unique `Request-Id`. It runs only when explicitly requested and never retries automatically. If the request times out, use the returned request ID to investigate before trying again. Use a fresh Payments token for each new charge.

If the app has only Accounting and OpenID enabled, remove `com.intuit.quickbooks.payment` from `INTUIT_SCOPES` before connecting. The Payments example requires that scope and a new authorization when it is added.

## Test locally

```sh
node --test
```

The test uses mock Intuit responses. See the official [OAuth flow](https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0), [sandbox discovery](https://developer.intuit.com/.well-known/openid_sandbox_configuration/), [CompanyInfo API](https://developer.intuit.com/docs/api/accounting/CompanyInfo), and [Charges API](https://developer.intuit.com/app/developer/qbpayments/docs/api/resources/all-entities/charges).
