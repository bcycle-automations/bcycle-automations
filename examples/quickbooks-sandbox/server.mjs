import { createServer } from 'node:http'
import { randomBytes, randomUUID } from 'node:crypto'
import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const authorizeUrl = 'https://appcenter.intuit.com/connect/oauth2'
const tokenUrl = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
const companyUrl = 'https://sandbox-quickbooks.api.intuit.com/v3/company'
const userInfoUrl = 'https://sandbox-accounts.platform.intuit.com/v1/openid_connect/userinfo'
const chargeUrl = 'https://sandbox.api.intuit.com/quickbooks/v4/payments/charges'
const refreshLeadTime = 60_000

class AppError extends Error {
  constructor(status, message, details = {}) {
    super(message)
    this.status = status
    this.details = details
  }
}

export function createApp({
  env = process.env,
  fetchRequest = fetch,
  tokenDir = fileURLToPath(new URL('./.tokens/', import.meta.url)),
} = {}) {
  const clientId = env.INTUIT_CLIENT_ID?.trim()
  const clientSecret = env.INTUIT_CLIENT_SECRET
  if (!clientId || !clientSecret?.trim()) {
    throw new Error('Set INTUIT_CLIENT_ID and INTUIT_CLIENT_SECRET in .env.')
  }
  const redirect = new URL(env.INTUIT_REDIRECT_URI || 'http://localhost:3000/oauth/callback')
  if (!['http:', 'https:'].includes(redirect.protocol) ||
      !['localhost', '127.0.0.1'].includes(redirect.hostname) ||
      redirect.username || redirect.password || redirect.search || redirect.hash ||
      redirect.pathname !== '/oauth/callback') {
    throw new Error('Set INTUIT_REDIRECT_URI to the local sandbox callback /oauth/callback.')
  }
  if (redirect.protocol !== 'http:') {
    throw new Error('This local example serves HTTP. Register the HTTP callback under Development.')
  }

  const scopes = env.INTUIT_SCOPES ||
    'com.intuit.quickbooks.accounting com.intuit.quickbooks.payment openid profile email phone address'
  const basic = Buffer.from(clientId + ':' + clientSecret).toString('base64')
  const tokenFile = join(tokenDir, 'quickbooks.json')
  let pendingAuth
  let queue = Promise.resolve()

  function serial(operation) {
    const result = queue.then(operation)
    queue = result.catch(() => {})
    return result
  }

  async function upstream(url, options, operation) {
    let response
    try {
      response = await fetchRequest(url, { ...options, redirect: 'error', signal: AbortSignal.timeout(15_000) })
    } catch {
      throw new AppError(502, 'Intuit request failed or timed out.', { operation })
    }

    const payload = await response.json().catch(() => null)
    if (!response.ok) {
      const safeIdentifier = (value) =>
        typeof value === 'string' && /^[A-Za-z0-9_-]{1,100}$/.test(value) ? value : undefined
      const code = safeIdentifier(payload?.error || payload?.Fault?.Error?.[0]?.code || payload?.errors?.[0]?.code)
      const message = code === 'invalid_grant'
        ? 'Intuit rejected the grant. Start again at /connect and verify the Development callback.'
        : 'Intuit rejected the request.'
      throw new AppError(response.status === 401 ? 401 : 502, message, {
        operation, intuitStatus: response.status, code,
        intuitTid: safeIdentifier(response.headers.get('intuit_tid')),
      })
    }
    if (!payload || typeof payload !== 'object') {
      throw new AppError(502, 'Intuit returned an invalid JSON response.', { operation })
    }
    return payload
  }

  async function storeGrant(realmId, fields) {
    const grant = await upstream(tokenUrl, {
      method: 'POST',
      headers: {
        Authorization: 'Basic ' + basic,
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: 'application/json',
      },
      body: new URLSearchParams(fields),
    }, fields.grant_type)

    if (typeof grant.access_token !== 'string' || !grant.access_token ||
        typeof grant.refresh_token !== 'string' || !grant.refresh_token ||
        !Number.isFinite(grant.expires_in) || grant.expires_in <= 0) {
      throw new AppError(502, 'Intuit returned incomplete token data.')
    }
    const saved = {
      clientId, realmId, accessToken: grant.access_token,
      refreshToken: grant.refresh_token, expiresAt: Date.now() + grant.expires_in * 1000,
    }
    await mkdir(tokenDir, { recursive: true, mode: 0o700 })
    const temporary = tokenFile + '.' + randomUUID()
    try {
      await writeFile(temporary, JSON.stringify(saved), { mode: 0o600, flag: 'wx' })
      await rename(temporary, tokenFile)
    } finally {
      await rm(temporary, { force: true })
    }
    return saved
  }

  function getGrant({ force = false, rejectedToken } = {}) {
    return serial(async () => {
      let saved
      try {
        saved = JSON.parse(await readFile(tokenFile, 'utf8'))
      } catch (error) {
        if (error.code === 'ENOENT') throw new AppError(409, 'Connect the sandbox company at /connect first.')
        throw new AppError(500, 'Could not read local token storage.')
      }
      if (saved?.clientId !== clientId || !/^\d+$/.test(saved.realmId) ||
          typeof saved.accessToken !== 'string' || typeof saved.refreshToken !== 'string' ||
          !Number.isFinite(saved.expiresAt)) {
        throw new AppError(409, 'Connect again at /connect to replace the saved grant.')
      }
      const expiresSoon = saved.expiresAt <= Date.now() + refreshLeadTime
      if (!force && !expiresSoon && saved.accessToken !== rejectedToken) return saved
      return storeGrant(saved.realmId, {
        grant_type: 'refresh_token', refresh_token: saved.refreshToken,
      })
    })
  }

  async function getExample(endpoint, name) {
    const grant = await getGrant()
    const read = (saved) => upstream(endpoint(saved), {
      headers: { Authorization: 'Bearer ' + saved.accessToken, Accept: 'application/json' },
    }, name)
    try {
      return await read(grant)
    } catch (error) {
      if (error.status !== 401) throw error
      return read(await getGrant({ rejectedToken: grant.accessToken }))
    }
  }

  function json(res, status, body) {
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' })
    res.end(JSON.stringify(body, null, 2) + '\n')
  }

  function move(res, location) {
    res.writeHead(303, { Location: location })
    res.end()
  }

  async function handler(req, res) {
    res.setHeader('Cache-Control', 'no-store')
    res.setHeader('Referrer-Policy', 'no-referrer')
    res.setHeader('X-Content-Type-Options', 'nosniff')
    try {
      if (req.headers.host !== redirect.host) throw new AppError(400, 'Use the configured local host.')
      const url = new URL(req.url, redirect.origin)
      const route = req.method + ' ' + url.pathname
      if (req.method === 'POST' &&
          (req.headers['x-local-action'] !== '1' || (req.headers.origin && req.headers.origin !== redirect.origin))) {
        throw new AppError(403, 'Send X-Local-Action: 1 from the local client.')
      }
      switch (route) {
        case 'GET /':
          return json(res, 200, {
            connect: '/connect', companyInfo: '/company-info', userInfo: '/user-info',
            refresh: 'POST /refresh', testCharge: 'POST /test-charge',
          })
        case 'GET /connect': {
          const state = randomBytes(32).toString('hex')
          pendingAuth = { state, expiresAt: Date.now() + 10 * 60_000 }
          res.setHeader('Set-Cookie', 'qb_state=' + state + '; HttpOnly; SameSite=Lax; Path=/oauth/callback; Max-Age=600')
          const authorize = new URL(authorizeUrl)
          authorize.search = new URLSearchParams({
            client_id: clientId, redirect_uri: redirect.href, response_type: 'code', scope: scopes, state,
          })
          return move(res, authorize.href)
        }
        case 'GET /oauth/callback': {
          const state = url.searchParams.get('state')
          const cookie = /(?:^|;\s*)qb_state=([a-f0-9]{64})(?:;|$)/.exec(req.headers.cookie || '')?.[1]
          if (!pendingAuth || pendingAuth.expiresAt < Date.now() || state !== pendingAuth.state || state !== cookie) {
            throw new AppError(400, 'OAuth state is invalid or expired. Start again at /connect.')
          }
          pendingAuth = undefined
          res.setHeader('Set-Cookie', 'qb_state=; HttpOnly; SameSite=Lax; Path=/oauth/callback; Max-Age=0')
          const code = url.searchParams.get('code')
          const realmId = url.searchParams.get('realmId')
          if (url.searchParams.has('error') || !code || !realmId || !/^\d+$/.test(realmId)) {
            throw new AppError(400, 'Authorization did not return a code and sandbox company. Start again at /connect.')
          }
          await serial(() => storeGrant(realmId, {
            grant_type: 'authorization_code', code, redirect_uri: redirect.href,
          }))
          return move(res, '/company-info')
        }
        case 'GET /company-info':
          return json(res, 200, await getExample(
            ({ realmId }) => companyUrl + '/' + realmId + '/companyinfo/' + realmId, 'company-info',
          ))
        case 'GET /user-info':
          return json(res, 200, await getExample(() => userInfoUrl, 'user-info'))
        case 'POST /refresh': {
          const saved = await getGrant({ force: true })
          return json(res, 200, { realmId: saved.realmId, expiresAt: new Date(saved.expiresAt).toISOString() })
        }
        case 'POST /test-charge': {
          const paymentToken = env.INTUIT_SANDBOX_PAYMENT_TOKEN?.trim()
          if (!paymentToken) throw new AppError(400, 'Set INTUIT_SANDBOX_PAYMENT_TOKEN to a fresh sandbox Payments token.')
          const grant = await getGrant()
          const requestId = randomUUID()
          try {
            const charge = await upstream(chargeUrl, {
              method: 'POST',
              headers: {
                Authorization: 'Bearer ' + grant.accessToken, Accept: 'application/json',
                'Content-Type': 'application/json', 'Request-Id': requestId,
              },
              body: JSON.stringify({
                amount: '1.00', currency: 'USD', token: paymentToken,
                capture: true, context: { mobile: false, isEcommerce: true },
              }),
            }, 'test-charge')
            return json(res, 201, { requestId, id: charge.id, status: charge.status, amount: charge.amount, currency: charge.currency })
          } catch (error) {
            if (error instanceof AppError) error.details.requestId = requestId
            throw error
          }
        }
        default:
          throw new AppError(404, 'Unknown route.')
      }
    } catch (error) {
      if (error instanceof AppError) return json(res, error.status, { error: error.message, ...error.details })
      return json(res, 500, { error: 'Local application error. Check token-directory permissions.' })
    }
  }

  return { handler, redirect }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const { handler, redirect } = createApp()
    const port = Number(redirect.port || 80)
    const server = createServer(handler)
    server.on('error', (error) => {
      console.error('Local server failed:', error.code || 'unknown error')
      process.exitCode = 1
    })
    server.listen(port, '127.0.0.1', () => console.log('Open ' + redirect.origin + '/connect'))
  } catch (error) {
    console.error(error instanceof TypeError ? 'Invalid local configuration.' : error.message)
    process.exitCode = 1
  }
}
