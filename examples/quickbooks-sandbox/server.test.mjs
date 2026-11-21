import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { createApp } from './server.mjs'

test('sandbox authorization, storage, API examples and refresh', async (t) => {
  const tokenDir = await mkdtemp(join(tmpdir(), 'quickbooks-test-'))
  t.after(() => rm(tokenDir, { recursive: true, force: true }))
  const calls = []
  let refreshCount = 0
  const { handler, redirect } = createApp({
    tokenDir,
    env: {
      INTUIT_CLIENT_ID: 'test-client',
      INTUIT_CLIENT_SECRET: 'fixture-secret',
      INTUIT_SANDBOX_PAYMENT_TOKEN: 'fixture-payment-token',
    },
    async fetchRequest(url, options) {
      calls.push({ url, options })
      if (url.includes('/tokens/bearer')) {
        const grant = options.body
        assert.equal(options.headers.Authorization, 'Basic ' + Buffer.from('test-client:fixture-secret').toString('base64'))
        if (grant.get('grant_type') === 'authorization_code') {
          assert.equal(grant.get('code'), 'fixture-code')
          assert.equal(grant.get('redirect_uri'), redirect.href)
        } else {
          assert.equal(grant.get('grant_type'), 'refresh_token')
          assert.equal(grant.get('refresh_token'), 'refresh-' + refreshCount)
          refreshCount++
        }
        return Response.json({
          access_token: 'access-' + refreshCount, refresh_token: 'refresh-' + refreshCount, expires_in: 3600,
        })
      }
      assert.equal(options.headers.Authorization, 'Bearer access-' + refreshCount)
      if (url.endsWith('/userinfo')) return Response.json({ sub: 'fixture-user' })
      if (url.endsWith('/charges')) {
        assert.match(options.headers['Request-Id'], /^[a-f\d-]{36}$/)
        assert.equal(JSON.parse(options.body).token, 'fixture-payment-token')
        return Response.json({ id: 'fixture-charge', status: 'CAPTURED' }, { status: 201 })
      }
      assert.equal(url, 'https://sandbox-quickbooks.api.intuit.com/v3/company/123/companyinfo/123')
      return Response.json({ CompanyInfo: { CompanyName: 'Sandbox' } })
    },
  })

  async function request(url, headers = {}, method = 'GET') {
    const result = { headers: {} }
    await handler({ url, method, headers: { host: redirect.host, ...headers } }, {
      setHeader(name, value) { result.headers[name] = value },
      writeHead(status, extra) { Object.assign(result.headers, extra); result.status = status },
      end(body) { if (body) result.body = JSON.parse(body) },
    })
    return result
  }

  assert.equal((await request('/company-info')).status, 409)
  const start = await request('/connect')
  const authorize = new URL(start.headers.Location)
  const state = authorize.searchParams.get('state')
  assert.equal(authorize.origin, 'https://appcenter.intuit.com')
  assert.equal(authorize.searchParams.get('response_type'), 'code')
  assert.match(state, /^[a-f0-9]{64}$/)
  const callback = '/oauth/callback?realmId=123&code=fixture-code&state=' + state
  assert.equal((await request(callback)).status, 400)
  const cookie = start.headers['Set-Cookie'].split(';')[0]
  assert.equal((await request(callback, { cookie })).headers.Location, '/company-info')
  assert.equal((await request(callback, { cookie })).status, 400)
  const tokenFile = join(tokenDir, 'quickbooks.json')
  assert.equal((await stat(tokenFile)).mode & 0o777, 0o600)
  assert.equal((await request('/company-info')).body.CompanyInfo.CompanyName, 'Sandbox')
  assert.equal((await request('/user-info')).body.sub, 'fixture-user')

  const saved = JSON.parse(await readFile(tokenFile, 'utf8'))
  await writeFile(tokenFile, JSON.stringify({ ...saved, expiresAt: 0 }))
  await Promise.all([request('/company-info'), request('/user-info')])
  assert.equal(refreshCount, 1)
  const action = { 'x-local-action': '1' }
  assert.equal((await request('/refresh', action, 'POST')).status, 200)
  assert.equal(JSON.parse(await readFile(tokenFile, 'utf8')).refreshToken, 'refresh-2')
  assert.equal((await request('/test-charge', {}, 'POST')).status, 403)
  assert.equal((await request('/test-charge', action, 'POST')).body.id, 'fixture-charge')
  assert.equal(calls.filter(({ url }) => url.endsWith('/charges')).length, 1)
})
