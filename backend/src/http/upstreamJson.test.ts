import assert from 'node:assert/strict';
import test from 'node:test';
import {
  fetchBoundedJson,
  UpstreamCircuit,
  UpstreamRequestError,
} from './upstreamJson.js';

const url = new URL('https://upstream.example.test/value');

function options(fetchFn: typeof fetch, circuit = new UpstreamCircuit(3, 1_000)) {
  return {
    connectTimeoutMs: 50,
    totalTimeoutMs: 100,
    maxResponseBytes: 64,
    circuit,
    fetchFn,
  };
}

test('bounded upstream JSON accepts only a small application/json response and disables redirects', async () => {
  let redirect: RequestRedirect | undefined;
  const fetchFn = (async (_input, init) => {
    redirect = init?.redirect;
    return new Response('{"ok":true}', {
      headers: { 'content-type': 'application/json; charset=utf-8' },
    });
  }) as typeof fetch;
  assert.deepEqual(
    await fetchBoundedJson(url, {}, options(fetchFn)),
    { ok: true },
  );
  assert.equal(redirect, 'error');
});

test('bounded upstream JSON rejects invalid content, oversized bodies, and invalid JSON', async () => {
  const invalidContent = (async () => new Response('{}', {
    headers: { 'content-type': 'text/html' },
  })) as typeof fetch;
  await assert.rejects(
    fetchBoundedJson(url, {}, options(invalidContent)),
    (error: unknown) => error instanceof UpstreamRequestError
      && error.code === 'INVALID_CONTENT_TYPE',
  );

  const oversized = (async () => new Response(`"${'x'.repeat(128)}"`, {
    headers: { 'content-type': 'application/json' },
  })) as typeof fetch;
  await assert.rejects(
    fetchBoundedJson(url, {}, options(oversized)),
    (error: unknown) => error instanceof UpstreamRequestError
      && error.code === 'RESPONSE_TOO_LARGE',
  );

  const invalidJson = (async () => new Response('{', {
    headers: { 'content-type': 'application/json' },
  })) as typeof fetch;
  await assert.rejects(
    fetchBoundedJson(url, {}, options(invalidJson)),
    (error: unknown) => error instanceof UpstreamRequestError
      && error.code === 'INVALID_JSON',
  );
});

test('upstream connect timeout aborts and repeated failures open the circuit', async () => {
  const slowFetch = ((_input: unknown, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
    init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true });
  })) as typeof fetch;
  await assert.rejects(
    fetchBoundedJson(url, {}, {
      ...options(slowFetch),
      connectTimeoutMs: 10,
    }),
    (error: unknown) => error instanceof UpstreamRequestError
      && error.code === 'CONNECT_TIMEOUT',
  );

  const circuit = new UpstreamCircuit(2, 10_000);
  const failedFetch = (async () => {
    throw new Error('offline');
  }) as typeof fetch;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    await assert.rejects(fetchBoundedJson(url, {}, options(failedFetch, circuit)));
  }
  await assert.rejects(
    fetchBoundedJson(url, {}, options(failedFetch, circuit)),
    (error: unknown) => error instanceof UpstreamRequestError
      && error.code === 'CIRCUIT_OPEN',
  );
});
