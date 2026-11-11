import assert from 'node:assert/strict';
import test from 'node:test';
import {
  ApiResponseError,
  checkedJson,
  withScopeParams,
} from './api.js';

test('scope parameters preserve existing queries', () => {
  assert.equal(
    withScopeParams('/api/items?limit=2', { network: 'ukmesh', observer: 'AA' }),
    '/api/items?limit=2&network=ukmesh&observer=AA',
  );
});

test('checked JSON rejects HTTP, malformed, oversized, and schema-invalid responses', async () => {
  await assert.rejects(
    checkedJson(new Response('no', { status: 503 })),
    (error: unknown) => error instanceof ApiResponseError && error.status === 503,
  );
  await assert.rejects(
    checkedJson(new Response('not-json', { status: 200 })),
    /not valid JSON/,
  );
  await assert.rejects(
    checkedJson(new Response(JSON.stringify({ value: '12345' }), { status: 200 }), { maxBytes: 4 }),
    /byte limit/,
  );
  await assert.rejects(
    checkedJson<{ value: number }>(
      new Response(JSON.stringify({ value: 'wrong' }), { status: 200 }),
      {
        validate: (value): value is { value: number } => (
          typeof value === 'object'
          && value !== null
          && typeof (value as { value?: unknown }).value === 'number'
        ),
      },
    ),
    /expected schema/,
  );
  assert.deepEqual(
    await checkedJson<{ value: number }>(new Response('{"value":3}', { status: 200 })),
    { value: 3 },
  );
});

test('JSON error messages require opt-in and are bounded while preserving the HTTP status', async () => {
  const response = () => new Response(JSON.stringify({ error: 'The broker denied the default topic root' }), {
    status: 403, headers: { 'content-type': 'application/json' },
  });
  await assert.rejects(checkedJson(response()), /Request failed \(403\)/);
  await assert.rejects(checkedJson(response(), { readErrorMessage: true }),
    (error: unknown) => error instanceof ApiResponseError && error.status === 403 && /broker denied/.test(error.message));
  for (const [body, headers] of [
    ['<html>proxy error</html>', { 'content-type': 'text/html' }],
    ['not-json', { 'content-type': 'application/json' }],
    [JSON.stringify({ error: { nested: 'invalid' } }), { 'content-type': 'application/json' }],
    [JSON.stringify({ error: 'x'.repeat(2_049) }), { 'content-type': 'application/json' }],
    [JSON.stringify({ error: 'x'.repeat(9_000) }), { 'content-type': 'application/json' }],
    [JSON.stringify({ error: 'too big' }), { 'content-type': 'application/json', 'content-length': '9999' }],
  ] as const) {
    await assert.rejects(checkedJson(new Response(body, { status: 403, headers }), { readErrorMessage: true }), /Request failed \(403\)/);
  }
});
