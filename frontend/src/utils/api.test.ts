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
