import assert from 'node:assert/strict';
import test from 'node:test';
import { LOOKUP_RESPONSE_MAX_BYTES, validateLookupResponse } from './lookupResponseBody.js';
import type { LookupResponseSpec } from './lookupResponseBody.js';
import { createLookupResponseMemo } from './lookupResponseMemo.js';

const spec = { status: 200, expectedStatus: 200, populated: true };
const bytes = (value: string): ArrayBuffer => Uint8Array.from(Buffer.from(value)).buffer;
const outcome = (fn: () => unknown) => { try { return { result: fn() }; } catch (error) { return { error: (error as Error).message }; } };
function equivalent(memo: ReturnType<typeof createLookupResponseMemo>, body: ArrayBuffer, context: LookupResponseSpec) {
  assert.deepEqual(outcome(() => memo.validate(body, context)), outcome(() => validateLookupResponse(body, context)));
}

test('repeated exact responses retain evidence while every response gets a fresh raw hash', () => {
  const memo = createLookupResponseMemo(), body = bytes('{"nodes":[{"name":"North ☀️"}],"page":{"snapshot":"one"}}');
  const first = memo.validate(body, spec); first.digest = 'caller mutation';
  for (let n = 0; n < 200; n++) equivalent(memo, body, spec);
  assert.deepEqual(memo.statistics(), { calls: 201, freshHashes: 201, hits: 200, misses: 1,
    evictions: 0, fullValidations: 1, errors: 0, entries: 1, maxEntries: 1 });
  const statistics = memo.statistics(); statistics.calls = 0;
  assert.equal(memo.statistics().calls, 201, 'returned counters cannot change the memo');
});

test('same raw body under changed status, expected status and populated contracts is independently checked', () => {
  const memo = createLookupResponseMemo(), body = bytes('{"nodes":[]}');
  const contexts = [{ ...spec, populated: false }, spec, { status: 404, expectedStatus: 404, populated: false },
    { status: 404, expectedStatus: 200, populated: false }, { status: 200, expectedStatus: 404, populated: false }];
  for (let repeat = 0; repeat < 2; repeat++) for (const context of contexts) equivalent(memo, body, context);
  assert.equal(memo.statistics().entries, 2); assert.equal(memo.statistics().errors, 6);
});

test('distinct snapshot bytes miss even when normalized data digests match', () => {
  const memo = createLookupResponseMemo();
  const a = memo.validate(bytes('{"nodes":[1],"page":{"snapshot":"one"}}'), spec);
  const b = memo.validate(bytes('{"nodes":[1],"page":{"snapshot":"two"}}'), spec);
  assert.equal(a.dataDigest, b.dataDigest); assert.notEqual(a.digest, b.digest);
  assert.equal(memo.statistics().fullValidations, 2); assert.equal(memo.statistics().hits, 0);
});

test('in-place one-byte changes cannot reuse evidence or mutate a retained result', () => {
  const memo = createLookupResponseMemo(), body = bytes('[1]');
  const one = memo.validate(body, spec); new Uint8Array(body)[1] = '2'.charCodeAt(0);
  const two = memo.validate(body, spec);
  assert.notEqual(one.digest, two.digest); equivalent(memo, body, spec);
  assert.equal(memo.statistics().fullValidations, 2); assert.equal(memo.statistics().hits, 1);
});

test('invalid JSON and empty values fail every time, while existing accepted JSON shapes remain accepted', () => {
  const memo = createLookupResponseMemo();
  for (let n = 0; n < 2; n++) for (const raw of ['{broken', 'null', '[]', '{"nodes":[]}']) equivalent(memo, bytes(raw), spec);
  assert.equal(memo.statistics().entries, 0); assert.equal(memo.statistics().errors, 8); assert.equal(memo.statistics().hits, 0);
  equivalent(memo, bytes('42'), spec);
});

test('a ninth distinct response evicts bounded derived evidence and a returning response is revalidated', () => {
  const memo = createLookupResponseMemo();
  for (let n = 0; n < 9; n++) equivalent(memo, bytes(`[${n}]`), spec);
  assert.equal(memo.statistics().entries, 8); assert.equal(memo.statistics().maxEntries, 8); assert.equal(memo.statistics().evictions, 1);
  equivalent(memo, bytes('[0]'), spec);
  assert.equal(memo.statistics().fullValidations, 10); assert.equal(memo.statistics().evictions, 2);
});

test('exact8MiB input and oversize rejection retain the original validator outcomes', () => {
  const memo = createLookupResponseMemo(), body = bytes('"' + 'x'.repeat(LOOKUP_RESPONSE_MAX_BYTES - 2) + '"');
  const context = { ...spec, populated: false };
  equivalent(memo, body, context); equivalent(memo, body, context);
  equivalent(memo, new ArrayBuffer(LOOKUP_RESPONSE_MAX_BYTES + 1), context);
  assert.equal(memo.statistics().freshHashes, 2); assert.equal(memo.statistics().hits, 1);
  assert.equal(memo.statistics().errors, 1); assert.equal(memo.statistics().entries, 1);
});
