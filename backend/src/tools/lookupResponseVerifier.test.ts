import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { Worker } from 'node:worker_threads';
import test from 'node:test';
import { LOOKUP_RESPONSE_MAX_BYTES } from './lookupResponseBody.js';
import { LookupResponseVerifier } from './lookupResponseVerifier.js';

const spec = { status: 200, expectedStatus: 200, populated: true };
const hash = (value: string) => createHash('sha256').update(value).digest('hex');

test('real worker retains exact UTF-8 response slice, populated count and raw/normalized hashes', async () => {
  const verifier = new LookupResponseVerifier();
  const raw = '{"nodes":[{"name":"North ☀️"}],"page":{"snapshot":"2026-11-14T00:00:00Z","nextCursor":"cursor"}}';
  const normalized = '{"nodes":[{"name":"North ☀️"}],"page":{"snapshot":"[request snapshot]","nextCursor":"cursor"}}';
  const slab = Buffer.from('unrelated-prefix' + raw + 'unrelated-suffix');
  const body = slab.subarray(Buffer.byteLength('unrelated-prefix'), slab.byteLength - Buffer.byteLength('unrelated-suffix'));
  try {
    const result = await verifier.verify(body, spec);
    assert.deepEqual(result, { entries: 1, digest: hash(raw), dataDigest: hash(normalized) });
    assert.equal(slab.toString().startsWith('unrelated-prefix'), true, 'shared backing bytes must remain intact');
  } finally { await verifier.close(); }
});

test('malformed, oversize, wrong-status and empty populated responses fail without losing later validation', async () => {
  const verifier = new LookupResponseVerifier();
  try {
    await assert.rejects(verifier.verify(Buffer.from('{broken'), spec), /not valid JSON/);
    await assert.rejects(verifier.verify(Buffer.alloc(LOOKUP_RESPONSE_MAX_BYTES + 1), spec), /byte budget/);
    await assert.rejects(verifier.verify(Buffer.from('{}'), { ...spec, status: 429 }), /HTTP 429/);
    for (const raw of ['[]', '{"nodes":[]}', 'null']) await assert.rejects(verifier.verify(Buffer.from(raw), spec), /empty fixture/);
    await assert.rejects(verifier.verify(Buffer.from('null'), { ...spec, populated: false }), /empty fixture/);
    const missing = await verifier.verify(Buffer.from('{"error":"missing"}'), { status: 404, expectedStatus: 404, populated: false });
    assert.equal(missing.entries, null);
  } finally { await verifier.close(); }
});

test('a valid response at the existing8MiB limit survives bounded worker validation', async () => {
  const verifier = new LookupResponseVerifier();
  const raw = '"' + 'x'.repeat(LOOKUP_RESPONSE_MAX_BYTES - 2) + '"';
  const expected = hash(raw);
  const body = Buffer.from(raw);
  try {
    const result = await verifier.verify(body, { ...spec, populated: false });
    assert.deepEqual(result, { entries: null, digest: expected, dataDigest: expected });
    assert.equal(body.byteLength, 0, 'standalone buffers transfer without retaining a second response copy');
  } finally { await verifier.close(); }
});

test('a silent worker deadline rejects all pending responses and terminates the owned helper', async t => {
  const worker = new Worker("require('node:worker_threads').parentPort.on('message',()=>{});", { eval: true });
  const realSetTimeout = globalThis.setTimeout;
  let expire: (() => void) | undefined;
  t.mock.method(globalThis, 'setTimeout', (callback: () => void, milliseconds: number) => {
    if (milliseconds === 30_000 && !expire) expire = callback;
    return realSetTimeout(callback, milliseconds);
  });
  const verifier = new LookupResponseVerifier(worker);
  const pending = Array.from({ length: 3 }, () => verifier.verify(Buffer.from('[1]'), spec).then(
    () => 'unexpected success', error => (error as Error).message));
  try {
    assert.ok(expire); expire();
    assert.deepEqual(await Promise.all(pending), Array(3).fill('lookup response validation deadline exceeded'));
  } finally { await verifier.close(); }
  assert.equal(worker.threadId, -1);
});

test('at most eight queued validations and idempotent close rejects every outstanding job', async () => {
  const worker = new Worker("require('node:worker_threads').parentPort.on('message',()=>{});", { eval: true });
  const verifier = new LookupResponseVerifier(worker);
  const pending = Array.from({ length: 8 }, () => verifier.verify(Buffer.from('[1]'), spec).then(() => false, () => true));
  await assert.rejects(verifier.verify(Buffer.from('[1]'), spec), /queue is full/);
  await verifier.close(); await verifier.close();
  assert.deepEqual(await Promise.all(pending), Array(8).fill(true));
  assert.equal(worker.threadId, -1);
  await assert.rejects(verifier.verify(Buffer.from('[1]'), spec), /closed/);
});

test('malformed worker replies fail under the owner and reject all queued responses', async () => {
  const worker = new Worker("const {parentPort}=require('node:worker_threads');parentPort.once('message',()=>parentPort.postMessage(null));", { eval: true });
  const verifier = new LookupResponseVerifier(worker);
  const pending = Array.from({ length: 3 }, () => verifier.verify(Buffer.from('[1]'), spec).then(
    () => 'unexpected success', error => (error as Error).message));
  try { assert.deepEqual(await Promise.all(pending), Array(3).fill('lookup response worker returned a malformed reply')); }
  finally { await verifier.close(); }
  assert.equal(worker.threadId, -1);
});

test('startup failures and unexpected clean/crash exits reject jobs and leave no worker', {timeout:10000}, async () => {
  for (const spawn of [() => new Worker(new URL('./missing-lookup-worker.js', import.meta.url)),
    () => new Worker('process.exit(0)', { eval: true }), () => new Worker('throw new Error("injected crash")', { eval: true })]) {
    const worker=spawn();
    const verifier = new LookupResponseVerifier(worker);
    try { await assert.rejects(verifier.verify(Buffer.from('[1]'), spec), /worker/); }
    finally { await verifier.close(); }
    assert.equal(worker.threadId, -1);
  }
});
