import assert from 'node:assert/strict';
import test from 'node:test';
import { ApiResponseError } from './api.js';
import { createPathResolutionFetcher } from './pathResolutionRequest.js';

const isLimited = (error: unknown) => error instanceof ApiResponseError && error.status === 429;

test('path HTTP attempts, including pending polls and detail requests, share a rolling 30/minute cap', async () => {
  let calls = 0;
  let time = 0;
  const request = createPathResolutionFetcher(async () => {
    calls += 1;
    return new Response(null, { status: calls % 2 ? 202 : 200 });
  }, () => time);
  for (let i = 0; i < 30; i += 1) {
    await request(i % 2 ? '/api/path-beta/resolve-multi?hash=detail' : '/api/path-beta/resolve-multi?mode=slow');
  }
  await assert.rejects(request('/api/path-beta/resolve-multi?hash=next'), isLimited);
  assert.equal(calls, 30);
  time = 59_999;
  await assert.rejects(request('/api/path-beta/resolve-multi'), isLimited);
  time = 60_000;
  await request('/api/path-beta/resolve-multi');
  assert.equal(calls, 31);
});

test('429 pauses requests across packet changes, honors Retry-After and increases the cooldown', async () => {
  let calls = 0;
  let time = 0;
  const request = createPathResolutionFetcher(async () => {
    calls += 1;
    return new Response(null, { status: 429, headers: { 'Retry-After': '90' } });
  }, () => time);
  assert.equal((await request('/first')).status, 429);
  for (let i = 0; i < 100; i += 1) await assert.rejects(request(`/packet-${i}`), isLimited);
  time = 89_999;
  await assert.rejects(request('/early'), isLimited);
  assert.equal(calls, 1);
  time = 90_000;
  await request('/second');
  time = 209_999;
  await assert.rejects(request('/early-again'), isLimited);
  time = 210_000;
  await request('/third');
  assert.equal(calls, 3);
});

test('path requests have bounded concurrency and release capacity after failures', async () => {
  const pending: Array<{ resolve: (response: Response) => void; reject: (error: Error) => void }> = [];
  const request = createPathResolutionFetcher(() => new Promise((resolve, reject) => {
    pending.push({ resolve, reject });
  }));
  const first = request('/first');
  const second = request('/second');
  await assert.rejects(request('/third'), isLimited);
  pending[0].reject(new Error('offline'));
  await assert.rejects(first, /offline/);
  const third = request('/third');
  assert.equal(pending.length, 3);
  pending[1].resolve(new Response(null, { status: 200 }));
  pending[2].resolve(new Response(null, { status: 200 }));
  await Promise.all([second, third]);
});

test('path backoff honors HTTP-date Retry-After and retains caller cancellation', async () => {
  let time = Date.UTC(2026, 9, 28);
  const controller = new AbortController();
  const request = createPathResolutionFetcher(async (_input, init) => {
    assert.equal(new Headers(init?.headers).get('Accept'), 'application/json');
    controller.abort();
    assert.equal(init?.signal?.aborted, true);
    return new Response(null, {
      status: 429, headers: { 'Retry-After': new Date(time + 180_000).toUTCString() },
    });
  }, () => time);
  await request('/first', { signal: controller.signal });
  time += 179_999;
  await assert.rejects(request('/early'), isLimited);
});

test('an older concurrent success cannot undo 429 backoff', async () => {
  let time = 0;
  const pending: Array<(response: Response) => void> = [];
  const request = createPathResolutionFetcher(() => new Promise((resolve) => {
    pending.push(resolve);
  }), () => time);
  const first = request('/first');
  const second = request('/second');
  pending[0](new Response(null, { status: 429 }));
  await first;
  pending[1](new Response(null, { status: 200 }));
  await second;
  time = 60_000;
  const third = request('/third');
  pending[2](new Response(null, { status: 429 }));
  await third;
  time = 120_000;
  await assert.rejects(request('/early'), isLimited);
});
