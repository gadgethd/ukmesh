import assert from 'node:assert/strict';
import test from 'node:test';
import { createWebVitalsReporter, publicVitalRoute } from './webVitals.js';

test('Web Vitals report only allowlisted initial routes and bounded final values without attribution', async () => {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'window');
  Object.defineProperty(globalThis, 'window', { configurable: true, value: { location: { search: '' } } });
  const calls: RequestInit[] = [];
  try {
    const report = createWebVitalsReporter('/docs', async (_input, init) => { calls.push(init!); return new Response(null, { status: 202 }); });
    const withAttribution = { name: 'LCP', value: 1234, id: 'invented-browser-id', entries: [{ url: 'private' }] };
    report(withAttribution);
    report({ name: 'LCP', value: 2000 });
    report({ name: 'CLS', value: 0 });
    for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY, 600_001]) report({ name: 'INP', value });
    report({ name: 'unbounded-label', value: 1 });
    assert.equal(calls.length, 2, 'there is no synthetic INP without an observed interaction metric');
    report({ name: 'INP', value: 96 });
    assert.equal(calls.length, 3);
    assert.deepEqual(calls.map(call => JSON.parse(String(call.body))), [
      { route: '/docs', metric: 'LCP', value: 1234 }, { route: '/docs', metric: 'CLS', value: 0 }, { route: '/docs', metric: 'INP', value: 96 },
    ]);
    assert.ok(calls.every(call => call.credentials === 'omit' && call.referrerPolicy === 'no-referrer'
      && call.cache === 'no-store' && call.keepalive && String(call.body).length < 128));
    let failures = 0;
    const failing = createWebVitalsReporter('/feed', () => { failures++; throw new Error('transport unavailable'); });
    assert.doesNotThrow(() => failing({ name: 'LCP', value: 100 }));
    failing({ name: 'LCP', value: 100 });
    assert.equal(failures, 1, 'transport failure cannot start a retry loop');
    (globalThis as unknown as { window: { location: { search: string } } }).window.location.search = '?telemetry=off';
    createWebVitalsReporter('/feed', async () => { throw new Error('must not send'); })({ name: 'CLS', value: 0 });
    assert.equal(publicVitalRoute('/regions/'), '/regions');
    for (const route of ['/login', '/node/invented', '/docs?secret=example', '/unknown']) assert.equal(publicVitalRoute(route), null);
    await Promise.resolve();
  } finally {
    if (previous) Object.defineProperty(globalThis, 'window', previous);
    else Reflect.deleteProperty(globalThis, 'window');
  }
});
