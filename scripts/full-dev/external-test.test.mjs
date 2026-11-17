import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import test from 'node:test';

test('local providers survive malformed input and preserve explicit failure/recovery and forwarding proof', {timeout: 10000}, async () => {
  const child = spawn(process.execPath, [new URL('./external-test.mjs', import.meta.url).pathname], {
    env: {PORT: '0'}, stdio: ['ignore', 'pipe', 'pipe'],
  });
  const output = createInterface({input: child.stdout});
  try {
    const [line] = await once(output, 'line');
    const base = `http://127.0.0.1:${JSON.parse(line).listening}`;
    const post = (route, body) => fetch(base + route, {method: 'POST', body: typeof body === 'string' ? body : JSON.stringify(body)});
    assert.equal((await post('/failure', '{broken')).status, 400);
    assert.equal((await fetch(base + '/healthz')).status, 200);
    for (const delayMs of [-1, 2001, 1.5, '500']) {
      assert.equal((await post('/failure', {status: 0, delayMs})).status, 400);
    }
    assert.equal((await post('/failure', {status: 429})).status, 204);
    assert.equal((await post('/tag', {state: 'owned fixture'})).status, 429);
    assert.equal((await post('/alerts', {content: 'MeshIngestSilent'})).status, 204,
      'tagger faults must preserve unrelated alert delivery');
    assert.equal((await post('/failure', {status: 0})).status, 204);
    const classification = await (await post('/tag', {state: 'owned fixture'})).json();
    assert.equal(classification.answers.kind.choice, 'test-ping');
    assert.equal((await post('/failure', {status: 0, delayMs: 100})).status, 204);
    const started = performance.now();
    assert.equal((await post('/tag', {})).status, 200);
    assert.ok(performance.now() - started >= 90, 'provider delay must affect the real response');
    assert.equal((await post('/failure', {status: 0})).status, 204);
    const probe = 'a'.repeat(32);
    assert.equal((await post('/alerts', {content: 'OwnedFullStackProbe-' + probe})).status, 204);
    const counts = await (await fetch(base + '/counts')).json();
    assert.equal(counts.tags, 2);
    assert.equal(counts.failures, 1);
    assert.equal(counts.lastAlertProbe, probe);
    for (const value of [
      {status: 503}, {status: 503, route: '/alerts'},
      {status: 503, route: '/alerts', alertProbe: probe, delayMs: 1},
      {status: 503, route: '/alerts', alertProbe: probe + 'a'},
      {status: 503, route: '/alerts', alertProbe: probe.toUpperCase()},
      {status: 429, route: '/alerts', alertProbe: probe},
      {status: 0, route: '/all'}, null,
    ]) assert.equal((await post('/failure', JSON.stringify(value))).status, 400);
    assert.equal((await post('/failure', {status: 503, route: '/alerts', alertProbe: probe})).status, 204);
    assert.equal((await post('/tag', {})).status, 200);
    assert.equal((await post('/alerts', {content: 'MeshIngestSilent'})).status, 204);
    assert.equal((await post('/alerts', {content: 'OwnedFullStackProbe-' + 'b'.repeat(32)})).status, 204);
    for (const content of ['OwnedFullStackProbe-' + probe + 'a', 'OwnedFullStackProbe-' + probe + 'g',
      'OtherOwnedFullStackProbe-' + probe, 'OwnedFullStackProbe-' + probe + '-other']) {
      assert.equal((await post('/alerts', {content})).status, 204,
        'a longer alert name must not match the owned fault');
    }
    assert.equal((await post('/alerts', {content: 'OwnedFullStackProbe-' + probe})).status, 503);
    const failed = await (await fetch(base + '/counts')).json();
    assert.deepEqual(failed.failedAlertProbes, [probe]);
    assert.equal(failed.failures, 2);
    assert.equal((await post('/failure', {status: 0})).status, 204);
    assert.equal((await post('/alerts', {content: 'OwnedFullStackProbe-' + probe})).status, 204);
  } finally {
    output.close();
    child.kill('SIGTERM');
    await once(child, 'exit');
  }
});
