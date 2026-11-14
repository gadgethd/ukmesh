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
    assert.equal((await post('/failure', {status: 429})).status, 204);
    assert.equal((await post('/tag', {state: 'owned fixture'})).status, 429);
    assert.equal((await post('/failure', {status: 0})).status, 204);
    const classification = await (await post('/tag', {state: 'owned fixture'})).json();
    assert.equal(classification.answers.kind.choice, 'test-ping');
    const probe = 'a'.repeat(32);
    assert.equal((await post('/alerts', {content: 'OwnedFullStackProbe-' + probe})).status, 204);
    const counts = await (await fetch(base + '/counts')).json();
    assert.equal(counts.tags, 1);
    assert.equal(counts.failures, 1);
    assert.equal(counts.lastAlertProbe, probe);
  } finally {
    output.close();
    child.kill('SIGTERM');
    await once(child, 'exit');
  }
});
