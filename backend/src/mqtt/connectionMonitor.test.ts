import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

test('audit log permission failures retry quietly and report recovery without a restart', async (t) => {
  // No DB calls: the fake readable log has zero bytes. The URL only permits
  // constructing the owner pool when importing the monitor.
  const previousUrl = process.env['DATABASE_URL'];
  process.env['DATABASE_URL'] = 'postgresql://test@127.0.0.1:1/test';
  const { startMqttConnectionMonitor, stopMqttConnectionMonitor } = await import('./connectionMonitor.js');
  const warnings: string[] = [];
  const messages: string[] = [];
  let readable = false;
  let attempts = 0;
  t.mock.method(console, 'warn', (message: string) => warnings.push(message));
  t.mock.method(console, 'log', (message: string) => messages.push(message));
  t.mock.method(fs, 'statSync', () => {
    attempts += 1;
    if (!readable) throw Object.assign(new Error('permission denied'), { code: 'EACCES' });
    return { size: 0 } as fs.Stats;
  });
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
  const settle = async () => { for (let i = 0; i < 8; i += 1) await Promise.resolve(); };
  try {
    startMqttConnectionMonitor();
    await settle();
    for (let i = 0; i < 3; i += 1) { t.mock.timers.tick(30_000); await settle(); }
    assert.equal(attempts, 4);
    assert.equal(warnings.length, 1);
    assert.match(warnings[0]!, /EACCES.*retrying/);
    readable = true;
    t.mock.timers.tick(30_000);
    await settle();
    assert.equal(messages.filter(message => /access recovered/.test(message)).length, 1);
    readable = false;
    t.mock.timers.tick(2_000);
    await settle();
    assert.equal(warnings.length, 2, 'a new outage must still be visible');
  } finally {
    await stopMqttConnectionMonitor();
    if (previousUrl === undefined) delete process.env['DATABASE_URL'];
    else process.env['DATABASE_URL'] = previousUrl;
  }
});
