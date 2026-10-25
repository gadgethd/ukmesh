import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { PassThrough } from 'node:stream';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { createMqttConnectionMonitor } from './connectionMonitor.js';
import { mqttConnectionMonitorErrorsTotal, mqttConnectionMonitorHealthy } from '../metrics.js';

const denied = `Denied PUBLISH from meshcore_A1A1A1A1A1A1_2 (d0, q0, r0, m0, 'meshcore/NCL/${'A1'.repeat(32)}/status')\n`;
const connected = "New client connected from 192.0.2.10:35816 as meshcore_A1A1A1A1A1A1_2 (p4, c1, k60, u'test.user').\n";
const eacces = () => Object.assign(new Error('permission denied'), { code: 'EACCES' });

function fixture(t: test.TestContext) {
  const cache = path.resolve('node_modules/.cache');
  fs.mkdirSync(cache, { recursive: true });
  const root = fs.mkdtempSync(path.join(cache, 'conn-monitor-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const logPath = path.join(root, 'mosquitto.log');
  fs.writeFileSync(logPath, '');
  t.mock.method(console, 'error', () => {});
  t.mock.method(console, 'log', () => {});
  return logPath;
}

test('startup EACCES is unhealthy and a scheduled retry recovers only after a successful read', async (t) => {
  const logPath = fixture(t);
  let inaccessible = true;
  const monitor = createMqttConnectionMonitor({
    logPath, retryIntervalMs: 10,
    files: { ...fs, statSync: ((...args: Parameters<typeof fs.statSync>) => {
      if (inaccessible) throw eacces();
      return fs.statSync(...args);
    }) as typeof fs.statSync },
  });
  t.after(() => monitor.stop());
  assert.equal(monitor.status().state, 'starting');
  await monitor.start();
  assert.equal(monitor.status().state, 'error');
  assert.equal(monitor.status().lastError, 'init:EACCES');
  assert.equal(monitor.status().lastSuccessfulPollAt, null);
  assert.equal((await mqttConnectionMonitorHealthy.get()).values[0].value, 0);
  inaccessible = false;
  const deadline = Date.now() + 2_000;
  while (monitor.status().state !== 'healthy' && Date.now() < deadline) await delay(10);
  assert.equal(monitor.status().state, 'healthy');
  assert.equal(monitor.status().lastError, null);
  assert.equal((await mqttConnectionMonitorHealthy.get()).values[0].value, 1);
});

test('poll EACCES on stat and stream both degrade health, retain the cursor, and recover', async (t) => {
  const logPath = fixture(t);
  let failure: 'stat' | 'stream' | null = null;
  const writes: string[] = [];
  const monitor = createMqttConnectionMonitor({
    logPath,
    recordObservation: async (username) => { writes.push(username); },
    files: {
      statSync: ((...args: Parameters<typeof fs.statSync>) => {
        if (failure === 'stat') throw eacces();
        return fs.statSync(...args);
      }) as typeof fs.statSync,
      createReadStream: (...args) => {
        if (failure !== 'stream') return fs.createReadStream(...args);
        const stream = new PassThrough();
        queueMicrotask(() => stream.destroy(eacces()));
        return stream as fs.ReadStream;
      },
    },
  });
  t.after(() => monitor.stop());
  await monitor.start();
  const successfulAt = monitor.status().lastSuccessfulPollAt;
  failure = 'stat';
  await monitor.pollOnce();
  assert.equal(monitor.status().lastError, 'poll:EACCES');
  assert.equal(monitor.status().lastSuccessfulPollAt, successfulAt);
  fs.appendFileSync(logPath, connected + denied);
  failure = 'stream';
  await monitor.pollOnce();
  assert.equal(monitor.status().state, 'error');
  assert.deepEqual(writes, []);
  failure = null;
  await monitor.pollOnce();
  assert.equal(monitor.status().state, 'healthy');
  assert.deepEqual(writes, ['test.user']);
  await monitor.pollOnce();
  assert.deepEqual(writes, ['test.user'], 'successful ranges must not replay');
  assert.ok((await mqttConnectionMonitorErrorsTotal.get()).values.some(
    (value) => value.labels.phase === 'poll' && value.value >= 2,
  ));
});

test('audit write failures stay unhealthy across idle polls until an audit write succeeds', async (t) => {
  const logPath = fixture(t);
  fs.writeFileSync(logPath, connected + denied);
  let rejectWrite = true;
  const monitor = createMqttConnectionMonitor({
    logPath,
    recordObservation: async () => { if (rejectWrite) throw eacces(); },
  });
  t.after(() => monitor.stop());
  await monitor.start();
  assert.equal(monitor.status().lastError, 'audit:EACCES');
  await monitor.pollOnce();
  assert.equal(monitor.status().state, 'error');
  rejectWrite = false;
  fs.appendFileSync(logPath, denied);
  await monitor.pollOnce();
  assert.equal(monitor.status().state, 'healthy');
});

test('a missing log and truncation are observable and stop cancels retries', async (t) => {
  const logPath = fixture(t);
  fs.unlinkSync(logPath);
  const monitor = createMqttConnectionMonitor({ logPath, retryIntervalMs: 10 });
  await monitor.start();
  assert.equal(monitor.status().lastError, 'init:ENOENT');
  await monitor.stop();
  fs.writeFileSync(logPath, '');
  await delay(25);
  assert.equal(monitor.status().state, 'stopped');
  const restarted = createMqttConnectionMonitor({ logPath });
  t.after(() => restarted.stop());
  await restarted.start();
  fs.writeFileSync(logPath, 'log entry\n');
  await restarted.pollOnce();
  fs.writeFileSync(logPath, '');
  await restarted.pollOnce();
  assert.equal(restarted.status().state, 'healthy');
});
