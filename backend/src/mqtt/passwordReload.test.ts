import assert from 'node:assert/strict';
import { mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setImmediate } from 'node:timers/promises';
import { Store, type MqttClient } from 'mqtt';
import { pollMqttPassword, readMqttPassword, reconnectMqttTransport } from './passwordReload.js';

test('a broker-side auth rejection can wake the existing client without replacing its stores', () => {
  let destroyed = false;
  const transport = { get destroyed() { return destroyed; }, destroy: () => { destroyed = true; } };
  const stores = { incomingStore: new Store(), outgoingStore: new Store() };
  const reconnect = (options: typeof stores) => {
    assert.deepEqual(options, stores);
    assert.strictEqual(options.incomingStore, stores.incomingStore);
    assert.strictEqual(options.outgoingStore, stores.outgoingStore);
    reconnects++;
    return client as MqttClient;
  };
  const client = { stream: transport as MqttClient['stream'], ...stores, reconnect: reconnect as MqttClient['reconnect'] };
  let reconnects = 0;
  reconnectMqttTransport(client);
  assert.equal(destroyed, true);
  assert.equal(reconnects, 0);
  reconnectMqttTransport(client);
  assert.equal(reconnects, 1);
});

test('a mounted password is read after atomic replacement and preserves whitespace', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'mqtt-password-fixture-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'password');
  const next = path.join(directory, 'next');
  assert.equal(await readMqttPassword(file), null);
  await writeFile(file, ' synthetic first ');
  assert.equal(await readMqttPassword(file), ' synthetic first ');
  await writeFile(next, ' synthetic replacement \r\n');
  await rename(next, file);
  assert.equal(await readMqttPassword(file), ' synthetic replacement ');
  for (const malformed of ['', 'first\nsecond', 'x'.repeat(1025)]) {
    await writeFile(file, malformed);
    await assert.rejects(readMqttPassword(file), /INVALID_MQTT_PASSWORD_FILE/);
  }
});

test('unverified and removed passwords preserve the active transport; verified updates apply once', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const controller = new AbortController();
  let candidate: string | null = 'rejected';
  let current = 'bootstrap';
  let failures = 0;
  const transitions: string[] = [];
  const stop = pollMqttPassword({
    signal: controller.signal,
    read: async () => candidate,
    current: () => current,
    verify: async (password) => { if (password === 'rejected') throw new Error('not authorized'); },
    rotate: async (password) => { current = password; transitions.push(password); },
    rejected: () => { failures++; },
  });
  t.after(async () => { controller.abort(); await stop(); });
  const tick = async (delay: number) => { t.mock.timers.tick(delay); await setImmediate(); };

  await tick(10_000);
  assert.equal(current, 'bootstrap');
  assert.equal(failures, 1);
  await tick(30_000);
  assert.equal(failures, 1);

  candidate = 'verified';
  await tick(30_000);
  assert.equal(current, 'verified');
  await tick(10_000);
  candidate = null;
  await tick(10_000);
  assert.deepEqual(transitions, ['verified']);
});
