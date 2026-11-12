import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import type mqtt from 'mqtt';
import type { IClientOptions, IClientPublishOptions } from 'mqtt';
import { createMqttPublisher } from './mqttPublisher.js';

function fakeBroker(connectAutomatically = true) {
  const clients: Array<EventEmitter & { connected: boolean; closes: number }> = [];
  const connections: Array<{ url: string; options: IClientOptions }> = [];
  const publications: Array<{ topic: string; options: IClientPublishOptions }> = [];
  let failPublish = false;
  const connect = ((url: string, options: IClientOptions) => {
    connections.push({ url, options });
    const client = Object.assign(new EventEmitter(), {
      connected: false, closes: 0,
      async publishAsync(topic: string, _payload: string, publishOptions: IClientPublishOptions) {
        publications.push({ topic, options: publishOptions });
        if (failPublish) { failPublish = false; throw new Error('lost acknowledgment'); }
      },
      async endAsync() { this.end(); },
      end() { this.connected = false; this.closes++; this.emit('close'); },
    });
    clients.push(client);
    if (connectAutomatically) queueMicrotask(() => { client.connected = true; client.emit('connect'); });
    return client;
  }) as unknown as typeof mqtt.connect;
  return { connect, clients, connections, publications, failNextPublish() { failPublish = true; } };
}

test('MQTT publisher coalesces connects, forwards wire options and refreshes expiring credentials', async t => {
  let now = 1_000_000, credentialReads = 0;
  t.mock.method(Date, 'now', () => now);
  const broker = fakeBroker();
  const publisher = createMqttPublisher({
    connection() { credentialReads++; return { url: 'mqtt://owned.test', expiresAt: now / 1_000 + 120,
      options: { username: `credential-${credentialReads}`, clean: true, reconnectPeriod: 0 } }; },
    connectionLabel: 'destination MQTT', publishLabel: 'destination MQTT publish',
    publishOptions: { qos: 1 }, connectTimeoutMs: 1_000, publishTimeoutMs: 1_000,
    discardOnPublishFailure: true, logError: () => {}, connect: broker.connect,
  });
  await Promise.all([publisher.publish('first', 'one'), publisher.publish('second', 'two')]);
  assert.equal(credentialReads, 1);
  assert.equal(broker.clients.length, 1);
  assert.equal(broker.connections[0]?.options.username, 'credential-1');
  assert.deepEqual(broker.publications.map(p => p.options), [{ qos: 1 }, { qos: 1 }]);
  now += 61_000;
  await publisher.publish('renewed', 'three');
  assert.equal(credentialReads, 2);
  assert.equal(broker.clients[0]?.closes, 1);
  broker.failNextPublish();
  await assert.rejects(publisher.publish('lost', 'four'), /lost acknowledgment/);
  assert.equal(broker.clients[1]?.closes, 1);
  await publisher.publish('retry', 'five');
  assert.equal(credentialReads, 3);
  await publisher.close();
  assert.equal(broker.clients[2]?.closes, 1);
});

test('nonexpiring observation publisher keeps QoS 0/nonretained options and owned cleanup', async () => {
  const broker = fakeBroker();
  const publisher = createMqttPublisher({
    connection: () => ({ url: 'mqtt://owned.test', options: { username: 'public-publisher' } }),
    connectionLabel: 'public feed MQTT', publishLabel: 'public feed publish',
    publishOptions: { qos: 0, retain: false }, connectTimeoutMs: 1_000,
    logError: () => {}, connect: broker.connect,
  });
  await publisher.publish('public/owned/packets', 'one');
  await publisher.publish('public/owned/packets', 'two');
  assert.equal(broker.clients.length, 1);
  assert.deepEqual(broker.publications.map(p => p.options), [{ qos: 0, retain: false }, { qos: 0, retain: false }]);
  await publisher.close();
  assert.equal(broker.clients[0]?.closes, 1);
});

test('close during credential loading cannot create a late connection', async () => {
  const broker = fakeBroker();
  let credentialsReady!: (value: { url: string; options: IClientOptions }) => void;
  const publisher = createMqttPublisher({
    connection: () => new Promise(resolve => { credentialsReady = resolve; }),
    connectionLabel: 'destination MQTT', publishLabel: 'destination MQTT publish',
    publishOptions: { qos: 1 }, connectTimeoutMs: 1_000,
    logError: () => {}, connect: broker.connect,
  });
  const published = publisher.publish('owned', 'one');
  const rejected = assert.rejects(published, /connection closed before connect/);
  await publisher.close();
  credentialsReady({ url: 'mqtt://owned.test', options: {} });
  await rejected;
  assert.equal(broker.clients.length, 0);
});


test('a silent connection times out, closes and permits a fresh connection attempt', async () => {
  const broker = fakeBroker(false);
  const publisher = createMqttPublisher({
    connection: () => ({ url: 'mqtt://owned.test', options: {} }),
    connectionLabel: 'destination MQTT', publishLabel: 'destination MQTT publish',
    publishOptions: { qos: 1 }, connectTimeoutMs: 5,
    logError: () => {}, connect: broker.connect,
  });
  await assert.rejects(publisher.publish('owned', 'one'), /destination MQTT connection timed out/);
  assert.equal(broker.clients[0]?.closes, 1);
  const retry = publisher.publish('owned', 'two');
  await Promise.resolve();
  const connected = broker.clients[1]!;
  connected.connected = true;
  connected.emit('connect');
  await retry;
  assert.equal(broker.publications.length, 1);
  await publisher.close();
});
