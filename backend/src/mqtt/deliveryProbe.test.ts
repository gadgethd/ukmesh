import assert from 'node:assert/strict';
import test from 'node:test';
import { createDeliveryProbe } from './deliveryProbe.js';
import { withMqttDeliveryHealth } from '../health/mqttDelivery.js';
import net from 'node:net';
import { once } from 'node:events';
import mqtt from 'mqtt';
import mqttPacket from 'mqtt-packet';
import { installDurableMqttIntake } from './durableIntake.js';

test('flowing QoS 0 cannot hide a stalled QoS 1 session; recovery preserves the session and is rate limited', () => {
  let time = 1_000_000;
  let reconnects = 0;
  const published: string[] = [];
  const probe = createDeliveryProbe({ topic: 'owned/probe', now: () => time,
    nonce: () => `nonce-${time}`, publish: nonce => { published.push(nonce); },
    reconnect: () => { reconnects++; }, changed: () => {} });
  probe.connect();
  assert.equal(published.length, 1);
  assert.equal(probe.accept({ topic: 'packets', payload: 'traffic', qos: 0 }), false);
  probe.accept({ topic: 'owned/probe', payload: published[0]!, qos: 0 });
  probe.accept({ topic: 'owned/probe', payload: published[0]!, qos: 1, retain: true });
  probe.accept({ topic: 'owned/probe', payload: 'wrong nonce', qos: 1 });
  assert.equal(probe.status().state, 'starting');
  time += 60_000;
  probe.tick();
  assert.equal(probe.status().state, 'stalled');
  assert.equal(reconnects, 1);
  probe.disconnect();
  probe.connect();
  probe.accept({ topic: 'owned/probe', payload: published[0]!, qos: 1 });
  assert.equal(probe.status().state, 'stalled', 'a previous session probe cannot clear the failure');
  time += 60_000;
  probe.tick();
  assert.equal(reconnects, 1, 'no reconnect loop during a broker outage');
  time += 240_000;
  probe.tick();
  assert.equal(reconnects, 2);
  probe.disconnect();
  probe.connect();
  probe.accept({ topic: 'owned/probe', payload: published.at(-1)!, qos: 1 });
  assert.equal(probe.status().state, 'healthy');
  assert.equal(probe.status().lastSuccessAt, new Date(time).toISOString());
  time += 30_000;
  probe.tick();
  assert.equal(published.length, 4, 'healthy sessions continue being probed');
});

test('a stalled delivery path overrides a healthy cached overview and clears on recovery', () => {
  const overview = { status: 'healthy', incidents: [], components: { ingest: { status: 'ok' } } };
  const health = { state: 'stalled' as const, lastSuccessAt: null, recoveries: 1 };
  const failed = withMqttDeliveryHealth(overview, health);
  assert.equal(failed.status, 'critical');
  assert.equal(failed.components.ingest.status, 'stalled');
  assert.equal(failed.incidents[0]?.code, 'mqtt_delivery_stalled');
  assert.equal(overview.status, 'healthy', 'never mutate the cached health snapshot');
  assert.equal(withMqttDeliveryHealth(overview, { ...health, state: 'healthy' }), overview);
});

test('wire: a broker delivering QoS 0 but withholding QoS 1 causes a persistent reconnect and recovers', async t => {
  let connections = 0;
  let ordinaryPackets = 0;
  const sockets = new Set<net.Socket>();
  const states: string[] = [];
  const server = net.createServer(socket => {
    sockets.add(socket);
    socket.on('error', error => {
      if (!['EPIPE', 'ECONNRESET'].includes((error as NodeJS.ErrnoException).code ?? '')) throw error;
    });
    const connection = ++connections;
    const parser = mqttPacket.parser();
    socket.on('data', bytes => parser.parse(bytes));
    parser.on('packet', packet => {
      if (packet.cmd === 'connect') {
        assert.equal(packet.clean, false);
        socket.write(mqttPacket.generate({ cmd: 'connack', returnCode: 0, sessionPresent: true }));
        socket.write(mqttPacket.generate({ cmd: 'publish', topic: 'ordinary/packets',
          qos: 0, payload: 'flowing', retain: false, dup: false }));
      }
      if (packet.cmd === 'publish' && packet.qos === 1) {
        socket.write(mqttPacket.generate({ cmd: 'puback', messageId: packet.messageId }));
        if (connection > 1) socket.write(mqttPacket.generate({ cmd: 'publish', topic: packet.topic,
          qos: 1, messageId: 1, payload: packet.payload, retain: false, dup: false }));
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const client = mqtt.connect(`mqtt://127.0.0.1:${(server.address() as net.AddressInfo).port}`,
    { clean: false, clientId: 'persistent-probe-fixture', reconnectPeriod: 20 });
  const errors: string[] = [];
  client.on('error', e => errors.push(e.message));
  const probe = createDeliveryProbe({ topic: 'owned/probe', timeoutMs: 60, intervalMs: 30,
    recoveryCooldownMs: 100, publish: nonce => { client.publish('owned/probe', nonce, { qos: 1 }); },
    reconnect: () => { client.stream.destroy(); }, changed: health => states.push(health.state) });
  installDurableMqttIntake(client, async packet => { if (!probe.accept(packet)) ordinaryPackets++; },
    e => errors.push(e.message));
  client.on('connect', probe.connect);
  client.on('close', probe.disconnect);
  const timer = setInterval(probe.tick, 5);
  t.after(async () => {
    clearInterval(timer);
    await client.endAsync(true);
    for (const socket of sockets) socket.destroy();
    server.close();
  });
  const deadline = Date.now() + 5_000;
  while (probe.status().state !== 'healthy' && Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  assert.equal(probe.status().state, 'healthy');
  assert.ok(states.includes('stalled'));
  assert.ok(connections >= 2);
  assert.ok(ordinaryPackets >= 2);
  assert.deepEqual(errors, []);
});
