import assert from 'node:assert/strict';
import test from 'node:test';
import net from 'node:net';
import { once } from 'node:events';
import mqttPacket from 'mqtt-packet';
import { deliveryProbeTopics } from './deliveryProbeTopics.js';
import { createDeliveryProbe } from './deliveryProbe.js';
import { connectPersistentMqtt, subscribePersistentTopics } from './persistentClient.js';
import { installDurableMqttIntake } from './durableIntake.js';

test('default delivery topic and subscription permissions stay exactly compatible', () => {
  const prefixes = new Set(['meshcore', 'ukmesh', 'meshcore-test']);
  const before = Array.from(prefixes);
  for (const configured of [undefined, '', '   ']) {
    assert.deepEqual(deliveryProbeTopics(prefixes, 'backend', configured), {
      topic: 'meshcore/_system/ingest/backend',
    });
  }
  assert.deepEqual(Array.from(prefixes), before);
  assert.deepEqual(deliveryProbeTopics(prefixes, 'backend', 'meshcore/controls'), {
    topic: 'meshcore/controls/_system/ingest/backend',
  });
});

test('a separate control prefix selects one exact topic without broadening business filters', () => {
  const prefixes = new Set(['meshcore', 'ukmesh', 'meshcore-test']);
  const selected = deliveryProbeTopics(prefixes, 'backend', 'ukmesh-local-control');
  assert.deepEqual(selected, {
    topic: 'ukmesh-local-control/_system/ingest/backend',
    subscription: 'ukmesh-local-control/_system/ingest/backend',
  });
  assert.ok(Array.from(prefixes).every(prefix => !selected.topic.startsWith(`${prefix}/`)));
  assert.deepEqual(Array.from(prefixes), ['meshcore', 'ukmesh', 'meshcore-test']);
  assert.equal(deliveryProbeTopics(new Set(['mesh']), 'backend', 'meshcore').subscription,
    'meshcore/_system/ingest/backend', 'coverage must end on an MQTT level boundary');
});

test('invalid control prefixes fail before any MQTT connection or publish', () => {
  for (const prefix of ['#', 'control/+', 'control\0hidden', '/control', 'control/', 'control//nested']) {
    assert.throws(() => deliveryProbeTopics(new Set(['meshcore']), 'backend', prefix),
      /MQTT_DELIVERY_PROBE_TOPIC_PREFIX_INVALID/);
  }
  assert.throws(() => deliveryProbeTopics(new Set(), 'backend', 'control'), /MQTT_TOPIC_PREFIXES_EMPTY/);
  assert.throws(() => deliveryProbeTopics(new Set(['meshcore']), 'wildcard+client'), /MQTT_DELIVERY_PROBE_TOPIC_INVALID/);
});

async function until(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!condition()) {
    assert.ok(Date.now() < deadline, 'wire condition missed its bounded deadline');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  assert.ok(Date.now() < deadline, 'late wire completion cannot pass');
}

for (const denied of [undefined, 'business', 'control'] as const) {
  test(`wire: separate control waits for every SUBACK and preserves business delivery (denied=${denied ?? 'none'})`,
    { timeout: 5_000 }, async t => {
      const selected = deliveryProbeTopics(new Set(['meshcore']), 'owned-persistent-backend', 'ukmesh-local-control');
      assert.ok(selected.subscription);
      const subscriptions: Array<{ topic: string; qos: number }> = [];
      const published: Array<{ topic: string; qos: number; retain: boolean }> = [];
      const business: string[] = [];
      const errors: string[] = [];
      const subscriptionFailures: Array<Error & { packet?: { granted?: number[] } }> = [];
      const sockets = new Set<net.Socket>();
      let releaseControl: (() => void) | undefined;
      let echoAcknowledged = false;
      let businessAcknowledged = false;
      let connectionCount = 0;
      const server = net.createServer(socket => {
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
        socket.on('error', error => {
          if (!['EPIPE', 'ECONNRESET'].includes((error as NodeJS.ErrnoException).code ?? '')) errors.push(error.message);
        });
        const parser = mqttPacket.parser();
        socket.on('data', bytes => parser.parse(bytes));
        parser.on('packet', packet => {
          if (packet.cmd === 'connect') {
            connectionCount++;
            assert.equal(packet.clean, false);
            socket.write(mqttPacket.generate({ cmd: 'connack', returnCode: 0, sessionPresent: true }));
          }
          if (packet.cmd === 'subscribe') {
            for (const subscription of packet.subscriptions) {
              subscriptions.push({ topic: subscription.topic, qos: subscription.qos });
            }
            const control = packet.subscriptions.some(subscription => subscription.topic === selected.topic);
            const acknowledge = () => socket.write(mqttPacket.generate({ cmd: 'suback', messageId: packet.messageId,
              granted: packet.subscriptions.map(() => (control ? denied === 'control' : denied === 'business') ? 128 : 1) }));
            if (control) releaseControl = acknowledge;
            else acknowledge();
          }
          if (packet.cmd === 'publish') {
            published.push({ topic: packet.topic, qos: packet.qos, retain: packet.retain });
            assert.equal(packet.topic, selected.topic);
            assert.equal(packet.qos, 1);
            assert.equal(packet.retain, false);
            if (packet.qos === 1) {
              socket.write(mqttPacket.generate({ cmd: 'puback', messageId: packet.messageId }));
              socket.write(mqttPacket.generate({ cmd: 'publish', topic: selected.topic, messageId: 7,
                payload: packet.payload, qos: 1, retain: false, dup: false }));
            }
          }
          if (packet.cmd === 'puback') {
            if (packet.messageId === 7) echoAcknowledged = true;
            if (packet.messageId === 8) businessAcknowledged = true;
          }
        });
      });
      server.listen(0, '127.0.0.1');
      await once(server, 'listening');
      const client = connectPersistentMqtt(`mqtt://127.0.0.1:${(server.address() as net.AddressInfo).port}`, {
        clientId: 'owned-persistent-backend', reconnectPeriod: 0,
      });
      client.on('error', error => errors.push(error.message));
      const probe = createDeliveryProbe({ topic: selected.topic,
        publish: nonce => { client.publish(selected.topic, nonce, { qos: 1, retain: false }); },
        reconnect: () => { assert.fail('this initial-subscription fixture must not reconnect'); }, changed: () => {} });
      const intake = installDurableMqttIntake(client, async packet => {
        if (!probe.accept(packet)) business.push(`${packet.topic}:${packet.payload.toString()}`);
      }, error => errors.push(error.message));
      client.on('connect', () => subscribePersistentTopics(client, ['meshcore/#', selected.subscription!], {
        ready: probe.connect,
        failed: error => { subscriptionFailures.push(error); errors.push(error.message); },
      }));
      t.after(async () => {
        intake.stopAdmission();
        await client.endAsync(true);
        await intake.drain();
        for (const socket of sockets) socket.destroy();
        await new Promise<void>(resolve => server.close(() => resolve()));
      });
      await until(() => releaseControl !== undefined);
      assert.equal(published.length, 0, 'no probe may publish before the exact control SUBACK');
      assert.equal(probe.status().state, 'starting');
      releaseControl!();
      if (denied) {
        await until(() => subscriptionFailures.length > 0);
        assert.equal(subscriptionFailures.length, 1);
        assert.deepEqual(subscriptionFailures[0]!.packet?.granted, [128], 'retain the actual MQTT SUBACK refusal');
        assert.deepEqual(errors, [subscriptionFailures[0]!.message]);
        assert.equal(published.length, 0);
        assert.equal(probe.status().state, 'starting', 'a denied business/control subscription cannot certify delivery');
      } else {
        await until(() => probe.status().state === 'healthy' && echoAcknowledged);
        const socket = Array.from(sockets)[0]!;
        socket.write(mqttPacket.generate({ cmd: 'publish', topic: 'meshcore/LHR/observer/packets', messageId: 8,
          payload: 'ordinary-business', qos: 1, retain: false, dup: false }));
        await until(() => businessAcknowledged);
        assert.deepEqual(business, ['meshcore/LHR/observer/packets:ordinary-business']);
        assert.deepEqual(errors, []);
        assert.deepEqual(published, [{ topic: selected.topic, qos: 1, retain: false }]);
      }
      assert.equal(connectionCount, 1, 'business and control must use the same persistent connection');
      assert.deepEqual(subscriptions, [{ topic: 'meshcore/#', qos: 1 }, { topic: selected.topic, qos: 1 }]);
    });
}
