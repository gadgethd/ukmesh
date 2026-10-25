import assert from 'node:assert/strict';
import test from 'node:test';
import net from 'node:net';
import { once } from 'node:events';
import mqtt, { type MqttClient } from 'mqtt';
import mqttPacket from 'mqtt-packet';
import { installDurableMqttIntake } from './durableIntake.js';

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(check: () => boolean) {
  const deadline = Date.now() + 5_000;
  while (!check() && Date.now() < deadline) await pause(10);
  assert.ok(check(), 'condition did not settle');
}

test('wire: backlog processes keepalive responses while PUBACK waits for commit', async (t) => {
  let pings = 0;
  let acks = 0;
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    const parser = mqttPacket.parser();
    socket.on('data', (data) => parser.parse(data));
    parser.on('packet', (packet) => {
      if (packet.cmd === 'connect') {
        socket.write(mqttPacket.generate({ cmd: 'connack', returnCode: 0, sessionPresent: false }));
        for (let messageId = 1; messageId <= 30; messageId++) {
          socket.write(mqttPacket.generate({ cmd: 'publish', topic: 'test/packets', payload: 'data', qos: 1, messageId, dup: false, retain: false }));
        }
      }
      if (packet.cmd === 'pingreq') { pings++; socket.write(mqttPacket.generate({ cmd: 'pingresp' })); }
      if (packet.cmd === 'puback') acks++;
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  let release!: () => void;
  const commit = new Promise<void>((resolve) => { release = resolve; });
  const errors: string[] = [];
  const client = mqtt.connect(`mqtt://127.0.0.1:${(server.address() as net.AddressInfo).port}`, { clientId: 'durable-intake-test', keepalive: 1, reconnectPeriod: 0, clean: false });
  client.on('error', (error) => errors.push(error.message));
  installDurableMqttIntake(client, () => commit, (error) => errors.push(error.message));
  t.after(async () => { release(); await client.endAsync(true); for (const socket of sockets) socket.destroy(); server.close(); });
  await once(client, 'connect');
  await pause(2_700);
  assert.ok(pings >= 2);
  assert.equal(client.connected, true);
  assert.deepEqual(errors, []);
  assert.equal(acks, 0, 'no acknowledgement before persistence');
  release();
  await until(() => acks === 30);
});

test('failed commits, overflow and old sessions cannot acknowledge new deliveries', async () => {
  const sent: unknown[] = [];
  const failures: string[] = [];
  const stream = {};
  const client = { connected: true, stream, _sendPacket: (packet: unknown, cb?: () => void) => { sent.push(packet); cb?.(); } } as unknown as MqttClient;
  let release!: () => void;
  const commit = new Promise<void>((resolve) => { release = resolve; });
  const intake = installDurableMqttIntake(client, () => commit, (error) => failures.push(error.message), { maxItems: 1 });
  const packet = { cmd: 'publish', qos: 1, messageId: 1, topic: 'test', payload: Buffer.from('one') } as const;
  client.handleMessage(packet, () => (client as any)._sendPacket({ cmd: 'puback', messageId: 1 }));
  client.handleMessage({ ...packet, messageId: 2 }, (error) => { assert.match(error!.message, /QUEUE_FULL/); });
  assert.deepEqual(failures, ['MQTT_DURABLE_QUEUE_FULL']);
  (client as any).stream = {};
  release();
  await intake.drain();
  assert.deepEqual(sent, [], 'previous session commit cannot acknowledge reused IDs');
  const failed = installDurableMqttIntake(client, async () => { throw new Error('DB unavailable'); }, (error) => failures.push(error.message));
  client.handleMessage(packet, () => (client as any)._sendPacket({ cmd: 'puback', messageId: 1 }));
  await failed.drain();
  assert.equal(failures.at(-1), 'DB unavailable');
  assert.deepEqual(sent, []);
});
