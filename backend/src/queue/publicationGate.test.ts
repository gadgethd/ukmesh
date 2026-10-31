import assert from 'node:assert/strict';
import test from 'node:test';
import net from 'node:net';
import { once } from 'node:events';
import { Redis } from 'ioredis';
import { getRedisPublisherOptions } from '../platform/config/redis.js';
import { RedisPublicationGate, RedisPublicationUnavailableError } from './publicationGate.js';

test('offline publishers retain no work; hung commands have count and byte bounds', async () => {
  const gate = new RedisPublicationGate(2, 10);
  let commands = 0;
  const offline = { status: 'reconnecting' as const };
  for (let index = 0; index < 1_000; index++) {
    await assert.rejects(gate.run(offline, 1, async () => { commands++; }), /PUBLICATION_OFFLINE/);
  }
  assert.equal(commands, 0);
  const ready = { status: 'ready' as const };
  let release!: () => void;
  const hold = new Promise<void>((resolve) => { release = resolve; });
  const pending = gate.run(ready, 8, () => hold);
  await assert.rejects(gate.run(ready, 3, async () => {}), /PUBLICATION_FULL/);
  const second = gate.run(ready, 2, () => hold);
  await assert.rejects(gate.run(ready, 0, async () => {}), /PUBLICATION_FULL/);
  release();
  await Promise.all([pending, second]);
  assert.equal(await gate.run(ready, 10, async () => 'recovered'), 'recovered');
  await assert.rejects(gate.run(ready, 11, async () => {}), /PUBLICATION_OVERSIZED/);
  await assert.rejects(gate.run(ready, 10, async () => { throw new Error('connection lost'); }), /connection lost/);
  assert.equal(await gate.run(ready, 10, async () => 'released after failure'), 'released after failure');
});

test('real ioredis rejects disconnected work without an offline queue or replay', async (t) => {
  const reserve = net.createServer();
  reserve.listen(0, '127.0.0.1');
  await once(reserve, 'listening');
  const port = (reserve.address() as net.AddressInfo).port;
  await new Promise<void>((resolve) => reserve.close(() => resolve()));
  const client = new Redis(`redis://127.0.0.1:${port}`, {
    ...getRedisPublisherOptions(), lazyConnect: true, retryStrategy: () => null,
  });
  client.on('error', () => {});
  t.after(() => client.disconnect());
  await assert.rejects(client.publish('owned-fixture', 'no queued message'), /offlineQueue|writeable|closed/i);
  await assert.rejects(client.connect());
  assert.equal(client.offlineQueue.length, 0);
  assert.equal(client.commandQueue.length, 0);
  const gate = new RedisPublicationGate();
  await assert.rejects(gate.run(client, 1, async () => {}), RedisPublicationUnavailableError);
});

test('timed-out Redis commands release their retained buffers through reconnect without replay', { timeout: 5_000 }, async (t) => {
  const sockets = new Set<net.Socket>();
  const messages: Array<{ connection: number; payload: string }> = [];
  let connections = 0;
  const server = net.createServer((socket) => {
    sockets.add(socket);
    const connection = ++connections;
    let buffered = Buffer.alloc(0);
    socket.on('data', (data) => {
      buffered = Buffer.concat([buffered, data]);
      // This fixture accepts only RESP arrays of bulk strings, as emitted by
      // the real ioredis handshake and PUBLISH path.
      while (buffered.length) {
        const end = buffered.indexOf('\r\n');
        if (end < 0) return;
        const count = Number(buffered.subarray(1, end).toString());
        let cursor = end + 2;
        const args: string[] = [];
        for (let index = 0; index < count; index++) {
          const headerEnd = buffered.indexOf('\r\n', cursor);
          if (headerEnd < 0) return;
          const length = Number(buffered.subarray(cursor + 1, headerEnd).toString());
          cursor = headerEnd + 2;
          if (buffered.length < cursor + length + 2) return;
          args.push(buffered.subarray(cursor, cursor + length).toString());
          cursor += length + 2;
        }
        buffered = buffered.subarray(cursor);
        if (args[0]?.toLowerCase() === 'publish') {
          messages.push({ connection, payload: args[2]! });
          if (connection > 1) socket.write(':1\r\n');
        } else socket.write('+OK\r\n');
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const client = new Redis(`redis://127.0.0.1:${(server.address() as net.AddressInfo).port}`, {
    ...getRedisPublisherOptions(), lazyConnect: true, enableReadyCheck: false,
    commandTimeout: 100, retryStrategy: () => 10,
  });
  client.on('error', () => {});
  t.after(async () => {
    client.disconnect();
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  await client.connect();
  const gate = new RedisPublicationGate(2, 100);
  const pending = ['old-one', 'old-two'].map((payload) => gate.run(client, 10,
    () => client.publish('owned', payload)));
  const outcomes = await Promise.allSettled(pending);
  assert.ok(outcomes.every((outcome) => outcome.status === 'rejected'));
  const deadline = Date.now() + 2_000;
  while ((connections < 2 || client.status !== 'ready') && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.ok(connections >= 2, 'timed-out streams must be closed and reconnected');
  assert.equal(await gate.run(client, 10, () => client.publish('owned', 'recovered')), 1);
  assert.equal(client.offlineQueue.length, 0);
  assert.equal(client.commandQueue.length, 0);
  assert.deepEqual(messages.filter((message) => message.connection > 1).map((message) => message.payload), ['recovered']);
});
