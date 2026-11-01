import assert from 'node:assert/strict';
import test from 'node:test';
import { assertSupportedIngestConnections } from './ingestConfig.js';

test('F04 shared-subscription N>1 is gated until restart and resize recovery is qualified', () => {
  assert.doesNotThrow(() => assertSupportedIngestConnections(1));
  assert.throws(() => assertSupportedIngestConnections(2), /MQTT_INGEST_CONNECTIONS_UNSUPPORTED/);
  assert.throws(() => assertSupportedIngestConnections(16), /MQTT_INGEST_CONNECTIONS_UNSUPPORTED/);
});

test('intake queue capacity uses the configured bound before persistence admission', async () => {
  const { loadMqttIntakeConfig } = await import('./ingestConfig.js');
  const { installDurableMqttIntake } = await import('./durableIntake.js');
  assert.deepEqual(loadMqttIntakeConfig({}), { maxItems: 1_000 });
  assert.throws(() => loadMqttIntakeConfig({ MQTT_INGEST_QUEUE_MAX: '0' }), /MQTT_INGEST_QUEUE_MAX/);
  assert.throws(() => loadMqttIntakeConfig({ MQTT_INGEST_QUEUE_MAX: 'NaN' }), /MQTT_INGEST_QUEUE_MAX/);
  const sent: unknown[] = [], failures: string[] = [];
  const client = { connected: true, stream: {}, _sendPacket: (packet: unknown, done?: () => void) => { sent.push(packet); done?.(); } };
  let release!: () => void;
  const commit = new Promise<void>((resolve) => { release = resolve; });
  const intake = installDurableMqttIntake(client as any, () => commit,
    (error) => failures.push(error.message), loadMqttIntakeConfig({ MQTT_INGEST_QUEUE_MAX: '2' }));
  try {
    for (let messageId = 1; messageId <= 3; messageId += 1) {
      const packet = { cmd: 'publish', qos: 1, messageId, topic: 'one-observer/packets', payload: Buffer.from('packet') } as const;
      (client as any).handleMessage(packet, (error?: Error) => {
        if (messageId === 3) assert.match(error!.message, /QUEUE_FULL/);
        else { assert.ifError(error); client._sendPacket({ cmd: 'puback', messageId }); }
      });
    }
    assert.deepEqual(failures, ['MQTT_DURABLE_QUEUE_FULL']);
    assert.deepEqual(sent, [], 'queue limit must never release an early PUBACK');
  } finally { release(); await intake.drain(); }
  assert.equal(sent.length, 2);
});
