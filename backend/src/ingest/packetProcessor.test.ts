import assert from 'node:assert/strict';
import test from 'node:test';
import { createPacketProcessor } from './packetProcessor.js';
import { createIngestEvents } from './events.js';
import { mqttIngestOutcomesTotal } from '../metrics/ingest.js';

test('packet processing enforces wire bounds before decode and refuses a closed runtime', async t => {
  let deliveries = 0;
  const events = createIngestEvents();
  events.onPacket(() => { deliveries++; });
  const processor = createPacketProcessor({ clientId: 'owned-fixture', events,
    markMqttPersistenceHealthy: () => { assert.fail('unpersisted packet cannot mark persistence healthy'); },
    committedFollowUps: { schedule() { assert.fail('unpersisted packet cannot run committed work'); }, start() {}, close: async () => {} } });
  t.after(events.close); t.after(processor.close);
  await processor.process({ topic: 'meshcore/ZZZ/owned/packets', payload: Buffer.alloc(processor.maxPayloadBytes + 1) });
  await processor.process({ topic: 'x'.repeat(513), payload: '{}' });
  const outcomes = (await mqttIngestOutcomesTotal.get()).values;
  assert.equal(outcomes.find(value => value.labels.outcome === 'oversized_payload')?.value, 1);
  assert.equal(outcomes.find(value => value.labels.outcome === 'oversized_topic')?.value, 1);
  assert.equal(deliveries, 0);
  assert.equal(processor.activeCount(), 0);
  await processor.close();
  await assert.rejects(processor.process({ topic: 'owned', payload: '{}' }), /MQTT_PACKET_PROCESSOR_CLOSED/);
});
