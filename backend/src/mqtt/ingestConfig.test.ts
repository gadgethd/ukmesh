import assert from 'node:assert/strict';
import test from 'node:test';
import { assertSupportedIngestConnections } from './ingestConfig.js';

test('F04 shared-subscription N>1 is gated until restart and resize recovery is qualified', () => {
  assert.doesNotThrow(() => assertSupportedIngestConnections(1));
  assert.throws(() => assertSupportedIngestConnections(2), /MQTT_INGEST_CONNECTIONS_UNSUPPORTED/);
  assert.throws(() => assertSupportedIngestConnections(16), /MQTT_INGEST_CONNECTIONS_UNSUPPORTED/);
});
