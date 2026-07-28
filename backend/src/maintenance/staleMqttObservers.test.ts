import assert from 'node:assert/strict';
import test from 'node:test';
import { cleanupStaleMqttObservers } from './staleMqttObservers.js';

type StubResult = { rows: Array<Record<string, unknown>>; rowCount: number };

function stubPool(results: StubResult[]) {
  const calls: Array<{ text: string; values?: unknown[] }> = [];
  let released = false;
  const client = {
    async query(text: string, values?: unknown[]) {
      calls.push({ text, values });
      return results.shift() ?? { rows: [], rowCount: 0 };
    },
    release() {
      released = true;
    },
  };
  return {
    pool: { async connect() { return client; } },
    calls,
    released: () => released,
  };
}

test('does nothing when no MQTT observers have crossed the stale threshold', async () => {
  const stub = stubPool([
    { rows: [], rowCount: 0 }, // BEGIN
    { rows: [], rowCount: 0 }, // advisory lock
    { rows: [], rowCount: 0 }, // candidates
    { rows: [], rowCount: 0 }, // COMMIT
  ]);

  const result = await cleanupStaleMqttObservers({
    cleanupPool: stub.pool,
    thresholdDays: 30,
  });

  assert.deepEqual(result, {
    batchId: null,
    candidates: 0,
    nodes: 0,
    observerSightings: 0,
    networkSightings: 0,
  });
  assert.equal(stub.calls.some((call) => call.text.includes('DELETE FROM nodes')), false);
  assert.equal(stub.released(), true);
});

test('archives visibility records before deleting stale observer nodes', async () => {
  const stub = stubPool([
    { rows: [], rowCount: 0 }, // BEGIN
    { rows: [], rowCount: 0 }, // advisory lock
    { rows: [{ node_id: 'A'.repeat(64) }], rowCount: 1 },
    { rows: [], rowCount: 1 }, // archive nodes
    { rows: [], rowCount: 1 }, // archive observer sightings
    { rows: [], rowCount: 1 }, // archive network sightings
    { rows: [{ value: 1 }], rowCount: 1 }, // delete observer sightings
    { rows: [{ value: 1 }], rowCount: 1 }, // delete network sightings
    { rows: [{ value: 1 }], rowCount: 1 }, // delete nodes
    { rows: [], rowCount: 0 }, // COMMIT
  ]);

  const result = await cleanupStaleMqttObservers({
    cleanupPool: stub.pool,
    thresholdDays: 10,
    batchId: 'test-batch',
  });

  assert.equal(result.batchId, 'test-batch');
  assert.equal(result.candidates, 1);
  assert.equal(result.nodes, 1);
  assert.equal(stub.calls[2]?.values?.[0], 30, 'threshold is never allowed below one month');
  assert.match(stub.calls[2]?.text ?? '', /last_mqtt_observer_seen_at/);
  assert.match(stub.calls[2]?.text ?? '', /\(role IS NULL OR role = 2\)/);
  const archiveIndex = stub.calls.findIndex((call) => call.text.includes("SELECT $1, 'nodes'"));
  const deleteIndex = stub.calls.findIndex((call) => call.text.includes('DELETE FROM nodes'));
  assert.ok(archiveIndex >= 0 && archiveIndex < deleteIndex);
  assert.equal(stub.released(), true);
});
