import assert from 'node:assert/strict';
import test from 'node:test';
import { cleanupInactiveNodes, cleanupStaleMqttObservers } from './staleMqttObservers.js';

type StubResult = { rows: Array<Record<string, unknown>>; rowCount: number };

function stubPool(results: Array<StubResult | Error>) {
  const calls: Array<{ text: string; values?: unknown[] }> = [];
  let released = false;
  const client = {
    async query(text: string, values?: unknown[]) {
      calls.push({ text, values });
      const result = results.shift() ?? { rows: [], rowCount: 0 };
      if (result instanceof Error) throw result;
      return result;
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

test('inactive-node cleanup has no role or prior MQTT requirement and considers every activity clock', async () => {
  const stub = stubPool([]);
  const result = await cleanupInactiveNodes({ cleanupPool: stub.pool, thresholdDays: 10 });
  assert.equal(result.candidates, 0);
  const selection = stub.calls[2]!;
  assert.equal(selection.values?.[0], 30);
  assert.match(selection.text, /GREATEST\(last_seen, last_mqtt_observer_seen_at, last_path_evidence_at,\s+last_predicted_online_at, created_at\)/);
  assert.match(selection.text, /network IS DISTINCT FROM 'test'/);
  assert.match(selection.text, /COALESCE\(n.name, ''\) NOT LIKE '%🚫%'/);
  assert.match(selection.text, /private_node_prefixes p WHERE p.node_id = n.node_id/);
  assert.match(selection.text, /FOR UPDATE/);
  assert.match(selection.text, /s.node_id = n.node_id/);
  assert.match(selection.text, /s.last_seen_at >= NOW/);
  assert.match(selection.text, /s.rx_node_id = n.node_id/);
  assert.match(selection.text, /s.last_seen >= NOW/);
  assert.doesNotMatch(selection.text, /role\s*(?:=|IS)|last_mqtt_observer_seen_at IS NOT NULL/);
  assert.equal(stub.calls.some((call) => call.text.includes('DELETE')), false);
  assert.equal(stub.released(), true);
});

test('inactive companions, room servers and never-bridged nodes are archived before any delete', async () => {
  const ids = ['A'.repeat(64), 'B'.repeat(64), 'C'.repeat(64)];
  const stub = stubPool([
    { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
    { rows: ids.map((node_id) => ({ node_id })), rowCount: 3 },
    { rows: [], rowCount: 3 }, { rows: [], rowCount: 2 }, { rows: [], rowCount: 4 },
    { rows: [{ materialization_current: true }], rowCount: 1 },
    { rows: [], rowCount: 2 }, { rows: [], rowCount: 4 }, { rows: [], rowCount: 3 },
  ]);
  const result = await cleanupInactiveNodes({ cleanupPool: stub.pool, batchId: 'inactive-batch', thresholdDays: 45 });
  assert.deepEqual(result, {
    batchId: 'inactive-batch', candidates: 3, nodes: 3, observerSightings: 2, networkSightings: 4,
  });
  const archiveCalls = stub.calls.filter((call) => call.text.includes('INSERT INTO maintenance_removed_records'));
  assert.equal(archiveCalls.length, 3);
  for (const call of archiveCalls) {
    assert.deepEqual(call.values, ['inactive-batch', 'Node has no observed activity for at least 45 days', ids]);
  }
  const lastArchive = stub.calls.findLastIndex((call) => call.text.includes('INSERT INTO maintenance_removed_records'));
  const firstDelete = stub.calls.findIndex((call) => call.text.includes('DELETE FROM'));
  assert.ok(lastArchive < firstDelete);
  assert.equal(stub.calls.at(-1)?.text, 'COMMIT');
  const lockIndex = stub.calls.findIndex((call) => call.text.includes('FOR UPDATE OF visibility'));
  const fenceIndex = stub.calls.findIndex((call) => call.text.includes('UPDATE packet_visibility_materialization_state'));
  assert.ok(lastArchive < lockIndex && lockIndex < firstDelete);
  assert.ok(fenceIndex > stub.calls.findLastIndex((call) => call.text.includes('DELETE FROM')));
  assert.equal(stub.released(), true);
});

test('archive failure rolls back and admits no delete', async () => {
  const stub = stubPool([
    { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
    { rows: [{ node_id: 'A'.repeat(64) }], rowCount: 1 },
    new Error('archive failed'),
  ]);
  await assert.rejects(cleanupInactiveNodes({ cleanupPool: stub.pool }), /archive failed/);
  assert.equal(stub.calls.at(-1)?.text, 'ROLLBACK');
  assert.equal(stub.calls.some((call) => call.text.includes('DELETE FROM')), false);
  assert.equal(stub.released(), true);
});

test('delete failure rolls back the archive and earlier visibility deletes together', async () => {
  const stub = stubPool([
    { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
    { rows: [{ node_id: 'A'.repeat(64) }], rowCount: 1 },
    { rows: [], rowCount: 1 }, { rows: [], rowCount: 1 }, { rows: [], rowCount: 1 },
    { rows: [{ materialization_current: true }], rowCount: 1 },
    { rows: [], rowCount: 1 }, { rows: [], rowCount: 1 },
    new Error('delete failed'),
  ]);
  await assert.rejects(cleanupInactiveNodes({ cleanupPool: stub.pool }), /delete failed/);
  assert.equal(stub.calls.at(-1)?.text, 'ROLLBACK');
  assert.equal(stub.calls.some((call) => call.text === 'COMMIT'), false);
  assert.equal(stub.released(), true);
});

test('a visibility-lock failure rolls back the archives before any delete', async () => {
  const stub = stubPool([
    { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
    { rows: [{ node_id: 'public' }], rowCount: 1 },
    { rows: [], rowCount: 1 }, { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
    new Error('visibility lock failed'),
  ]);
  await assert.rejects(cleanupInactiveNodes({ cleanupPool: stub.pool }), /visibility lock failed/);
  assert.equal(stub.calls.at(-1)?.text, 'ROLLBACK');
  assert.equal(stub.calls.some((call) => call.text.includes('DELETE FROM')), false);
  assert.equal(stub.released(), true);
});

test('cleanup never certifies a pre-existing stale or missing privacy materialization', async () => {
  for (const rows of [[{ materialization_current: false }], []]) {
    const stub = stubPool([
      { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
      { rows: [{ node_id: 'public' }], rowCount: 1 },
      { rows: [], rowCount: 1 }, { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
      { rows, rowCount: rows.length },
      { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 }, { rows: [], rowCount: 1 },
    ]);
    await cleanupInactiveNodes({ cleanupPool: stub.pool });
    assert.equal(stub.calls.some((call) => call.text.includes('UPDATE packet_visibility_materialization_state')), false);
    assert.equal(stub.calls.at(-1)?.text, 'COMMIT');
  }
});

test('fence-update failure rolls back every archive and delete before releasing the client', async () => {
  const stub = stubPool([
    { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
    { rows: [{ node_id: 'public' }], rowCount: 1 },
    { rows: [], rowCount: 1 }, { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 },
    { rows: [{ materialization_current: true }], rowCount: 1 },
    { rows: [], rowCount: 0 }, { rows: [], rowCount: 0 }, { rows: [], rowCount: 1 },
    new Error('fence update failed'),
  ]);
  await assert.rejects(cleanupInactiveNodes({ cleanupPool: stub.pool }), /fence update failed/);
  assert.equal(stub.calls.at(-1)?.text, 'ROLLBACK');
  assert.equal(stub.calls.some((call) => call.text === 'COMMIT'), false);
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
    { rows: [{ materialization_current: true }], rowCount: 1 }, // visibility lock
    { rows: [{ value: 1 }], rowCount: 1 }, // delete observer sightings
    { rows: [{ value: 1 }], rowCount: 1 }, // delete network sightings
    { rows: [{ value: 1 }], rowCount: 1 }, // delete nodes
    { rows: [], rowCount: 1 }, // preserve the initially current privacy fence
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
  assert.match(stub.calls[2]?.text ?? '', /private_node_prefixes p WHERE p.node_id = n.node_id/);
  const archiveIndex = stub.calls.findIndex((call) => call.text.includes("SELECT $1, 'nodes'"));
  const deleteIndex = stub.calls.findIndex((call) => call.text.includes('DELETE FROM nodes'));
  assert.ok(archiveIndex >= 0 && archiveIndex < deleteIndex);
  assert.equal(stub.released(), true);
});
