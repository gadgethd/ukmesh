import assert from 'node:assert/strict';
import test from 'node:test';
import {
  observationIdentityKey,
  packetAlreadyPersisted,
  type ObservationIdentity,
} from './observationIdentity.js';

const base: ObservationIdentity = {
  packetHash: 'ABCDEF',
  rxNodeId: 'A'.repeat(64),
  topic: `meshcore/EMA/${'A'.repeat(64)}/packets`,
  network: 'ukmesh',
  hopCount: 2,
  routeType: 0,
  pathHashes: ['ABCD', 'EF01'],
  pathHashSizeBytes: 2,
  direction: 'rx',
};

test('F03 canonical observation key distinguishes hop, path, direction, observer and scope', () => {
  const key = observationIdentityKey(base);
  assert.equal(key, observationIdentityKey({ ...base, pathHashes: ['abcd', 'ef01'] }));
  for (const changed of [
    { hopCount: 3 },
    { pathHashes: ['ABCD', 'EF02'] },
    { pathHashSizeBytes: 3 },
    { routeType: 1 },
    { direction: 'tx' as const },
    { rxNodeId: 'B'.repeat(64) },
    { topic: `meshcore/MME/${'A'.repeat(64)}/packets` },
    { network: 'teesside' },
  ]) {
    assert.notEqual(key, observationIdentityKey({ ...base, ...changed }), JSON.stringify(changed));
  }
});

test('F03 restart probe checks the same identity, including a prior same-hash different-hop sighting', async () => {
  let statement = '';
  let values: unknown[] = [];
  const query = async (_sql: string, params: unknown[]) => {
    statement = _sql;
    values = params;
    return { rows: [{ present: false }] };
  };
  assert.equal(await packetAlreadyPersisted(query, { ...base, hopCount: 3 }), false);
  assert.match(statement, /hop_count IS NOT DISTINCT FROM/);
  assert.match(statement, /route_type IS NOT DISTINCT FROM/);
  assert.match(statement, /path_hashes IS NOT DISTINCT FROM/);
  assert.match(statement, /path_hash_size_bytes IS NOT DISTINCT FROM/);
  assert.match(statement, /payload->>'direction'/);
  assert.equal(values[4], 3);
  assert.deepEqual(values[6], ['ABCD', 'EF01']);
  assert.equal(values[8], 'rx');
});
