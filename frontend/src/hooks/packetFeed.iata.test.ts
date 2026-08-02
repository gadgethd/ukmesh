import assert from 'node:assert/strict';
import test from 'node:test';
import {
  aggregatedPacketObserverIataLabel,
  mapMessageRows,
  mergeAggregatedPacket,
  packetRowObserverIatas,
  type RecentPacketRow,
} from './packetFeed.js';

const baseRow: RecentPacketRow = {
  time: '2026-08-01T12:00:00.000Z',
  packet_hash: 'ABCD',
  packet_type: 5,
  rx_node_id: 'AABBCCDD',
  observer_node_ids: ['AABBCCDD'],
  topic: 'meshcore/MME/AABBCCDD/packets',
};

test('stored observer IATAs are first-class and normalized', () => {
  assert.deepEqual(packetRowObserverIatas({
    ...baseRow,
    iata: ' mme ',
    observer_iatas: ['NCL', 'mme', null, 'not/valid'],
  }), ['NCL', 'MME']);
});

test('legacy rows fall back to the topic IATA', () => {
  assert.deepEqual(packetRowObserverIatas(baseRow), ['MME']);
});

test('message mapping and aggregation retain every observer IATA', () => {
  const [first] = mapMessageRows([{ ...baseRow, observer_iatas: ['MME'] }]);
  const [second] = mapMessageRows([{
    ...baseRow,
    time: '2026-08-01T12:00:01.000Z',
    rx_node_id: '11223344',
    observer_node_ids: ['11223344'],
    iata: 'NCL',
  }]);
  assert.ok(first);
  assert.ok(second);
  assert.deepEqual(
    mergeAggregatedPacket(first, second).observerIatas,
    ['MME', 'NCL'],
  );
});

test('the compact feed prefers stored observer regions and retains a node fallback', () => {
  assert.equal(
    aggregatedPacketObserverIataLabel(
      { rxNodeId: 'AABBCCDD', observerIatas: [' mme ', 'NCL'] },
      new Map([['AABBCCDD', { iata: 'LBA' }]]),
    ),
    'MME · NCL',
  );
  assert.equal(
    aggregatedPacketObserverIataLabel(
      { rxNodeId: 'AABBCCDD', observerIatas: [] },
      new Map([['AABBCCDD', { iata: ' lba ' }]]),
    ),
    'LBA',
  );
});
