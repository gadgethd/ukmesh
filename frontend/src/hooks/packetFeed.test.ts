import assert from 'node:assert/strict';
import test from 'node:test';
import {
  mapMessageRows,
  mergeAggregatedPacket,
  packetRowObserverIatas,
  type RecentPacketRow,
} from './packetFeed.js';

const baseRow: RecentPacketRow = {
  time: '2026-08-01T12:00:00.000Z',
  packet_hash: 'ABCD',
  packet_type: 5,
  rx_node_id: 'observer-a',
  observer_node_ids: ['observer-a'],
  topic: 'meshcore/MME/observer-a/packets',
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
    rx_node_id: 'observer-b',
    observer_node_ids: ['observer-b'],
    iata: 'NCL',
  }]);
  assert.ok(first);
  assert.ok(second);
  assert.deepEqual(
    mergeAggregatedPacket(first, second).observerIatas,
    ['MME', 'NCL'],
  );
});
