import assert from 'node:assert/strict';
import test from 'node:test';
import { aggregateLearningEvidence, type LearningEvidenceInput } from './evidence.js';

function fixture(): LearningEvidenceInput {
  const node = (node_id: string) => ({ node_id, lat: 51, lon: -1, elevation_m: null, iata: 'LHR' });
  return {
    nodes: ['AA0011', 'BB0022', 'CC0033', 'DD0044'].map(node),
    links: [
      { node_a_id: 'AA0011', node_b_id: 'DD0044', itm_path_loss_db: 110, count_a_to_b: 0, count_b_to_a: 1 },
      { node_a_id: 'AA0011', node_b_id: 'BB0022', itm_path_loss_db: 110, count_a_to_b: 1, count_b_to_a: 0 },
      { node_a_id: 'BB0022', node_b_id: 'CC0033', itm_path_loss_db: 110, count_a_to_b: 1, count_b_to_a: 0 },
    ],
    packets: [{ time: '2026-08-07T12:00:00Z', rx_node_id: 'CC0033', src_node_id: 'DD0044', path_hashes: ['AA0011', 'BB0022'] }],
    privateNodeIds: [], nowMs: Date.parse('2026-08-08T00:00:00Z'),
    maxAggregateKeys: 10_000, assertActive() {},
  };
}

test('training publishes source-to-receiver counts and aligned champion positions', () => {
  const { datasets, calibration } = aggregateLearningEvidence(fixture());
  assert.deepEqual(datasets[1], [
    { prefix: 'AA', position: 0, node_id: 'AA0011', count: 1, probability: 1 },
    { prefix: 'BB', position: 1, node_id: 'BB0022', count: 1, probability: 1 },
  ]);
  assert.deepEqual(datasets[2], [
    { src_node_id: 'DD0044', rx_node_id: 'CC0033', position: 0, node_id: 'AA0011', count: 1, probability: 1 },
    { src_node_id: 'DD0044', rx_node_id: 'CC0033', position: 1, node_id: 'BB0022', count: 1, probability: 1 },
  ]);
  assert.deepEqual(datasets[4], [
    { position: 0, from_node_id: 'DD0044', to_node_id: 'AA0011', count: 1, probability: 1 },
    { position: 1, from_node_id: 'AA0011', to_node_id: 'BB0022', count: 1, probability: 1 },
    { position: 2, from_node_id: 'BB0022', to_node_id: 'CC0033', count: 1, probability: 1 },
  ]);
  assert.equal(calibration.evaluatedPackets, 1);
  assert.equal(calibration.top1Accuracy, 1);
});

test('missing relays do not create a verified bridge or a complete champion route', () => {
  const input = fixture();
  input.packets[0]!.path_hashes = ['AA0011', 'FFFFFF', 'BB0022'];
  const { datasets } = aggregateLearningEvidence(input);
  assert.deepEqual(datasets[1], []);
  assert.deepEqual(datasets[2], []);
  assert.deepEqual(datasets[4], []);
  assert.equal(datasets[3].some(row => row.from_node_id === 'AA0011' && row.to_node_id === 'BB0022'), false);
  assert.equal(datasets[3].some(row => row.from_node_id === 'DD0044' && row.to_node_id === 'AA0011'), true);
  assert.equal(datasets[3].some(row => row.from_node_id === 'BB0022' && row.to_node_id === 'CC0033'), true);
});

test('publication excludes a private current or preceding prefix', () => {
  const input = fixture();
  input.privateNodeIds = ['AA0011'];
  assert.deepEqual(aggregateLearningEvidence(input).datasets[0], []);
});

test('bounded training stops on lost ownership or exhausted aggregate capacity', () => {
  const input = fixture();
  input.maxAggregateKeys = 1;
  assert.throws(() => aggregateLearningEvidence(input), /PATH_LEARNING_AGGREGATE_LIMIT/);
  input.assertActive = () => { throw new Error('lease no longer owned'); };
  assert.throws(() => aggregateLearningEvidence(input), /lease no longer owned/);
});
