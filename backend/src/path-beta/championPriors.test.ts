import assert from 'node:assert/strict';
import test from 'node:test';
import { buildChampionPriors, loadChampionPriors } from './championPriors.js';

test('combines regional counts and keeps position indices aligned with source edges', () => {
  const priors = buildChampionPriors({
    positionPrefixes: [{ prefix: 'aa', position: 0, node_id: 'AA01', count: '3' }, { prefix: 'AA', position: 1, node_id: 'AA01', count: 2 }],
    corridors: [{ src_node_id: 'SRC', rx_node_id: 'RX', position: 0, node_id: 'AA01', count: 3 }],
    transitions: [{ from_node_id: 'AA01', to_node_id: 'BB01', count: 5 }, { from_node_id: 'AA01', to_node_id: 'BB01', count: '7' }],
    positionTransitions: [{ position: 0, from_node_id: 'SRC', to_node_id: 'AA01', count: 3 }, { position: 1, from_node_id: 'AA01', to_node_id: 'BB01', count: 2 }],
  });
  assert.equal(priors.prefixCounts.get('AA|AA01'), 5);
  assert.equal(priors.positionPrefixCounts.get('AA|0|AA01'), 3);
  assert.equal(priors.transitionCounts.get('AA01|BB01'), 12);
  assert.equal(priors.corridorCounts.get('SRC|RX|0|AA01'), 3);
  assert.equal(priors.positionTransitionCounts.get('0|SRC|AA01'), 3);
  assert.equal(priors.verifiedEdgeCounts.get('AA01|BB01'), 2);
  assert.equal(priors.verifiedEdgeCounts.has('BB01|AA01'), false);
});

test('loads all four count tables and fails visibly if a bounded model would be truncated', async () => {
  const tables: string[] = [];
  await loadChampionPriors('ukmesh', async (sql, params) => {
    assert.equal(params[0], 'ukmesh');
    tables.push(sql.match(/FROM (\w+)/)![1]!);
    return { rows: [] };
  });
  assert.deepEqual(tables.sort(), ['path_corridor_priors', 'path_position_prefix_priors', 'path_position_transition_priors', 'path_transition_priors']);
  await assert.rejects(loadChampionPriors('ukmesh', async () => ({ rows: new Array(100_001) })), /PATH_CHAMPION_PRIOR_LIMIT/);
});
