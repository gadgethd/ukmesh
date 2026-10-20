import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { getNodeHistory } from './index.js';

test('history retains its DTO, indexed alias predicates, time cap and fail-closed privacy in both directions', async (t) => {
  const rows = [{ time: '2026-09-07T12:00:00Z', packet_hash: '1234', src_node_id: 'B'.repeat(64),
    topic: 'ukmesh/LHR/observer/packets', packet_type: 4, hop_count: 1, rssi: -99, snr: 3, payload: {} }];
  let calls = 0;
  t.mock.method(pg.Pool.prototype, 'query', async (sql: string, params: unknown[]) => {
    calls += 1;
    assert.match(sql, /WHERE p.rx_node_id = requested.node_id/);
    assert.match(sql, /WHERE p.src_node_id = requested.node_id/);
    assert.match(sql, /FROM unnest\(ARRAY\(/);
    assert.match(sql, /UNION ALL/);
    assert.match(sql, /NOT COALESCE\(p.rx_node_id = ANY\(ARRAY\(/);
    assert.doesNotMatch(sql, /(?:UPPER|BTRIM)\(p\.(?:rx|src)_node_id\)\s*=\s*ANY/);
    assert.match(sql, /canonical_node_id = alias.canonical_node_id/);
    assert.match(sql, /p.time > NOW\(\) - INTERVAL '1 hour' \* \$2/);
    assert.match(sql, /p.time <= NOW\(\)/);
    assert.match(sql, /p.network = ANY\(\$3\)/);
    assert.match(sql, /meshcore-test/);
    assert.match(sql, /p.visibility_ok IS TRUE/);
    assert.match(sql, /p.is_private IS NOT TRUE/);
    assert.match(sql, /cached_visibility.visibility_generation = current_visibility.generation/);
    assert.match(sql, /ORDER BY time DESC LIMIT 500/);
    assert.deepEqual(params, ['A'.repeat(64), 24, ['ukmesh', 'northeast', 'teesside']]);
    return { rows };
  });
  assert.equal(await getNodeHistory('A'.repeat(64), 24, 'ukmesh'), rows);
  assert.equal(calls, 1);
});

test('history returns an empty array unchanged', async (t) => {
  t.mock.method(pg.Pool.prototype, 'query', async () => ({ rows: [] }));
  assert.deepEqual(await getNodeHistory('0'.repeat(64)), []);
});
