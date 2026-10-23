import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { getNodes, getRecentPackets, getRecentMessages, getRecentPacketEvents } from './index.js';
import { networkFilters } from '../api/utils/networkFilters.js';

test('region SQL scopes representatives, counts and nodes with parameterized indexed predicates', async (t) => {
  const calls: Array<{ sql: string; params?: unknown[] }> = [];
  t.mock.method(pg.Pool.prototype, 'query', async (sql: string, params?: unknown[]) => {
    calls.push({ sql, params });
    return { rows: [] };
  });
  for (const region of [undefined, 'Europe']) {
    await getNodes('ukmesh', undefined, 'full', undefined, region);
    const nodes = calls.at(-1)!;
    assert.deepEqual(nodes.params, [['ukmesh', 'northeast', 'teesside'], ...(region ? [region] : [])]);
    if (region) {
      assert.match(nodes.sql, /n.node_id IN \(SELECT COALESCE\(a.canonical_node_id, m.node_id\)/);
      assert.match(nodes.sql, /WHERE m.region = \$2/);
    } else assert.doesNotMatch(nodes.sql, /region_scope_members/);
    for (const load of [
      () => getRecentPackets(12, 'ukmesh', undefined, 'full', undefined, undefined, [], region),
      () => getRecentMessages(12, 'ukmesh', undefined, undefined, undefined, [], region),
      () => getRecentPacketEvents(12, 'ukmesh', undefined, region),
    ]) {
      await load();
      const { sql, params } = calls.at(-1)!;
      assert.deepEqual(params, [12, ['ukmesh', 'northeast', 'teesside'], ...(region ? [region] : [])]);
      assert.match(sql, /visibility_ok IS TRUE/);
      assert.match(sql, /p.time > NOW\(\) - INTERVAL '(?:5 minutes|24 hours)'/);
      if (region) {
        assert.match(sql, /p.region_scope = '#' \|\| \$3::text/);
        if (sql.includes('GROUP BY packet_hash')) {
          assert.equal((sql.match(/region_scope = '#' \|\| \$3::text/g) ?? []).length, 2);
        }
        assert.doesNotMatch(sql, /LOWER\([^)]*region_scope|LIKE.*region/);
      } else assert.doesNotMatch(sql, /region_scope\s*=/);
    }
  }
  const filters = networkFilters('ukmesh', 'A'.repeat(64), { region: 'eng-ne' });
  assert.match(filters.packetsAlias('p'), /p.region_scope = '#' \|\| \$3::text/);
  assert.match(filters.nodesAlias('n'), /WHERE m.region = \$3/);
  const migration = fs.readFileSync(new URL('./migrations/063_packet_region_scope_index.sql', import.meta.url), 'utf8');
  assert.match(migration, /ON packets \(region_scope, time DESC\)/);
});
