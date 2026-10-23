import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { getNodes, getRecentMessages, getRecentPackets, getRecentPacketEvents, getMultibytePathSegments, getViableLinks } from './index.js';
import { runMigrations } from './migrations.js';
import { processNextPrivacyRematerialization } from '../workers/privacy-rematerialization.js';
import { createNodeRepository } from '../repositories/nodes.js';
import { networkFilters } from '../api/utils/networkFilters.js';

const url = process.env['TEST_REGIONS_DATABASE_URL'];
const local = url && ['localhost', '127.0.0.1'].includes(new URL(url).hostname);

test('region reads preserve identity/privacy semantics and use indexes in real PostgreSQL plans', {
  skip: local ? false : 'disposable loopback PostgreSQL fixture required', timeout: 120_000,
}, async (t) => {
  const admin = new pg.Pool({ connectionString: url, max: 1 });
  const name = `meshcore_regions_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const owned = new URL(url!);
  owned.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: owned.toString(), max: 2 });
  // Capture the original method before redirecting application pools to our fixture.
  const query = pool.query.bind(pool);
  t.after(async () => {
    t.mock.restoreAll();
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  });
  await query(fs.readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8'));
  await runMigrations(pool);
  const a = 'A'.repeat(64), b = 'B'.repeat(64), c = 'C'.repeat(64), privateId = 'D'.repeat(64);
  await query(`INSERT INTO nodes (node_id, name, network, role, lat, lon, advert_count, last_rx_at)
    VALUES ($1, 'Canonical', 'ukmesh', 1, NULL, NULL, 3, NOW()),
           ($2, 'Representative', 'ukmesh', 2, 54, -1, 4, NOW()),
           ($3, 'Other deployment', 'othernet', 2, 55, -2, 1, NOW()),
           ($4, '🚫 Private', 'ukmesh', 2, 56, -3, 1, NOW())`, [a, b, c, privateId]);
  await query(`INSERT INTO nodes (node_id, name, network, role, lat, lon)
    SELECT upper(md5(i::text) || md5(i::text)), 'Fixture ' || i, 'ukmesh', 2, 54, -1
    FROM generate_series(1, 12000) i`);
  await query(`INSERT INTO node_identity_aliases
    (source_node_id, canonical_node_id, confidence, reason) VALUES ($1, $2, 'high', 'fixture')`, [b, a]);
  await query(`INSERT INTO region_scope_stats(region) VALUES ('Europe'), ('eng-ne')`);
  await query(`INSERT INTO region_scope_members(region, node_id, packets)
    VALUES ('Europe', $1, 4), ('Europe', $2, 4), ('Europe', $3, 4)`, [b, c, privateId]);
  await query(`INSERT INTO region_scope_members(region, node_id, packets)
    SELECT 'eng-ne', upper(md5(i::text) || md5(i::text)), 1 FROM generate_series(1, 12000) i`);
  await query(`INSERT INTO packets
    (packet_hash, rx_node_id, src_node_id, topic, network, packet_type, region_scope, path_hashes, path_hash_size_bytes, payload)
    SELECT md5(i::text), $1, $1, 'meshcore/ZZZ/fixture/packets', 'ukmesh', 5,
      '#eng-ne', ARRAY[]::text[], 1, '{"_summary":"fixture"}'::jsonb
    FROM generate_series(1, 24000) i`, [a]);
  // Same RF hash, different scopes: regional observer counts must stay regional.
  await query(`INSERT INTO packets
    (packet_hash, rx_node_id, src_node_id, topic, network, packet_type, region_scope, path_hashes, path_hash_size_bytes, payload)
    VALUES ('regional', $1, $1, 'meshcore/ZZZ/fixture/packets', 'ukmesh', 5, '#Europe', ARRAY[]::text[], 1, '{}'),
           ('regional', $2, $1, 'meshcore/ZZZ/fixture/packets', 'ukmesh', 5, '#eng-ne', ARRAY[]::text[], 1, '{}'),
           ('other-network', $3, $3, 'meshcore/ZZZ/fixture/packets', 'othernet', 5, '#Europe', ARRAY[]::text[], 1, '{}'),
           ('private', $4, $4, 'meshcore/ZZZ/fixture/packets', 'ukmesh', 5, '#Europe', ARRAY[]::text[], 1, '{}')`, [a, c, c, privateId]);
  while (await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 })) { /* fixture only */ }
  await query('ANALYZE');
  const calls: Array<{ sql: string; params?: unknown[] }> = [];
  t.mock.method(pg.Pool.prototype, 'query', (sql: string, params?: unknown[]) => {
    calls.push({ sql, params });
    return query(sql, params);
  });

  const regional = await getNodes('ukmesh', undefined, 'full', undefined, 'Europe');
  const all = await getNodes('ukmesh');
  assert.deepEqual(regional, all.filter((row) => [a, privateId].includes(row.node_id)));
  assert.equal(regional.find((row) => row.node_id === a)?.name, 'Representative');
  assert.deepEqual(await getNodes('ukmesh', undefined, 'full', undefined, 'unknown'), []);
  const repository = createNodeRepository((sql, params) => {
    calls.push({ sql, params });
    return query(sql, params);
  });
  const mapped = await repository.listPublicMapRows(['node_id', 'name', 'last_seen'], networkFilters('ukmesh', undefined, { region: 'Europe' }), new Date().toISOString(), null, 100);
  assert.deepEqual(mapped.map((row) => row['node_id']), [a]);
  const packets = await getRecentPackets(100, 'ukmesh', undefined, 'full', undefined, undefined, [], 'Europe');
  assert.deepEqual(packets.map((row) => row['packet_hash']), ['regional']);
  assert.equal(packets[0]?.['rx_count'], 1);
  assert.deepEqual(packets[0]?.['observer_node_ids'], [a]);
  const messages = await getRecentMessages(100, 'ukmesh', undefined, undefined, undefined, [], 'Europe');
  assert.deepEqual(messages.map((row) => row['packet_hash']), ['regional']);
  assert.equal(messages[0]?.['rx_count'], 1);
  const events = await getRecentPacketEvents(100, 'ukmesh', undefined, 'Europe');
  assert.deepEqual(events.map((row) => row['packet_hash']), ['regional']);
  assert.deepEqual(await getMultibytePathSegments('ukmesh', undefined, 'Europe'), { maxCount: 0, segments: [] });
  assert.deepEqual(await getViableLinks('ukmesh', undefined, undefined, 'Europe'), []);

  for (const { sql, params } of calls.filter((call) => call.params?.includes('Europe'))) {
    const result = await query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`, params);
    const plan = result.rows[0]['QUERY PLAN'][0];
    const serialized = JSON.stringify(plan);
    assert.match(serialized, /Index (?:Only )?Scan|Bitmap Index Scan/);
    const visit = (node: Record<string, unknown>) => {
      if (node['Node Type'] === 'Seq Scan') {
        assert.doesNotMatch(String(node['Relation Name']), /^(?:nodes|region_scope_members|_hyper_.*_chunk)$/,
          `unbounded regional read: ${serialized}`);
      }
      for (const child of (node['Plans'] ?? []) as Array<Record<string, unknown>>) visit(child);
    };
    visit(plan.Plan);
    if (sql.includes('FROM packets p')) assert.match(serialized, /region_scope_time_idx/);
    t.diagnostic(`regional ${sql.includes('FROM packets p') ? 'packet' : 'node'} plan: ${plan['Execution Time']} ms; indexed, no base-table sequential scan`);
  }
});
