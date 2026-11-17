import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from '../db/fixtures/disposableDatabase.js';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from '../db/fixtures/baseSchema.js';
import { networkFilters } from '../platform/query/networkFilters.js';
import { createStatsRepository } from './statsRepository.js';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];

test('summary preserves presence boundaries, role/privacy rules and international scope', {
  skip: endpoint ? false : 'owned loopback TEST_SAMPLE_DATABASE_URL required', timeout: 120_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'stats_summary');
  const pool = new pg.Pool({ connectionString: fixture.url, max: 2, options: POST_MOVE_SEARCH_PATH });
  fixture.addCleanup(() => pool.end());
  await bootstrapBaseSchemaFixture(pool);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // NOW() is fixed for every age boundary and every summary query below.
    await client.query(`INSERT INTO core.nodes
      (node_id,name,network,role,lat,lon,last_seen,last_path_evidence_at,advert_count)
      SELECT lpad(i::text,64,'0'),name,'ukmesh',role,lat,lon,
             NOW()+age::interval,CASE WHEN evidence THEN NOW()-INTERVAL '1 day' END,adverts
      FROM (VALUES
        (1,'Fresh',NULL::integer,51.0,-1.0,'-1 day',false,1),
        (2,'Stale boundary',2,51.0,-1.0,'-14 days',false,1),
        (3,'Map boundary',2,51.0,-1.0,'-28 days',false,1),
        (4,'Old',2,51.0,-1.0,'-29 days',false,1),
        (5,'Future',2,51.0,-1.0,'1 day',false,1),
        (6,'Path presence',2,51.0,-1.0,'-40 days',true,1),
        (7,'Private 🚫',2,51.0,-1.0,'-1 day',false,1),
        (8,'Client',1,51.0,-1.0,'-1 day',false,1),
        (9,'Companion',3,51.0,-1.0,'-1 day',false,1),
        (10,'Role four',4,51.0,-1.0,'-1 day',false,1),
        (11,'Invalid coordinates',2,95.0,-1.0,'-1 day',false,1),
        (12,'Origin',2,0.0,0.0,'-1 day',false,1),
        (13,'Belgian client',1,50.95,4.35,'-30 minutes',false,20),
        (14,'French role four',4,48.85,2.35,'-2 hours',false,10),
        (15,'International boundary',2,48.85,2.35,'-7 days',false,99)
      ) AS data(i,name,role,lat,lon,age,evidence,adverts)`);
    const repository = createStatsRepository({ query: (sql, params) => client.query(sql, params), networkFilters });
    const scoped = await repository.fetchStatsSummary('ukmesh', undefined);
    assert.equal(scoped.staleCount.rows[0]?.count, '1');
    assert.equal(scoped.mapNodeCount.rows[0]?.count, '6');
    assert.equal(scoped.totalNodeCount.rows[0]?.count, '12');
    const lastSeen = (await client.query("SELECT (NOW()-INTERVAL '30 minutes')::text AS value")).rows[0].value;
    assert.deepEqual(scoped.internationalCount.rows, [{
      count_connected: '1', total_adverts: '30', last_seen_at: lastSeen, last_lat: 50.95, last_lon: 4.35,
    }]);
    const observer = await repository.fetchStatsSummary('ukmesh', '1'.padStart(64, '0'));
    assert.equal(observer.mapNodeCount.rows[0]?.count, '1');
    assert.equal(observer.totalNodeCount.rows[0]?.count, '1');
    const empty = await repository.fetchStatsSummary('othernet', undefined);
    assert.equal(empty.staleCount.rows[0]?.count, '0');
    assert.equal(empty.mapNodeCount.rows[0]?.count, '0');
    assert.equal(empty.totalNodeCount.rows[0]?.count, '0');
    assert.deepEqual(empty.internationalCount.rows, [{
      count_connected: '0', total_adverts: null, last_seen_at: null, last_lat: null, last_lon: null,
    }]);
  } finally {
    await client.query('ROLLBACK');
    client.release();
  }
});
