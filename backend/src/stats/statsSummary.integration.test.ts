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

    const key = (value: string) => value.repeat(64);
    await client.query(`INSERT INTO core.nodes
      (node_id,name,network,role,lat,lon,last_seen,advert_count)
      SELECT id,name,network,role,lat,lon,NOW()+age::interval,adverts
      FROM jsonb_to_recordset($1::jsonb) AS fixture(id text,name text,network text,
        role integer,lat double precision,lon double precision,age text,adverts integer)`,
    [JSON.stringify([
      { id: key('A'), name: 'Canonical client', network: 'ukmesh', role: 1, lat: 51, lon: -1, age: '-3 hours', adverts: 10 },
      { id: key('B'), name: 'French relay alias', network: 'northeast', role: 2, lat: 48.85, lon: 2.35, age: '-20 minutes', adverts: 3 },
      { id: key('D'), name: 'Missing canonical', network: 'ukmesh', role: 2, lat: 51, lon: -1, age: '-1 day', adverts: 1 },
      { id: key('F'), name: 'Normalized relay', network: 'ukmesh', role: 2, lat: 51, lon: -1, age: '-2 days', adverts: 1 },
      { id: ` ${key('f')} `, name: 'Legacy relay', network: 'ukmesh', role: 2, lat: 51, lon: -1, age: '-1 day', adverts: 2 },
      { id: key('9'), name: 'Public member', network: 'ukmesh', role: 2, lat: 51, lon: -1, age: '-1 hour', adverts: 1 },
      { id: key('8'), name: 'Private 🚫 alias', network: 'ukmesh', role: 2, lat: 51, lon: -1, age: '-2 hours', adverts: 100 },
    ])]);
    await client.query(`INSERT INTO core.node_identity_aliases(source_node_id,canonical_node_id,confidence,reason)
      VALUES ($1,$2,'high','summary fixture'),($3,$4,'high','summary fixture'),($5,$6,'high','summary fixture')`,
    [key('B'), key('A'), key('D'), key('E'), key('8'), key('9')]);
    const identities = await repository.fetchStatsSummary('ukmesh', undefined);
    assert.equal(identities.staleCount.rows[0]?.count, '1');
    assert.equal(identities.mapNodeCount.rows[0]?.count, '9');
    assert.equal(identities.totalNodeCount.rows[0]?.count, '15');
    const aliasSeen = (await client.query("SELECT (NOW()-INTERVAL '20 minutes')::text AS value")).rows[0].value;
    assert.deepEqual(identities.internationalCount.rows, [{
      count_connected: '2', total_adverts: '43', last_seen_at: aliasSeen, last_lat: 48.85, last_lon: 2.35,
    }]);

    await client.query(`INSERT INTO core.nodes(node_id,name,network,role,lat,lon,last_seen,advert_count)
      VALUES ($1,'Tied Belgian','ukmesh',2,50.95,4.35,NOW()-INTERVAL '5 minutes',1),
             ($2,'Tied German','ukmesh',2,52.52,13.405,NOW()-INTERVAL '5 minutes',1)`, [key('C'), key('7')]);
    const tied = (await repository.fetchStatsSummary('ukmesh', undefined)).internationalCount.rows[0];
    assert.equal(tied?.count_connected, '4');
    assert.equal(tied?.total_adverts, '45');
    // The original SQL selects each coordinate independently on last_seen ties.
    // Preserve that contract instead of imposing a new row or node-id ordering.
    assert.ok([50.95, 52.52].includes(tied?.last_lat));
    assert.ok([4.35, 13.405].includes(tied?.last_lon));

    await client.query(`INSERT INTO mesh.packets(time, packet_hash, network, topic, rx_node_id, iata)
      SELECT NOW() - age::interval, packet_hash, 'ukmesh', topic, observer, iata
      FROM (VALUES
        ('26 hours', $1::text, 'meshcore/LHR/a/packets', $3::text, 'lhr'),
        ('1 second', $1, 'meshcore/LHR/b/packets', $4, ' LHR '),
        ('2 days', $2::text, 'meshcore/LHR/f/packets', $5::text, 'LHR'),
        ('3 minutes', $2, 'meshcore/unknown/a/packets', $3, NULL),
        ('1 second', repeat('6', 64), 'meshcore/LHR/invalid/packets', 'invalid', 'LHR'),
        ('1 second', repeat('7', 64), 'meshcore-test/LHR/a/packets', $3, 'LHR')
      ) AS fixture(age, packet_hash, topic, observer, iata)`,
    [key('1'), key('2'), key('A'), key('B'), key('F')]);
    const chart = await repository.fetchChartsData('ukmesh', undefined);
    const times = (await client.query(`SELECT (NOW() - INTERVAL '1 second')::text AS lhr,
      (NOW() - INTERVAL '3 minutes')::text AS unknown`)).rows[0];
    assert.deepEqual(chart.orSummaryResult.rows, [
      { iata: 'LHR', packets_24h: '1', packets_7d: '2', active_observers: '1', observers: '2', last_packet_at: times.lhr },
      { iata: 'UNK', packets_24h: '1', packets_7d: '1', active_observers: '0', observers: '1', last_packet_at: times.unknown },
    ]);
    const aliasChart = await repository.fetchChartsData('ukmesh', key('B'));
    assert.deepEqual(aliasChart.orSummaryResult.rows.map(({ iata, packets_7d, observers }) => [iata, packets_7d, observers]),
      [['LHR', '1', '1'], ['UNK', '1', '1']]);
  } finally {
    await client.query('ROLLBACK');
    client.release();
  }
});
