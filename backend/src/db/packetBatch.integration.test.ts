import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { networkFilters } from '../api/utils/networkFilters.js';
import { createStatsRepository } from '../stats/statsRepository.js';
import {
  NONEMPTY_PRIVATE_PREFIX_SUPERSESSION_APPROVAL,
  runMigrations,
} from './migrations.js';
import {
  configurePacketBatch,
  enqueuePacket,
  flush,
  type PacketBatchInput,
} from './packetBatch.js';

const { Pool } = pg;
const databaseUrl = process.env['TEST_INGEST_DATABASE_URL'];

test('packet batch atomically coalesces observer, sighting, and stats writes', {
  skip: databaseUrl ? false : 'TEST_INGEST_DATABASE_URL is not configured',
}, async (t) => {
  const pool = new Pool({
    connectionString: databaseUrl,
    application_name: 'meshcore-ingest-integration-test',
    max: 2,
  });
  t.after(() => pool.end());

  const baseSql = fs.readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8');
  await pool.query(baseSql);
  await pool.query(`
    CREATE TABLE schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    INSERT INTO schema_migrations (name)
    VALUES ('016_stale_mqtt_observer_cleanup.sql')
  `);
  const compatibilityNode = 'migration-compatibility-fixture';
  await pool.query(
    `INSERT INTO nodes (node_id, name, network)
     VALUES ($1, 'migration compatibility fixture', 'ukmesh')`,
    [compatibilityNode],
  );
  const previousApproval = process.env['MIGRATION_016_PRIVATE_PREFIXES_APPROVAL'];
  process.env['MIGRATION_016_PRIVATE_PREFIXES_APPROVAL'] =
    NONEMPTY_PRIVATE_PREFIX_SUPERSESSION_APPROVAL;
  let migrationRuns: string[][];
  try {
    migrationRuns = await Promise.all([
      runMigrations(pool),
      runMigrations(pool),
    ]);
  } finally {
    if (previousApproval === undefined) {
      delete process.env['MIGRATION_016_PRIVATE_PREFIXES_APPROVAL'];
    } else {
      process.env['MIGRATION_016_PRIVATE_PREFIXES_APPROVAL'] = previousApproval;
    }
  }
  await pool.query('DELETE FROM nodes WHERE node_id = $1', [compatibilityNode]);
  const migrationRunLengths = migrationRuns.map((run) => run.length).sort((a, b) => a - b);
  assert.equal(migrationRunLengths[0], 0);
  assert.ok((migrationRunLengths[1] ?? 0) > 0);
  assert.ok(migrationRuns.flat().some(
    (name) => name.startsWith('016_private_prefixes.sql -> 026_private_visibility_schema.sql'),
  ));

  // Simulate a process loss after the idempotent CREATE INDEX CONCURRENTLY
  // succeeded but before its non-transactional ledger insert committed. The
  // next runner must safely repeat the SQL and restore the ledger.
  await pool.query(
    `DELETE FROM schema_migrations
      WHERE name = '028_ml_cursor_index.sql'`,
  );
  assert.deepEqual(await runMigrations(pool), ['028_ml_cursor_index.sql']);
  assert.deepEqual(await runMigrations(pool), []);
  const migrationLedger = await pool.query<{
    missing_checksums: string;
    non_transactional_count: string;
    superseded_existing_count: string;
  }>(`
    SELECT
      COUNT(*) FILTER (WHERE checksum_sha256 IS NULL)::text AS missing_checksums,
      COUNT(*) FILTER (WHERE execution_mode = 'non-transactional')::text
        AS non_transactional_count,
      COUNT(*) FILTER (WHERE execution_mode = 'superseded-existing')::text
        AS superseded_existing_count
      FROM schema_migrations
  `);
  assert.deepEqual(migrationLedger.rows[0], {
    missing_checksums: '0',
    non_transactional_count: '1',
    superseded_existing_count: '2',
  });

  const suffix = `${process.pid}${Date.now()}`.slice(-12);
  const legacyPrivateNode = `C${suffix.padStart(63, 'C')}`.slice(0, 64);
  const legacyPublicNode = `D${suffix.padStart(63, 'D')}`.slice(0, 64);
  const legacyHashes = {
    private: `legacy-private-${suffix}`,
    test: `legacy-test-${suffix}`,
    public: `legacy-public-${suffix}`,
  };
  await pool.query(
    `INSERT INTO nodes (node_id, name, network)
     VALUES ($1, 'private fixture 🚫', 'ukmesh'),
            ($2, 'public fixture', 'ukmesh')`,
    [legacyPrivateNode, legacyPublicNode],
  );
  await pool.query(
    `INSERT INTO packets
       (time, packet_hash, rx_node_id, src_node_id, topic, network)
     VALUES
       (NOW(), $1, $2, $3, 'meshcore/ZZZ/private', 'ukmesh'),
       (NOW(), $4, $3, $3, 'meshcore-test/ZZZ/test', 'ukmesh'),
       (NOW(), $5, $3, $3, 'meshcore/ZZZ/public', 'ukmesh')`,
    [
      legacyHashes.private,
      legacyPrivateNode,
      legacyPublicNode,
      legacyHashes.test,
      legacyHashes.public,
    ],
  );
  const legacyFilters = networkFilters('ukmesh');
  const legacyVisible = await pool.query<{ packet_hash: string }>(
    `SELECT packet_hash
       FROM packets
      WHERE packet_hash = ANY($2::text[])
      ${legacyFilters.packets}
      ORDER BY packet_hash`,
    [legacyFilters.params[0], Object.values(legacyHashes)],
  );
  assert.deepEqual(legacyVisible.rows, [{ packet_hash: legacyHashes.public }]);
  await pool.query(
    'DELETE FROM packets WHERE packet_hash = ANY($1::text[])',
    [Object.values(legacyHashes)],
  );
  await pool.query(
    'DELETE FROM nodes WHERE node_id = ANY($1::text[])',
    [[legacyPrivateNode, legacyPublicNode]],
  );

  const network = `ingest_${suffix}`;
  const observerId = `A${suffix.padStart(63, 'A')}`.slice(0, 64);
  const sourceId = `B${suffix.padStart(63, 'B')}`.slice(0, 64);
  const packetHashes = [0, 1, 2].map(
    (index) => `packet-${suffix}-${index}`,
  );
  const now = Date.now();
  let statementCount = 0;
  configurePacketBatch(async (text, params) => {
    statementCount += 1;
    return pool.query(text, params);
  });

  const makePacket = (index: number, hopCount: number): PacketBatchInput => ({
    time: new Date(now + index * 1_000),
    packetHash: packetHashes[index]!,
    rxNodeId: observerId,
    srcNodeId: sourceId,
    topic: `meshcore/ZZZ/${observerId}/packets`,
    topicPrefix: 'meshcore',
    iata: 'ZZZ',
    packetType: 4,
    routeType: 0,
    hopCount,
    rssi: -90 + index,
    snr: 5 + index,
    payloadJson: JSON.stringify({ index }),
    companionSender: null,
    rawHex: `0${index}`,
    advertCount: null,
    pathHashes: null,
    pathHashSizeBytes: null,
    network,
    transportCodes: null,
    regionScope: null,
  });

  try {
    const writes = [
      enqueuePacket(makePacket(0, 1)),
      enqueuePacket(makePacket(1, 5)),
      enqueuePacket(makePacket(2, 3)),
    ];
    await flush();
    assert.deepEqual(await Promise.all(writes), [
      { isPrivate: false, visibilityOk: true },
      { isPrivate: false, visibilityOk: true },
      { isPrivate: false, visibilityOk: true },
    ]);
    assert.equal(statementCount, 1);

    const packets = await pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count
         FROM packets
        WHERE network = $1 AND packet_hash = ANY($2::text[])`,
      [network, packetHashes],
    );
    assert.equal(packets.rows[0]?.count, '3');

    const observer = await pool.query<{
      iata: string;
      observer_iata: string;
      network: string;
      is_online: boolean;
      last_seen: Date;
      last_rx_at: Date;
      last_mqtt_observer_seen_at: Date;
    }>(
      `SELECT iata, observer_iata, network, is_online,
              last_seen, last_rx_at, last_mqtt_observer_seen_at
         FROM nodes
        WHERE node_id = $1`,
      [observerId],
    );
    assert.equal(observer.rows.length, 1);
    assert.deepEqual(
      {
        iata: observer.rows[0]?.iata,
        observerIata: observer.rows[0]?.observer_iata,
        network: observer.rows[0]?.network,
        online: observer.rows[0]?.is_online,
      },
      { iata: 'ZZZ', observerIata: 'ZZZ', network, online: true },
    );
    assert.equal(observer.rows[0]?.last_seen.getTime(), now + 2_000);
    assert.equal(observer.rows[0]?.last_rx_at.getTime(), now + 2_000);
    assert.equal(observer.rows[0]?.last_mqtt_observer_seen_at.getTime(), now + 2_000);

    const sighting = await pool.query<{
      first_seen_at: Date;
      last_seen_at: Date;
    }>(
      `SELECT first_seen_at, last_seen_at
         FROM node_network_sightings
        WHERE node_id = $1 AND network = $2`,
      [sourceId, network],
    );
    assert.equal(sighting.rows[0]?.first_seen_at.getTime(), now);
    assert.equal(sighting.rows[0]?.last_seen_at.getTime(), now + 2_000);

    const daily = await pool.query<{ max_hop_count: number; max_hop_hash: string }>(
      `SELECT max_hop_count, max_hop_hash
         FROM packet_daily_stats
        WHERE network = $1 AND day = $2::date`,
      [network, new Date(now).toISOString()],
    );
    assert.deepEqual(daily.rows[0], {
      max_hop_count: 5,
      max_hop_hash: packetHashes[1],
    });

    const hourly = await pool.query<{
      packet_count: string;
      highest_hop: number;
    }>(
      `SELECT SUM(packet_count)::text AS packet_count,
              MAX(hop_count)::int AS highest_hop
         FROM packet_hourly_stats
        WHERE network = $1`,
      [network],
    );
    assert.deepEqual(hourly.rows[0], {
      packet_count: '3',
      highest_hop: 5,
    });

    const rollups = await pool.query<{
      packet_sightings: string;
      observer_sightings: string;
    }>(
      `SELECT
         (SELECT COUNT(*)::text
            FROM observer_region_packet_sightings
           WHERE network = $1 AND iata = 'ZZZ') AS packet_sightings,
         (SELECT COUNT(*)::text
            FROM observer_region_observer_sightings
           WHERE network = $1 AND iata = 'ZZZ') AS observer_sightings`,
      [network],
    );
    assert.deepEqual(rollups.rows[0], {
      packet_sightings: '3',
      observer_sightings: '1',
    });

    const shadowWarnings: unknown[][] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => {
      if (args[0] === '[stats-aggregate-shadow] mismatch') shadowWarnings.push(args);
      else originalWarn(...args);
    };
    try {
      const repository = createStatsRepository({
        query: (text, params) => pool.query(text, params),
        networkFilters,
        aggregateReadsEnabled: true,
        aggregateShadowEnabled: true,
      });
      await repository.fetchChartsData(network, undefined);
    } finally {
      console.warn = originalWarn;
    }
    assert.deepEqual(shadowWarnings, []);
  } finally {
    await pool.query('DELETE FROM packets WHERE network = $1', [network]);
    await pool.query('DELETE FROM packet_hourly_stats WHERE network = $1', [network]);
    await pool.query('DELETE FROM packet_daily_stats WHERE network = $1', [network]);
    await pool.query('DELETE FROM observer_region_packet_sightings WHERE network = $1', [network]);
    await pool.query('DELETE FROM observer_region_observer_sightings WHERE network = $1', [network]);
    await pool.query('DELETE FROM node_network_sightings WHERE network = $1', [network]);
    await pool.query('DELETE FROM nodes WHERE node_id = $1', [observerId]);
  }
});
