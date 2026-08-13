import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { networkFilters } from '../api/utils/networkFilters.js';
import {
  deleteExpiredRows,
  RETENTION_TARGETS,
} from '../health/status.js';
import { backfillMultibytePathFacts } from '../stats/multibytePathFacts.js';
import { createStatsRepository } from '../stats/statsRepository.js';
import {
  NONEMPTY_PRIVATE_PREFIX_SUPERSESSION_APPROVAL,
  loadMigrationFiles,
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

test('base schema and every migration apply to a brand-new database', {
  skip: databaseUrl ? false : 'TEST_INGEST_DATABASE_URL is not configured',
}, async (t) => {
  const adminPool = new Pool({
    connectionString: databaseUrl,
    application_name: 'meshcore-fresh-migrations-admin-test',
    max: 1,
  });
  const databaseName = `meshcore_fresh_${process.pid}_${Date.now()}`;
  const databaseIdentifier = `"${databaseName}"`;
  const freshUrl = new URL(databaseUrl as string);
  freshUrl.pathname = `/${databaseName}`;
  let freshPool: pg.Pool | null = null;

  t.after(async () => {
    await freshPool?.end();
    await adminPool.query(`DROP DATABASE IF EXISTS ${databaseIdentifier} WITH (FORCE)`);
    await adminPool.end();
  });

  await adminPool.query(`CREATE DATABASE ${databaseIdentifier}`);
  freshPool = new Pool({
    connectionString: freshUrl.toString(),
    application_name: 'meshcore-fresh-migrations-test',
    max: 1,
  });
  const baseSql = fs.readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8');
  await freshPool.query(baseSql);

  // 050 used to COMMIT inside the runner transaction. Inject a failure at its
  // ledger insert and prove the table plus policies are now rolled back.
  let injectedLedgerFailure = false;
  const rollbackProbePool = {
    connect: async () => {
      const client = await freshPool!.connect();
      return {
        query: async (text: string, params?: unknown[]) => {
          if (
            !injectedLedgerFailure
            && text.includes('INSERT INTO schema_migrations')
            && params?.[0] === '050_packet_paths_and_retention.sql'
          ) {
            injectedLedgerFailure = true;
            throw new Error('INJECTED_MIGRATION_LEDGER_FAILURE');
          }
          return client.query(text, params);
        },
        release: () => client.release(),
      };
    },
  } as unknown as pg.Pool;
  await assert.rejects(
    runMigrations(rollbackProbePool),
    /INJECTED_MIGRATION_LEDGER_FAILURE/,
  );
  const rolledBack050 = await freshPool.query<{
    packet_paths: string | null;
    retention_jobs: string;
    ledger_rows: string;
  }>(`
    SELECT to_regclass('public.packet_paths')::text AS packet_paths,
           (SELECT COUNT(*)::text
              FROM timescaledb_information.jobs
             WHERE proc_name = 'policy_retention') AS retention_jobs,
           (SELECT COUNT(*)::text
              FROM schema_migrations
             WHERE name = '050_packet_paths_and_retention.sql') AS ledger_rows
  `);
  assert.deepEqual(rolledBack050.rows[0], {
    packet_paths: null,
    retention_jobs: '0',
    ledger_rows: '0',
  });

  const executed = await runMigrations(freshPool);
  assert.deepEqual(executed, [
    '050_packet_paths_and_retention.sql',
    '051_reset_readiness.sql',
  ]);
  assert.deepEqual(await runMigrations(freshPool), []);

  const ledger = await freshPool.query<{
    migration_count: string;
    missing_checksums: string;
  }>(`
    SELECT COUNT(*)::text AS migration_count,
           COUNT(*) FILTER (WHERE checksum_sha256 IS NULL)::text AS missing_checksums
      FROM schema_migrations
  `);
  assert.deepEqual(ledger.rows[0], {
    migration_count: String(loadMigrationFiles().length),
    missing_checksums: '0',
  });

  const hypertables = await freshPool.query<{
    hypertable_name: string;
    time_interval: string;
    compression_enabled: boolean;
  }>(`
    SELECT hypertable.hypertable_name,
           dimension.time_interval::text,
           hypertable.compression_enabled
      FROM timescaledb_information.hypertables hypertable
      JOIN timescaledb_information.dimensions dimension
        USING (hypertable_schema, hypertable_name)
     WHERE hypertable.hypertable_schema = 'public'
       AND dimension.dimension_number = 1
     ORDER BY hypertable.hypertable_name
  `);
  assert.deepEqual(hypertables.rows, [
    { hypertable_name: 'node_neighbor_samples', time_interval: '7 days', compression_enabled: true },
    { hypertable_name: 'node_status_samples', time_interval: '7 days', compression_enabled: true },
    { hypertable_name: 'packet_paths', time_interval: '7 days', compression_enabled: true },
    { hypertable_name: 'packets', time_interval: '1 day', compression_enabled: true },
  ]);

  const jobs = await freshPool.query<{
    hypertable_name: string;
    proc_name: string;
    interval_value: string;
  }>(`
    SELECT hypertable_name,
           proc_name,
           COALESCE(config->>'compress_after', config->>'drop_after') AS interval_value
      FROM timescaledb_information.jobs
     WHERE hypertable_schema = 'public'
       AND proc_name IN ('policy_compression', 'policy_retention')
     ORDER BY hypertable_name, proc_name
  `);
  assert.deepEqual(jobs.rows, [
    { hypertable_name: 'node_neighbor_samples', proc_name: 'policy_compression', interval_value: '1 day' },
    { hypertable_name: 'node_neighbor_samples', proc_name: 'policy_retention', interval_value: '7 days' },
    { hypertable_name: 'node_status_samples', proc_name: 'policy_compression', interval_value: '14 days' },
    { hypertable_name: 'node_status_samples', proc_name: 'policy_retention', interval_value: '180 days' },
    { hypertable_name: 'packet_paths', proc_name: 'policy_compression', interval_value: '14 days' },
    { hypertable_name: 'packets', proc_name: 'policy_compression', interval_value: '14 days' },
    { hypertable_name: 'packets', proc_name: 'policy_retention', interval_value: '30 days' },
  ]);
  assert.equal(jobs.rows.some(
    (job) => job.hypertable_name === 'packet_paths' && job.proc_name === 'policy_retention',
  ), false);

  const matviews = await freshPool.query<{ matviewname: string; ispopulated: boolean }>(`
    SELECT matviewname, ispopulated
      FROM pg_matviews
     WHERE schemaname = 'public'
       AND matviewname IN ('node_identity_links', 'node_identity_sightings')
     ORDER BY matviewname
  `);
  assert.deepEqual(matviews.rows, [
    { matviewname: 'node_identity_links', ispopulated: true },
    { matviewname: 'node_identity_sightings', ispopulated: true },
  ]);
  const identityIndexes = await freshPool.query<{ indexname: string }>(`
    SELECT indexname
      FROM pg_indexes
     WHERE schemaname = 'public'
       AND tablename IN ('node_identity_links', 'node_identity_sightings')
     ORDER BY indexname
  `);
  assert.deepEqual(identityIndexes.rows.map((row) => row.indexname), [
    'node_identity_links_node_a_idx',
    'node_identity_links_node_b_idx',
    'node_identity_links_pair_uidx',
    'node_identity_sightings_network_node_idx',
    'node_identity_sightings_node_network_uidx',
  ]);
  await freshPool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY node_identity_links');
  await freshPool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY node_identity_sightings');

  const packetPathSchema = await freshPool.query<{
    nullable_observer: boolean;
    unique_key: boolean;
    privacy_indexes: string;
    decryption_index: string | null;
  }>(`
    SELECT
      (SELECT is_nullable = 'YES'
         FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'packet_paths'
          AND column_name = 'rx_node_id') AS nullable_observer,
      to_regclass('public.packet_paths_observation_key_uidx') IS NOT NULL AS unique_key,
      (SELECT COUNT(*)::text FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'packet_paths'
          AND indexname IN (
            'packet_paths_rx_privacy_idx',
            'packet_paths_src_privacy_idx',
            'packet_paths_path_prefix_privacy_idx'
          )) AS privacy_indexes,
      to_regclass('public.packet_decryptions_created_at_idx')::text AS decryption_index
  `);
  assert.deepEqual(packetPathSchema.rows[0], {
    nullable_observer: true,
    unique_key: true,
    privacy_indexes: '3',
    decryption_index: 'packet_decryptions_created_at_idx',
  });

  const compatibility = await freshPool.query<{
    migration_name: string;
    disposition: string;
    replacement_name: string | null;
  }>(`
    SELECT migration_name, disposition, replacement_name
    FROM schema_migration_compatibility
    WHERE migration_name IN (
      '011_invalidate_pre_visibility_path_cache.sql',
      '015_public_visibility_generation.sql'
    )
    ORDER BY migration_name
  `);
  assert.deepEqual(compatibility.rows, [
    {
      migration_name: '011_invalidate_pre_visibility_path_cache.sql',
      disposition: 'superseded-empty',
      replacement_name: '044_health_current_remove_path_history.sql',
    },
    {
      migration_name: '015_public_visibility_generation.sql',
      disposition: 'superseded-empty',
      replacement_name: '044_health_current_remove_path_history.sql',
    },
  ]);
});

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
    CREATE TABLE path_history_cache (
      packet_hash TEXT PRIMARY KEY
    );
    CREATE TABLE schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    INSERT INTO schema_migrations (name)
    VALUES ('011_invalidate_pre_visibility_path_cache.sql'),
           ('015_public_visibility_generation.sql'),
           ('016_stale_mqtt_observer_cleanup.sql')
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
  const retiredPathCache = await pool.query<{ exists: string | null }>(
    "SELECT to_regclass('public.path_history_cache')::text AS exists",
  );
  assert.equal(retiredPathCache.rows[0]?.exists, null);

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
    if (text.includes('jsonb_agg(jsonb_build_object')) {
      // Private-prefix cache refresh is bookkeeping, not part of the atomic
      // batch statement (same convention as packetBatch.test.ts).
      return pool.query(text, params);
    }
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
    pathHashes: [sourceId.slice(0, 4), observerId.slice(0, 4)],
    pathHashSizeBytes: 2,
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

    const durablePaths = await pool.query<{
      path_count: string;
      shared_observation_ids: string;
    }>(`
      SELECT COUNT(*)::text AS path_count,
             COUNT(*) FILTER (
               WHERE path.observation_id = packet.observation_id
             )::text AS shared_observation_ids
        FROM packet_paths path
        JOIN packets packet
          ON packet.time = path.time
         AND packet.packet_hash = path.packet_hash
         AND packet.network = path.network
         AND packet.rx_node_id IS NOT DISTINCT FROM path.rx_node_id
         AND packet.topic = path.topic
       WHERE path.network = $1 AND path.packet_hash = ANY($2::text[])
    `, [network, packetHashes]);
    assert.deepEqual(durablePaths.rows[0], {
      path_count: '3',
      shared_observation_ids: '3',
    });

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

    await pool.query(
      `UPDATE nodes SET name = 'integration observer 🚫' WHERE node_id = $1`,
      [observerId],
    );
    const privateBeforeRetention = await pool.query<{
      private_packets: string;
      private_paths: string;
    }>(`
      SELECT
        (SELECT COUNT(*)::text FROM packets
          WHERE network = $1 AND is_private AND NOT visibility_ok) AS private_packets,
        (SELECT COUNT(*)::text FROM packet_paths
          WHERE network = $1 AND is_private AND NOT visibility_ok) AS private_paths
    `, [network]);
    assert.deepEqual(privateBeforeRetention.rows[0], {
      private_packets: '3',
      private_paths: '3',
    });

    await pool.query(
      `UPDATE nodes SET name = 'integration observer' WHERE node_id = $1`,
      [observerId],
    );
    const publicBeforeRetention = await pool.query<{
      public_packets: string;
      public_paths: string;
    }>(`
      SELECT
        (SELECT COUNT(*)::text FROM packets
          WHERE network = $1 AND NOT is_private AND visibility_ok) AS public_packets,
        (SELECT COUNT(*)::text FROM packet_paths
          WHERE network = $1 AND NOT is_private AND visibility_ok) AS public_paths
    `, [network]);
    assert.deepEqual(publicBeforeRetention.rows[0], {
      public_packets: '3',
      public_paths: '3',
    });

    const pathObservationIds = await pool.query<{ observation_id: string }>(
      `SELECT observation_id::text
         FROM packet_paths
        WHERE network = $1
        ORDER BY time`,
      [network],
    );
    await pool.query('DELETE FROM packets WHERE network = $1', [network]);
    await pool.query(
      `UPDATE nodes SET name = 'integration observer 🚫' WHERE node_id = $1`,
      [observerId],
    );
    const privateAfterRetention = await pool.query<{
      packet_count: string;
      private_paths: string;
    }>(`
      SELECT
        (SELECT COUNT(*)::text FROM packets WHERE network = $1) AS packet_count,
        (SELECT COUNT(*)::text FROM packet_paths
          WHERE network = $1 AND is_private AND NOT visibility_ok) AS private_paths
    `, [network]);
    assert.deepEqual(privateAfterRetention.rows[0], {
      packet_count: '0',
      private_paths: '3',
    });
    await pool.query(
      `UPDATE nodes SET name = 'integration observer' WHERE node_id = $1`,
      [observerId],
    );

    const visibility = await pool.query<{ generation: string }>(
      `SELECT generation::text FROM public_visibility_state WHERE singleton = TRUE`,
    );
    const factResult = await backfillMultibytePathFacts(
      (text, params) => pool.query(text, params),
      {
        windowStart: new Date(now - 1_000),
        cutoff: new Date(now + 3_000),
        visibilityGeneration: Number(visibility.rows[0]?.generation),
      },
    );
    assert.equal(factResult.affectedRows, 3);
    const postRetentionFacts = await pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count
         FROM multibyte_path_facts
        WHERE observation_id = ANY($1::uuid[])`,
      [pathObservationIds.rows.map((row) => row.observation_id)],
    );
    assert.equal(postRetentionFacts.rows[0]?.count, '3');

    const nullablePathHash = `nullable-path-${suffix}`;
    await pool.query(
      `INSERT INTO packet_paths (
         time, packet_hash, rx_node_id, src_node_id, topic, path_hashes,
         path_hash_size_bytes, network, observation_id
       ) VALUES (
         $1, $2, NULL, $3, $4, $5::text[], 2, $6, gen_random_uuid()
       ) ON CONFLICT DO NOTHING`,
      [new Date(now + 10_000), nullablePathHash, sourceId, `meshcore/ZZZ/path`, [sourceId.slice(0, 4)], network],
    );
    await pool.query(
      `INSERT INTO packet_paths (
         time, packet_hash, rx_node_id, src_node_id, topic, path_hashes,
         path_hash_size_bytes, network, observation_id
       ) VALUES (
         $1, $2, NULL, $3, $4, $5::text[], 2, $6, gen_random_uuid()
       ) ON CONFLICT DO NOTHING`,
      [new Date(now + 10_000), nullablePathHash, sourceId, `meshcore/ZZZ/path`, [sourceId.slice(0, 4)], network],
    );
    const nullablePaths = await pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM packet_paths
        WHERE network = $1 AND packet_hash = $2 AND rx_node_id IS NULL`,
      [network, nullablePathHash],
    );
    assert.equal(nullablePaths.rows[0]?.count, '1');

    const retentionTarget = (table: string) => {
      const target = RETENTION_TARGETS.find((candidate) => candidate.table === table);
      assert.ok(target, `missing retention target ${table}`);
      return target;
    };
    const executeRetention = (target: ReturnType<typeof retentionTarget>) =>
      deleteExpiredRows(target, (text, params) => pool.query(text, params));

    await pool.query(
      `INSERT INTO packet_decryptions (packet_hash, decrypted, summary, created_at)
       VALUES ($1, '{}'::jsonb, '', NOW() - INTERVAL '31 days'),
              ($2, '{}'::jsonb, '', NOW() - INTERVAL '29 days')`,
      [`expired-decryption-${suffix}`, `retained-decryption-${suffix}`],
    );
    await executeRetention(retentionTarget('packet_decryptions'));
    const decryptionRetention = await pool.query<{ packet_hash: string }>(
      `SELECT packet_hash FROM packet_decryptions
        WHERE packet_hash = ANY($1::text[]) ORDER BY packet_hash`,
      [[`expired-decryption-${suffix}`, `retained-decryption-${suffix}`]],
    );
    assert.deepEqual(decryptionRetention.rows, [
      { packet_hash: `retained-decryption-${suffix}` },
    ]);
    await pool.query('SET enable_seqscan = off');
    const decryptionPlan = await pool.query<{ 'QUERY PLAN': string }>(
      `EXPLAIN (COSTS OFF)
       SELECT tableoid, ctid FROM packet_decryptions
        WHERE created_at < NOW() - INTERVAL '30 days'
        ORDER BY created_at ASC, tableoid, ctid LIMIT 2000`,
    );
    assert.match(
      decryptionPlan.rows.map((row) => row['QUERY PLAN']).join('\n'),
      /packet_decryptions_created_at_idx/,
    );
    await pool.query('RESET enable_seqscan');

    const registrationPrefix = suffix.padStart(64, 'E').slice(0, 64);
    await pool.query(
      `INSERT INTO observer_registration_requests
         (public_key, iata, contact, status, created_at, updated_at)
       VALUES
         ($1, 'ZZZ', 'terminal@example.invalid', 'rejected',
          NOW() - INTERVAL '366 days', NOW() - INTERVAL '366 days'),
         ($2, 'ZZZ', 'pending@example.invalid', 'pending',
          NOW() - INTERVAL '366 days', NOW() - INTERVAL '366 days')`,
      [`E${registrationPrefix.slice(1)}`, `F${registrationPrefix.slice(1)}`],
    );
    await executeRetention(retentionTarget('observer_registration_requests'));
    const registrationRetention = await pool.query<{ status: string }>(
      `SELECT status FROM observer_registration_requests
        WHERE contact IN ('terminal@example.invalid', 'pending@example.invalid')
        ORDER BY status`,
    );
    assert.deepEqual(registrationRetention.rows, [{ status: 'pending' }]);

    await pool.query(
      `INSERT INTO operator_audit_events
         (actor, action, target_type, target_id, idempotency_key, status, created_at)
       VALUES
         ('integration', 'test', 'reset', $1, $2, 'succeeded', NOW() - INTERVAL '731 days'),
         ('integration', 'test', 'reset', $3, $4, 'succeeded', NOW() - INTERVAL '729 days')`,
      [suffix, `expired-audit-${suffix}`, `${suffix}-retained`, `retained-audit-${suffix}`],
    );
    await executeRetention(retentionTarget('operator_audit_events'));
    const auditRetention = await pool.query<{ idempotency_key: string }>(
      `SELECT idempotency_key FROM operator_audit_events
        WHERE idempotency_key = ANY($1::text[]) ORDER BY idempotency_key`,
      [[`expired-audit-${suffix}`, `retained-audit-${suffix}`]],
    );
    assert.deepEqual(auditRetention.rows, [
      { idempotency_key: `retained-audit-${suffix}` },
    ]);

    await pool.query(
      `INSERT INTO packets (time, packet_hash, topic, network)
       VALUES (NOW() - INTERVAL '30 days 1 minute', $1, 'meshcore/ZZZ/expired', $3),
              (NOW() - INTERVAL '29 days 23 hours', $2, 'meshcore/ZZZ/retained', $3)`,
      [`expired-packet-${suffix}`, `retained-packet-${suffix}`, network],
    );
    await executeRetention(retentionTarget('packets'));
    const packetBoundary = await pool.query<{
      packet_hash: string;
      within_bound: boolean;
    }>(
      `SELECT packet_hash, time >= NOW() - INTERVAL '30 days' AS within_bound
         FROM packets WHERE network = $1 ORDER BY packet_hash`,
      [network],
    );
    assert.deepEqual(packetBoundary.rows, [
      { packet_hash: `retained-packet-${suffix}`, within_bound: true },
    ]);
  } finally {
    await pool.query('DELETE FROM packets WHERE network = $1', [network]);
    await pool.query('DELETE FROM multibyte_path_facts WHERE network = $1', [network]);
    await pool.query('DELETE FROM packet_paths WHERE network = $1', [network]);
    await pool.query('DELETE FROM packet_hourly_stats WHERE network = $1', [network]);
    await pool.query('DELETE FROM packet_daily_stats WHERE network = $1', [network]);
    await pool.query('DELETE FROM observer_region_packet_sightings WHERE network = $1', [network]);
    await pool.query('DELETE FROM observer_region_observer_sightings WHERE network = $1', [network]);
    await pool.query('DELETE FROM node_network_sightings WHERE network = $1', [network]);
    await pool.query(
      `DELETE FROM packet_decryptions WHERE packet_hash LIKE $1`,
      [`%-${suffix}`],
    );
    await pool.query(
      `DELETE FROM observer_registration_requests
        WHERE contact IN ('terminal@example.invalid', 'pending@example.invalid')`,
    );
    await pool.query(
      `DELETE FROM operator_audit_events WHERE idempotency_key LIKE $1`,
      [`%-${suffix}`],
    );
    await pool.query('DELETE FROM nodes WHERE node_id = $1', [observerId]);
  }
});
