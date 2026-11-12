/** Explicit local fixture maintenance; never invoked by application startup. */
import pg from 'pg';
import { pathToFileURL } from 'node:url';
import { assertLocalDatabaseUrl } from './sampleData.js';
import { convergePacketVisibility } from '../privacy/convergence.js';
import { refreshNodeIdentityAliases } from '../db/nodeIdentity.js';
import { backfillMultibyteObservationIds, backfillMultibytePathFacts } from '../stats/multibytePathFacts.js';
import { backfillPackets } from './backfillPacketIdentity.js';

export async function prepareSampleDataset(databaseUrl: string): Promise<void> {
  assertLocalDatabaseUrl(databaseUrl);
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 2, statement_timeout: 900_000 });
  try {
    // Freeze local lifecycle/background policies for comparable runs. This does
    // not add, remove or activate compression/retention on any external DB.
    const jobs = await pool.query<{ job_id: number }>('SELECT job_id FROM timescaledb_information.jobs WHERE job_id >= 1000');
    for (const { job_id } of jobs.rows) await pool.query('SELECT alter_job($1, scheduled => false)', [job_id]);
    await pool.query(`INSERT INTO mesh.ingest_observation_ledger (observation_id, observed_at, packet_hash, network)
      SELECT observation_id, time, packet_hash, network FROM mesh.packets WHERE observation_id IS NOT NULL
      ON CONFLICT (observation_id) DO NOTHING`);
    await pool.query(`INSERT INTO mesh.packet_paths (
      time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix, route_type,
      hop_count, rssi, snr, path_hashes, path_hash_size_bytes, is_private, visibility_ok, network, observation_id
    ) SELECT time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix, route_type,
      hop_count, rssi, snr, path_hashes, path_hash_size_bytes, is_private, visibility_ok, network, observation_id
      FROM mesh.packets WHERE path_hashes IS NOT NULL AND path_hash_size_bytes > 1
      ON CONFLICT DO NOTHING`);
    await pool.query(`INSERT INTO core.node_network_sightings (node_id, network, first_seen_at, last_seen_at)
      SELECT node_id, network, MIN(time), MAX(time) FROM (
        SELECT rx_node_id AS node_id, network, time FROM mesh.packets
        UNION ALL SELECT src_node_id, network, time FROM mesh.packets
      ) seen WHERE node_id IS NOT NULL GROUP BY node_id, network
      ON CONFLICT (node_id, network) DO UPDATE SET
        first_seen_at = LEAST(node_network_sightings.first_seen_at, EXCLUDED.first_seen_at),
        last_seen_at = GREATEST(node_network_sightings.last_seen_at, EXCLUDED.last_seen_at)`);
    await refreshNodeIdentityAliases(pool);
    await pool.query('REFRESH MATERIALIZED VIEW core.node_identity_links');
    await pool.query('REFRESH MATERIALIZED VIEW core.node_identity_sightings');
    const generation = (await pool.query<{ generation: string }>(
      'SELECT generation::text FROM ops.public_visibility_state WHERE singleton = TRUE',
    )).rows[0]?.generation;
    if (!generation) throw new Error('missing fixture visibility generation');
    const visibility = await convergePacketVisibility(pool, { apply: true, expectedGeneration: generation });
    if (visibility.mode !== 'apply') throw new Error('fixture convergence was not applied');
    const current = Number(visibility.after.generation);
    const bounds = (await pool.query<{ first: Date | null; last: Date | null }>(
      'SELECT MIN(time) AS first, MAX(time) AS last FROM mesh.packets',
    )).rows[0]!;
    if (bounds.first && bounds.last) {
      const windowStart = new Date(Math.min(bounds.first.getTime(), Date.now() - 8 * 86_400_000));
      const cutoff = new Date(Math.max(bounds.last.getTime(), Date.now()));
      const query = <T extends pg.QueryResultRow>(sql: string, params?: unknown[]) => pool.query<T>(sql, params);
      while (await backfillMultibyteObservationIds(query, { windowStart, cutoff, batchSize: 5000 })) { /* bounded batches */ }
      await backfillMultibytePathFacts(query, { windowStart, cutoff, visibilityGeneration: current });
      const identity = await backfillPackets(pool, 5000, false, 0, null);
      if (identity.status !== 'packets_complete') throw new Error('fixture packet identity backfill is incomplete');
    }
    const counts = await pool.query(`SELECT 'nodes' AS relation, COUNT(*)::text AS rows FROM core.nodes
      UNION ALL SELECT 'packets', COUNT(*)::text FROM mesh.packets
      UNION ALL SELECT 'paths', COUNT(*)::text FROM mesh.packet_paths
      UNION ALL SELECT 'multibyte_facts', COUNT(*)::text FROM mesh.multibyte_path_facts
      UNION ALL SELECT 'network_sightings', COUNT(*)::text FROM core.node_network_sightings
      UNION ALL SELECT 'identity_sightings', COUNT(*)::text FROM core.node_identity_sightings`);
    console.log(JSON.stringify({ fixtureVersion: 1, generation: current, bounds, counts: counts.rows,
      gaps: ['Rollups and chart snapshots require the explicit stats backfill and API warm-up.'],
      scheduledJobsDisabled: jobs.rows.length }, null, 2));
  } finally { await pool.end(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  prepareSampleDataset(process.env['DATABASE_URL'] ?? '').catch((error: unknown) => {
    console.error('[sample:prepare]', error instanceof Error ? error.message : 'failed');
    process.exitCode = 1;
  });
}
