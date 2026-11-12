/** Load the local UKMesh sample dataset into a migrated database.
 *
 * Usage:
 *   DATABASE_URL=postgresql://... npm run sample:load -- \
 *     --data-dir ../data/ukmesh-sample-2026-11-10 --reset
 *
 * Bulk loads through PostgreSQL COPY into temporary staging tables, then
 * transforms into the application schema. The dataset is local test data and
 * is never committed. This tool never logs row payloads.
 */
import pg from 'pg';
import path from 'node:path';
import { createReadStream } from 'node:fs';
import { createGunzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { pathToFileURL } from 'node:url';
import { from as copyFrom } from 'pg-copy-streams';
import { flag, option, inspectGzipCsv, defaultDataDir, assertLocalDatabaseUrl } from './sampleData.js';

const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

function databaseUrl(args: readonly string[]): string {
  const url = option(args, 'database-url') ?? process.env['DATABASE_URL'];
  if (!url) throw new Error('DATABASE_URL or --database-url is required');
  assertLocalDatabaseUrl(url);
  return url;
}

function dataDir(args: readonly string[]): string {
  return path.resolve(option(args, 'data-dir') ?? defaultDataDir());
}

function csvHeaders(file: string): Promise<string[]> {
  return inspectGzipCsv(file, true).then((table) => table.headers);
}

async function stageCsv(
  client: pg.Client,
  file: string,
  headers: readonly string[],
  stage: string,
): Promise<void> {
  if (headers.length === 0 || headers.some((header) => !IDENTIFIER.test(header))) {
    throw new Error(`${path.basename(file)}: unexpected CSV header`);
  }
  const columns = headers.map((header) => `"${header}"`).join(', ');
  await client.query(`CREATE TEMPORARY TABLE ${stage} (${headers.map((header) => `"${header}" text`).join(', ')})`);
  const copy = client.query(copyFrom(`COPY ${stage} (${columns}) FROM STDIN WITH (FORMAT csv, HEADER true)`));
  await pipeline(createReadStream(file), createGunzip(), copy);
}

async function resetTables(client: pg.Client): Promise<void> {
  await client.query(`TRUNCATE TABLE mesh.packets, mesh.packet_paths, mesh.multibyte_path_facts,
    mesh.ingest_observation_ledger, mesh.ingest_observation_archive, mesh.ingest_observation_tombstones,
    core.node_status_samples, core.node_links, core.nodes CASCADE`);
  await client.query('DELETE FROM mesh.multibyte_path_fact_state');
  await client.query(`TRUNCATE TABLE mesh.packet_daily_stats, mesh.packet_hourly_stats,
    core.node_network_sightings, core.node_identity_aliases, core.node_identity_refresh_state,
    core.observer_region_packet_sightings, core.observer_region_observer_sightings,
    core.region_scope_stats, core.region_scope_members, core.repeater_scope_status,
    core.region_scope_run, analytics.stats_chart_snapshots CASCADE`);
  await client.query('DELETE FROM ops.packet_identity_backfill_state');
  await client.query('INSERT INTO ops.packet_identity_backfill_state (singleton) VALUES (TRUE)');
  await client.query("DELETE FROM ops.maintenance_backfill_checkpoints WHERE job_name = 'packet-hourly-stats-v1'");
  console.log('[sample:load] truncated packets, node_status_samples, node_links, nodes');
}

async function loadPackets(client: pg.Client, file: string, rawFile: string, shiftMs: number, redactContent: boolean): Promise<number> {
  const headers = await csvHeaders(file);
  await stageCsv(client, file, headers, 'stage_packets');
  await stageCsv(client, rawFile, await csvHeaders(rawFile), 'stage_raw');
  const result = await client.query(
    `WITH raw_receptions AS (
       SELECT DISTINCT ON ("time", packet_hash, rx_node_id) "time", packet_hash, rx_node_id, raw_hex
       FROM stage_raw ORDER BY "time", packet_hash, rx_node_id
     ), receptions AS (
       SELECT stage_packets.*, raw_receptions.raw_hex AS imported_raw_hex
       FROM stage_packets LEFT JOIN raw_receptions USING ("time", packet_hash, rx_node_id)
     ) INSERT INTO mesh.packets (
       "time", packet_hash, packet_type, route_type, hop_count,
       path_hash_size_bytes, path_hashes, src_node_id, rx_node_id, rssi, snr,
       iata, network, region_scope, transport_codes, topic, payload,
       topic_prefix, is_private, visibility_ok, raw_hex, observation_id
     )
     SELECT NULLIF("time", '')::timestamptz + ($1::double precision * INTERVAL '1 millisecond'),
            NULLIF(packet_hash, ''),
            NULLIF(packet_type, '')::int,
            NULLIF(route_type, '')::int,
            NULLIF(hop_count, '')::int,
            NULLIF(path_hash_size_bytes, '')::int,
            CASE WHEN COALESCE(path, '') = '' THEN NULL ELSE string_to_array(path, '>') END,
            NULLIF(src_node_id, ''),
            NULLIF(rx_node_id, ''),
            NULLIF(rssi, '')::double precision,
            NULLIF(snr, '')::double precision,
            NULLIF(iata, ''),
            COALESCE(NULLIF(network, ''), 'ukmesh'),
            NULLIF(region_scope, ''),
            NULLIF(transport_codes, ''),
            NULLIF(topic, ''),
            jsonb_build_object('_summary', CASE WHEN $2::boolean
              THEN LEFT(REPEAT('Synthetic fixture text. ', (length(summary) + 23) / 24), length(summary))
              ELSE NULLIF(summary, '') END),
            split_part(COALESCE(topic, ''), '/', 1),
            true,
            false,
            NULLIF(imported_raw_hex, ''),
            gen_random_uuid()
       FROM receptions`,
    [shiftMs, redactContent],
  );
  await client.query('DROP TABLE stage_packets');
  await client.query('DROP TABLE stage_raw');
  return result.rowCount ?? 0;
}

async function loadNodes(client: pg.Client, file: string, shiftMs: number): Promise<number> {
  const headers = await csvHeaders(file);
  const target = await client.query<{ column_name: string; data_type: string }>(
    `SELECT column_name, data_type FROM information_schema.columns
      WHERE table_name = 'nodes' AND table_schema = 'core'`,
  );
  const targetColumns = new Map(target.rows.map((row) => [row.column_name, row.data_type]));
  const selected = headers.filter((header) => IDENTIFIER.test(header) && targetColumns.has(header));
  if (selected.length === 0) throw new Error('nodes: no matching columns');
  const cast = (dataType: string | undefined): string => {
    switch (dataType) {
      case 'integer': return '::integer';
      case 'bigint': return '::bigint';
      case 'smallint': return '::smallint';
      case 'double precision': return '::double precision';
      case 'real': return '::real';
      case 'numeric': return '::numeric';
      case 'boolean': return '::boolean';
      case 'timestamp with time zone': return '::timestamptz';
      case 'jsonb': return '::jsonb';
      default: return '';
    }
  };
  await stageCsv(client, file, headers, 'stage_nodes');
  const hasTimes = selected.some((column) => targetColumns.get(column) === 'timestamp with time zone');
  const result = await client.query(
    `INSERT INTO core.nodes (${selected.map((column) => `"${column}"`).join(', ')})
     SELECT ${selected.map((column) => `NULLIF("${column}", '')${cast(targetColumns.get(column))}`
       + (targetColumns.get(column) === 'timestamp with time zone' ? " + ($1::double precision * INTERVAL '1 millisecond')" : '')).join(', ')}
       FROM stage_nodes
      ON CONFLICT (node_id) DO NOTHING`, hasTimes ? [shiftMs] : [],
  );
  await client.query('DROP TABLE stage_nodes');
  return result.rowCount ?? 0;
}

async function loadNodeLinks(client: pg.Client, file: string, shiftMs: number): Promise<number> {
  const headers = await csvHeaders(file);
  await stageCsv(client, file, headers, 'stage_links');
  const result = await client.query(
    `INSERT INTO core.node_links (
       node_a_id, node_b_id, observed_count, last_observed, itm_path_loss_db,
       itm_viable, count_a_to_b, count_b_to_a, multibyte_observed_count, force_viable
     )
     SELECT NULLIF(node_a_id, ''), NULLIF(node_b_id, ''),
            NULLIF(observed_count, '')::int,
            NULLIF(last_observed, '')::timestamptz + ($1::double precision * INTERVAL '1 millisecond'),
            NULLIF(itm_path_loss_db, '')::double precision,
            NULLIF(itm_viable, '')::boolean,
            NULLIF(count_a_to_b, '')::int,
            NULLIF(count_b_to_a, '')::int,
            NULLIF(multibyte_observed_count, '')::int,
            NULLIF(force_viable, '')::boolean
       FROM stage_links
      ON CONFLICT (node_a_id, node_b_id) DO NOTHING`, [shiftMs],
  );
  await client.query('DROP TABLE stage_links');
  return result.rowCount ?? 0;
}

async function loadStatusSamples(client: pg.Client, file: string, shiftMs: number): Promise<number> {
  const headers = await csvHeaders(file);
  await stageCsv(client, file, headers, 'stage_status');
  const result = await client.query(
    `INSERT INTO core.node_status_samples (
       "time", node_id, network, battery_mv, uptime_secs, tx_air_secs,
       rx_air_secs, channel_utilization, air_util_tx, status, stats
     )
     SELECT NULLIF("time", '')::timestamptz + ($1::double precision * INTERVAL '1 millisecond'),
            NULLIF(node_id, ''),
            COALESCE(NULLIF(network, ''), 'ukmesh'),
            NULLIF(battery_mv, '')::int,
            NULLIF(uptime_secs, '')::bigint,
            NULLIF(tx_air_secs, '')::bigint,
            NULLIF(rx_air_secs, '')::bigint,
            NULLIF(channel_utilization, '')::double precision,
            NULLIF(air_util_tx, '')::double precision,
            NULLIF(status, ''),
            NULLIF(stats, '')::jsonb
       FROM stage_status`, [shiftMs],
  );
  await client.query('DROP TABLE stage_status');
  return result.rowCount ?? 0;
}

async function invalidatePrivacyState(client: pg.Client): Promise<void> {
  const current = await client.query<{ generation: string }>(
    'SELECT generation::text FROM ops.public_visibility_state WHERE singleton = TRUE FOR UPDATE',
  );
  if (!current.rows[0] || BigInt(current.rows[0].generation) < 1n) {
    throw new Error('missing migrated visibility generation');
  }
  // A bulk import is not a privacy convergence certificate. Keep public reads
  // fenced until the explicit fixture preparation verifies every retained row.
  await client.query(
    `INSERT INTO ops.packet_visibility_materialization_state (singleton, visibility_generation)
     VALUES (TRUE, 0)
     ON CONFLICT (singleton) DO UPDATE
       SET visibility_generation = 0,
           updated_at = NOW()`,
  );
}

export async function loadSampleDataset(args: string[]): Promise<void> {
  const directory = dataDir(args);
  const reset = flag(args, 'reset');
  const fast = flag(args, 'fast');
  const dryRun = flag(args, 'dry-run');
  const files = {
    nodes: path.join(directory, 'nodes.csv.gz'),
    packets: path.join(directory, 'packets.csv.gz'),
    raw: path.join(directory, 'raw_packets.csv.gz'),
    links: path.join(directory, 'node_links.csv.gz'),
    status: path.join(directory, 'node_status_samples.csv.gz'),
  };
  const tables = {
    nodes: await inspectGzipCsv(files.nodes),
    packets: await inspectGzipCsv(files.packets),
    raw: await inspectGzipCsv(files.raw),
    links: await inspectGzipCsv(files.links),
    status: await inspectGzipCsv(files.status),
  };
  console.log(
    `[sample:load] rows nodes=${tables.nodes.count} packets=${tables.packets.count}`
      + ` links=${tables.links.count} status=${tables.status.count}`,
  );
  let shiftMs = 0;
  const shiftTo = option(args, 'shift-to');
  if (shiftTo) {
    const anchor = Date.parse(shiftTo);
    const latest = tables.packets.lastTime;
    if (!Number.isFinite(anchor) || latest === null) throw new Error('--shift-to requires an ISO timestamp and dated packets');
    shiftMs = anchor - latest;
    console.log(`[sample:load] local copy time shift ${shiftMs}ms; latest packet ${new Date(anchor).toISOString()}`);
  }
  if (dryRun) return;

  const client = new pg.Client({ connectionString: databaseUrl(args) });
  await client.connect();
  try {
    await client.query('BEGIN');
    if (reset) await resetTables(client);
    // Keep node triggers enabled even in fast mode: they maintain private
    // prefixes and the authoritative visibility generation.
    const nodes = await loadNodes(client, files.nodes, shiftMs);
    if (fast) {
      await client.query('SAVEPOINT fast_mode');
      try {
        await client.query('SET LOCAL session_replication_role = replica');
        console.log('[sample:load] fast mode: triggers and FK checks disabled for this transaction');
      } catch {
        await client.query('ROLLBACK TO SAVEPOINT fast_mode');
        console.warn('[sample:load] fast mode unavailable (superuser required); continuing normally');
      }
      await client.query('RELEASE SAVEPOINT fast_mode');
    }
    const loaded = {
      nodes,
      packets: flag(args, 'omit-packets') ? 0 : await loadPackets(client, files.packets, files.raw, shiftMs, flag(args, 'redact-content')),
      links: await loadNodeLinks(client, files.links, shiftMs),
      status: flag(args, 'omit-packets') ? 0 : await loadStatusSamples(client, files.status, shiftMs),
    };
    await invalidatePrivacyState(client);
    await client.query('ANALYZE core.nodes; ANALYZE mesh.packets; ANALYZE core.node_links; ANALYZE core.node_status_samples');
    await client.query('COMMIT');

    const result = await client.query<{ label: string; rows: string }>(
      `SELECT 'nodes' AS label, COUNT(*)::text AS rows FROM core.nodes
       UNION ALL SELECT 'packets', COUNT(*)::text FROM mesh.packets
       UNION ALL SELECT 'node_links', COUNT(*)::text FROM core.node_links
       UNION ALL SELECT 'node_status_samples', COUNT(*)::text FROM core.node_status_samples`,
    );
    console.log(
      `[sample:load] inserted ${Object.entries(loaded).map(([name, count]) => `${name}=${count}`).join(' ')}`,
    );
    for (const row of result.rows) console.log(`[sample:load] ${row.label}: ${row.rows}`);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    await client.end();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  loadSampleDataset(process.argv.slice(2)).catch((error: unknown) => {
    console.error('[sample:load] failed:', error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
