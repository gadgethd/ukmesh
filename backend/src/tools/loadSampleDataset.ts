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
import { from as copyFrom } from 'pg-copy-streams';
import { flag, option, readGzipCsv, defaultDataDir } from './sampleData.js';

const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

function databaseUrl(args: readonly string[]): string {
  const url = option(args, 'database-url') ?? process.env['DATABASE_URL'];
  if (!url) throw new Error('DATABASE_URL or --database-url is required');
  return url;
}

function dataDir(args: readonly string[]): string {
  return path.resolve(option(args, 'data-dir') ?? defaultDataDir());
}

function csvHeaders(file: string): Promise<string[]> {
  return readGzipCsv(file).then((table) => table.headers);
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
  await client.query('TRUNCATE TABLE packets, node_status_samples, node_links, nodes CASCADE');
  console.log('[sample:load] truncated packets, node_status_samples, node_links, nodes');
}

async function loadPackets(client: pg.Client, file: string): Promise<number> {
  const headers = await csvHeaders(file);
  await stageCsv(client, file, headers, 'stage_packets');
  const result = await client.query(
    `INSERT INTO packets (
       "time", packet_hash, packet_type, route_type, hop_count,
       path_hash_size_bytes, path_hashes, src_node_id, rx_node_id, rssi, snr,
       iata, network, region_scope, transport_codes, topic, payload,
       topic_prefix, is_private, visibility_ok
     )
     SELECT NULLIF("time", '')::timestamptz,
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
            jsonb_build_object('_summary', NULLIF(summary, '')),
            split_part(COALESCE(topic, ''), '/', 1),
            false,
            true
       FROM stage_packets`,
  );
  await client.query('DROP TABLE stage_packets');
  return result.rowCount ?? 0;
}

async function loadNodes(client: pg.Client, file: string): Promise<number> {
  const headers = await csvHeaders(file);
  const target = await client.query<{ column_name: string; data_type: string }>(
    `SELECT column_name, data_type FROM information_schema.columns
      WHERE table_name = 'nodes' AND table_schema = ANY (current_schemas(false))`,
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
  const result = await client.query(
    `INSERT INTO nodes (${selected.map((column) => `"${column}"`).join(', ')})
     SELECT ${selected.map((column) => `NULLIF("${column}", '')${cast(targetColumns.get(column))}`).join(', ')}
       FROM stage_nodes
      ON CONFLICT (node_id) DO NOTHING`,
  );
  await client.query('DROP TABLE stage_nodes');
  return result.rowCount ?? 0;
}

async function loadNodeLinks(client: pg.Client, file: string): Promise<number> {
  const headers = await csvHeaders(file);
  await stageCsv(client, file, headers, 'stage_links');
  const result = await client.query(
    `INSERT INTO node_links (
       node_a_id, node_b_id, observed_count, last_observed, itm_path_loss_db,
       itm_viable, count_a_to_b, count_b_to_a, multibyte_observed_count, force_viable
     )
     SELECT NULLIF(node_a_id, ''), NULLIF(node_b_id, ''),
            NULLIF(observed_count, '')::int,
            NULLIF(last_observed, '')::timestamptz,
            NULLIF(itm_path_loss_db, '')::double precision,
            NULLIF(itm_viable, '')::boolean,
            NULLIF(count_a_to_b, '')::int,
            NULLIF(count_b_to_a, '')::int,
            NULLIF(multibyte_observed_count, '')::int,
            NULLIF(force_viable, '')::boolean
       FROM stage_links
      ON CONFLICT (node_a_id, node_b_id) DO NOTHING`,
  );
  await client.query('DROP TABLE stage_links');
  return result.rowCount ?? 0;
}

async function loadStatusSamples(client: pg.Client, file: string): Promise<number> {
  const headers = await csvHeaders(file);
  await stageCsv(client, file, headers, 'stage_status');
  const result = await client.query(
    `INSERT INTO node_status_samples (
       "time", node_id, network, battery_mv, uptime_secs, tx_air_secs,
       rx_air_secs, channel_utilization, air_util_tx, status, stats
     )
     SELECT NULLIF("time", '')::timestamptz,
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
       FROM stage_status`,
  );
  await client.query('DROP TABLE stage_status');
  return result.rowCount ?? 0;
}

async function seedPrivacyState(client: pg.Client): Promise<void> {
  await client.query(
    `INSERT INTO public_visibility_state (singleton, generation)
     VALUES (TRUE, 1)
     ON CONFLICT (singleton) DO UPDATE
       SET generation = GREATEST(public_visibility_state.generation, 1),
           updated_at = NOW()`,
  );
  await client.query(
    `INSERT INTO packet_visibility_materialization_state (singleton, visibility_generation)
     VALUES (TRUE, 1)
     ON CONFLICT (singleton) DO UPDATE
       SET visibility_generation = GREATEST(packet_visibility_materialization_state.visibility_generation, 1),
           updated_at = NOW()`,
  );
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const directory = dataDir(args);
  const reset = flag(args, 'reset');
  const fast = flag(args, 'fast');
  const dryRun = flag(args, 'dry-run');
  const files = {
    nodes: path.join(directory, 'nodes.csv.gz'),
    packets: path.join(directory, 'packets.csv.gz'),
    links: path.join(directory, 'node_links.csv.gz'),
    status: path.join(directory, 'node_status_samples.csv.gz'),
  };
  const tables = {
    nodes: await readGzipCsv(files.nodes),
    packets: await readGzipCsv(files.packets),
    links: await readGzipCsv(files.links),
    status: await readGzipCsv(files.status),
  };
  console.log(
    `[sample:load] rows nodes=${tables.nodes.rows.length} packets=${tables.packets.rows.length}`
      + ` links=${tables.links.rows.length} status=${tables.status.rows.length}`,
  );
  if (dryRun) return;

  const client = new pg.Client({ connectionString: databaseUrl(args) });
  await client.connect();
  try {
    await client.query('BEGIN');
    if (fast) {
      try {
        await client.query('SET LOCAL session_replication_role = replica');
        console.log('[sample:load] fast mode: triggers and FK checks disabled for this transaction');
      } catch {
        console.warn('[sample:load] fast mode unavailable (superuser required); continuing normally');
      }
    }
    if (reset) await resetTables(client);
    const loaded = {
      nodes: await loadNodes(client, files.nodes),
      packets: await loadPackets(client, files.packets),
      links: await loadNodeLinks(client, files.links),
      status: await loadStatusSamples(client, files.status),
    };
    await seedPrivacyState(client);
    await client.query('ANALYZE nodes; ANALYZE packets; ANALYZE node_links; ANALYZE node_status_samples');
    await client.query('COMMIT');

    const result = await client.query<{ label: string; rows: string }>(
      `SELECT 'nodes' AS label, COUNT(*)::text AS rows FROM nodes
       UNION ALL SELECT 'packets', COUNT(*)::text FROM packets
       UNION ALL SELECT 'node_links', COUNT(*)::text FROM node_links
       UNION ALL SELECT 'node_status_samples', COUNT(*)::text FROM node_status_samples`,
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

main().catch((error: unknown) => {
  console.error('[sample:load] failed:', error instanceof Error ? error.message : error);
  process.exit(1);
});
