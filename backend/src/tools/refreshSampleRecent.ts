/** A fixed synthetic overlay keeps five-minute reads meaningful in long lab
 * runs. No archive row is modified; identities/count stay stable across runs. */
import pg from 'pg';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { assertLocalDatabaseUrl } from './sampleData.js';

export async function refreshSampleRecent(databaseUrl: string): Promise<void> {
  assertLocalDatabaseUrl(databaseUrl);
  const client = new pg.Client({ connectionString: databaseUrl, statement_timeout: 30_000 });
  await client.connect();
  const nodeId = '0102' + '11'.repeat(30);
  const topic = `meshcore/LHR/${nodeId}/packets`;
  const rows = Array.from({ length: 50 }, (_, index) => {
    const payload = Buffer.alloc(4);
    payload.writeUInt32LE(0xf17e0000 + index);
    const rawHex = '0D00' + payload.toString('hex').toUpperCase();
    const hash = createHash('sha256').update(Buffer.from([0x0c])).update(payload).digest('hex').toUpperCase();
    return { observation_id: `f17e0000-0000-4000-8000-${(index + 1).toString(16).padStart(12, '0')}`,
      packet_hash: hash, raw_hex: rawHex, time: new Date(Date.now() - index * 1000).toISOString() };
  });
  try {
    await client.query('BEGIN');
    await client.query(`INSERT INTO core.nodes (node_id, name, role, lat, lon, network, iata, last_seen, is_online)
      VALUES ($1, 'Synthetic benchmark observer', 2, 51.5, -0.1, 'ukmesh', 'LHR', NOW(), true)
      ON CONFLICT (node_id) DO UPDATE SET last_seen = NOW(), is_online = true`, [nodeId]);
    const values = JSON.stringify(rows);
    // Update the same 50 receptions, rather than accumulating fresh identities.
    await client.query(`UPDATE mesh.packets p SET time = v.time
      FROM jsonb_to_recordset($1::jsonb) AS v(observation_id uuid, time timestamptz)
      WHERE p.observation_id = v.observation_id AND p.topic = $2`, [values, topic]);
    const inserted = await client.query(`INSERT INTO mesh.packets
      (observation_id, time, packet_hash, packet_uid, raw_hex, topic, topic_prefix, iata, network,
       rx_node_id, packet_type, route_type, hop_count, path_hash_size_bytes, path_hashes, payload)
      SELECT v.observation_id, v.time, v.packet_hash, decode(v.packet_hash, 'hex'), v.raw_hex,
        $2, 'meshcore', 'LHR', 'ukmesh', $3, 3, 1, 0, 1, ARRAY[]::text[],
        jsonb_build_object('_summary', 'Synthetic benchmark ACK', '_local_fixture', 'recent-v1')
      FROM jsonb_to_recordset($1::jsonb) AS v(observation_id uuid, time timestamptz, packet_hash text, raw_hex text)
      WHERE NOT EXISTS (SELECT 1 FROM mesh.packets p WHERE p.observation_id = v.observation_id)`, [values, topic, nodeId]);
    if ((inserted.rowCount ?? 0) > 0) await client.query('DELETE FROM analytics.stats_chart_snapshots');
    await client.query(`INSERT INTO mesh.ingest_observation_ledger (observation_id, observed_at, packet_hash, network)
      SELECT observation_id, time, packet_hash, network FROM mesh.packets
      WHERE observation_id = ANY($1::uuid[]) ON CONFLICT (observation_id) DO NOTHING`, [rows.map((row) => row.observation_id)]);
    await client.query(`UPDATE mesh.ingest_observation_ledger l SET observed_at = v.time
      FROM jsonb_to_recordset($1::jsonb) AS v(observation_id uuid, time timestamptz)
      WHERE l.observation_id = v.observation_id`, [values]);
    const verified = await client.query<{ n: number }>(`SELECT COUNT(*)::int AS n FROM mesh.packets
      WHERE observation_id = ANY($1::uuid[]) AND payload->>'_local_fixture' = 'recent-v1'
      AND visibility_ok IS TRUE AND is_private IS NOT TRUE`, [rows.map((row) => row.observation_id)]);
    if (verified.rows[0]?.n !== rows.length) throw new Error('synthetic recent fixture did not verify');
    await client.query('COMMIT');
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
  finally { await client.end(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  refreshSampleRecent(process.env['DATABASE_URL'] ?? '').catch((error: unknown) => {
    console.error('[sample:refresh-recent]', error instanceof Error ? error.message : 'failed');
    process.exitCode = 1;
  });
}
