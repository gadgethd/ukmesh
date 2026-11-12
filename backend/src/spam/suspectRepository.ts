import { pool } from '../db/runtime.js';
import { UKMESH_NETWORKS } from '../networks.js';

export interface SpamSuspectRow {
  srcNodeId: string;
  spoofedName: string;
  publicKey?: string;
  claimedLat?: number;
  claimedLon?: number;
  canonicalKey?: string;
  verdict: string;
  signals: Array<{ name: string; score: number; detail: string }>;
  totalScore: number;
  network: string;
}

export async function insertOrUpdateSpamSuspect(s: SpamSuspectRow): Promise<void> {
  await pool.query(
    `INSERT INTO analytics.spam_suspects
       (time, first_seen, src_node_id, spoofed_name, public_key, claimed_lat, claimed_lon,
        canonical_key, verdict, signals, total_score, network)
     VALUES (NOW(), NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     ON CONFLICT (network, src_node_id) DO UPDATE SET
       time        = NOW(),
       first_seen  = COALESCE(analytics.spam_suspects.first_seen, EXCLUDED.first_seen),
       verdict     = EXCLUDED.verdict,
       signals     = EXCLUDED.signals,
       total_score = EXCLUDED.total_score,
       claimed_lat = COALESCE(EXCLUDED.claimed_lat, analytics.spam_suspects.claimed_lat),
       claimed_lon = COALESCE(EXCLUDED.claimed_lon, analytics.spam_suspects.claimed_lon)`,
    [s.srcNodeId, s.spoofedName, s.publicKey ?? null, s.claimedLat ?? null, s.claimedLon ?? null,
     s.canonicalKey ?? null, s.verdict, JSON.stringify(s.signals), s.totalScore, s.network]
  );
}

export async function replaceSpamSuspects(
  sourceNetworks: readonly string[],
  suspects: SpamSuspectRow[],
): Promise<void> {
  if (sourceNetworks.length === 0) throw new Error('SPAM_SOURCE_SCOPE_REQUIRED');
  assertSpamSuspectScope(sourceNetworks, suspects);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM analytics.spam_suspects WHERE network = ANY($1)', [sourceNetworks]);
    for (const s of suspects) {
      await client.query(
        `INSERT INTO analytics.spam_suspects
           (time, first_seen, src_node_id, spoofed_name, public_key, claimed_lat, claimed_lon,
            canonical_key, verdict, signals, total_score, network)
         VALUES (NOW(), NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [s.srcNodeId, s.spoofedName, s.publicKey ?? null, s.claimedLat ?? null, s.claimedLon ?? null,
         s.canonicalKey ?? null, s.verdict, JSON.stringify(s.signals), s.totalScore, s.network]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export function assertSpamSuspectScope(
  sourceNetworks: readonly string[],
  suspects: ReadonlyArray<Pick<SpamSuspectRow, 'network'>>,
): void {
  const allowed = new Set(sourceNetworks);
  if (suspects.some((suspect) => !allowed.has(suspect.network))) {
    throw new Error('SPAM_SUSPECT_OUT_OF_SCOPE');
  }
}

export interface SpamSuspectQueryOptions {
  hours?: number;
  verdict?: 'spam' | 'suspect';
  minScore?: number;
  limit?: number;
  offset?: number;
  includePacketCounts?: boolean;
}

function buildSpamSuspectWhere(options: SpamSuspectQueryOptions, params: unknown[]): string {
  const clauses = [
    `ss.time > NOW() - ($1 * INTERVAL '1 hour')`,
    `ss.network = ANY($2)`,
    `ss.spoofed_name NOT LIKE '%🚫%'`,
    `NOT EXISTS (
       SELECT 1
       FROM core.nodes private_node
       WHERE private_node.node_id = ss.src_node_id
         AND private_node.name LIKE '%🚫%'
     )`,
  ];

  if (options.verdict) {
    params.push(options.verdict);
    clauses.push(`ss.verdict = $${params.length}`);
  }

  if (options.minScore != null) {
    params.push(options.minScore);
    clauses.push(`ss.total_score >= $${params.length}`);
  }

  return clauses.join(' AND ');
}

export async function getSpamSuspects(options: number | SpamSuspectQueryOptions = 48) {
  const queryOptions: SpamSuspectQueryOptions = typeof options === 'number' ? { hours: options } : options;
  const params: unknown[] = [queryOptions.hours ?? 48, UKMESH_NETWORKS];
  const where = buildSpamSuspectWhere(queryOptions, params);
  const limit = Math.max(1, Math.min(200, Math.floor(queryOptions.limit ?? 100)));
  const offset = Math.max(0, Math.floor(queryOptions.offset ?? 0));
  params.push(limit, offset);
  const limitParam = params.length - 1;
  const offsetParam = params.length;

  if (!queryOptions.includePacketCounts) {
    const res = await pool.query(
      `SELECT
         ss.src_node_id,
         ss.spoofed_name,
         ss.public_key,
         ss.claimed_lat,
         ss.claimed_lon,
         ss.canonical_key,
         ss.verdict,
         ss.signals,
         ss.total_score,
         ss.network,
         ss.time AS detected_at,
         ss.first_seen,
         NULL::int AS packet_count,
         NULL::timestamptz AS first_packet,
         NULL::timestamptz AS last_packet
       FROM analytics.spam_suspects ss
       WHERE ${where}
       ORDER BY ss.total_score DESC, ss.time DESC
       LIMIT $${limitParam} OFFSET $${offsetParam}`,
      params
    );
    return res.rows;
  }

  const res = await pool.query(
    `WITH filtered AS (
       SELECT
         ss.src_node_id,
         ss.spoofed_name,
         ss.public_key,
         ss.claimed_lat,
         ss.claimed_lon,
         ss.canonical_key,
         ss.verdict,
         ss.signals,
         ss.total_score,
         ss.network,
         ss.time,
         ss.first_seen
       FROM analytics.spam_suspects ss
       WHERE ${where}
       ORDER BY ss.total_score DESC, ss.time DESC
       LIMIT $${limitParam} OFFSET $${offsetParam}
     )
     SELECT
       f.src_node_id,
       f.spoofed_name,
       f.public_key,
       f.claimed_lat,
       f.claimed_lon,
       f.canonical_key,
       f.verdict,
       f.signals,
       f.total_score,
       f.network,
       f.time AS detected_at,
       f.first_seen,
       COUNT(p.packet_hash) AS packet_count,
       MIN(p.time) AS first_packet,
       MAX(p.time) AS last_packet
     FROM filtered f
     LEFT JOIN mesh.packets p ON p.src_node_id = f.src_node_id
       AND p.network = f.network
       AND p.time > NOW() - INTERVAL '1 hour' * $1
     GROUP BY f.src_node_id, f.spoofed_name, f.public_key, f.claimed_lat, f.claimed_lon,
              f.canonical_key, f.verdict, f.signals, f.total_score, f.network, f.time, f.first_seen
     ORDER BY f.total_score DESC, f.time DESC`,
    params
  );
  return res.rows;
}

export async function getSpamSuspectSummary(options: SpamSuspectQueryOptions = {}) {
  const params: unknown[] = [options.hours ?? 48, UKMESH_NETWORKS];
  const where = buildSpamSuspectWhere(options, params);
  const res = await pool.query(
    `SELECT verdict, COUNT(*)::int AS count
     FROM analytics.spam_suspects ss
     WHERE ${where}
     GROUP BY verdict`,
    params
  );

  const summary = { total: 0, spam: 0, suspect: 0 };
  for (const row of res.rows) {
    const count = Number(row.count ?? 0);
    summary.total += count;
    if (row.verdict === 'spam') summary.spam = count;
    if (row.verdict === 'suspect') summary.suspect = count;
  }
  return summary;
}

export async function getSpamPacketObservers(srcNodeId: string) {
  const res = await pool.query(
    `SELECT DISTINCT ON (p.rx_node_id)
       p.rx_node_id AS node_id,
       n.name,
       n.iata,
       n.lat,
       n.lon,
       p.hop_count,
       p.rssi,
       p.time,
       ss.claimed_lat,
       ss.claimed_lon,
       ss.spoofed_name
     FROM mesh.packets p
     LEFT JOIN core.nodes n ON n.node_id = p.rx_node_id
     LEFT JOIN analytics.spam_suspects ss ON ss.src_node_id = p.src_node_id
       AND ss.network = p.network
     WHERE p.src_node_id = $1
       AND p.network = ANY($2)
       AND COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
       AND p.packet_type = 4
       AND p.time > NOW() - INTERVAL '30 days'
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       AND (ss.spoofed_name IS NULL OR ss.spoofed_name NOT LIKE '%🚫%')
       AND NOT EXISTS (
         SELECT 1 FROM core.nodes source_node
         WHERE source_node.node_id = p.src_node_id
           AND source_node.name LIKE '%🚫%'
       )
     ORDER BY p.rx_node_id, p.hop_count ASC NULLS LAST, p.time ASC`,
    [srcNodeId, UKMESH_NETWORKS]
  );
  return res.rows;
}

export async function getSpamAllObservers() {
  const res = await pool.query(
    `SELECT
       ss.src_node_id,
       ss.claimed_lat,
       ss.claimed_lon,
       ss.spoofed_name,
       p.rx_node_id        AS observer_id,
       n.name              AS observer_name,
       n.lat               AS observer_lat,
       n.lon               AS observer_lon,
       MIN(p.hop_count)    AS hop_count,
       MAX(p.rssi)         AS rssi
     FROM analytics.spam_suspects ss
     JOIN mesh.packets p
       ON  p.src_node_id = ss.src_node_id
       AND p.network = ss.network
       AND p.packet_type  = 4
       AND p.time > NOW() - INTERVAL '30 days'
       AND p.rx_node_id   IS NOT NULL
     JOIN core.nodes n ON n.node_id = p.rx_node_id
     WHERE ss.claimed_lat IS NOT NULL
       AND ss.claimed_lon IS NOT NULL
       AND ss.network = ANY($1)
       AND COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
       AND n.lat IS NOT NULL
       AND n.lon IS NOT NULL
       AND ss.spoofed_name NOT LIKE '%🚫%'
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       AND NOT EXISTS (
         SELECT 1 FROM core.nodes source_node
         WHERE source_node.node_id = ss.src_node_id
           AND source_node.name LIKE '%🚫%'
       )
     GROUP BY
       ss.src_node_id, ss.claimed_lat, ss.claimed_lon, ss.spoofed_name,
       p.rx_node_id, n.name, n.lat, n.lon
     ORDER BY ss.src_node_id, MIN(p.hop_count) ASC NULLS LAST`
  , [UKMESH_NETWORKS]);
  return res.rows;
}
