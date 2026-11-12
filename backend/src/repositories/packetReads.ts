import { pool } from '../db/runtime.js';
import { query } from '../db/runtime.js';
import { analyticsQuery } from '../db/runtime.js';
import type { DatabaseQueryFn as QueryFn } from '../db/contracts.js';
import { channelHashesForName } from '../mqtt/channelRegistry.js';
import { isPublicChannelName } from '../mqtt/channelRegistry.js';
import { sanitizePublicPacketPayload } from '../mqtt/channelRegistry.js';
import { getRamIngestMode } from '../ram/protocol.js';
import { packetIdentityKeySql } from '../db/packetIdentityRead.js';
import { packetIdentityReadAuthority } from '../db/packetIdentityRead.js';
import { buildScopePlaceholders } from '../db/readScope.js';
import { buildPacketScopeClause } from '../db/readScope.js';
import { buildPublicPacketPrivacyClause } from '../db/readScope.js';

/**
 * Resolve durable decryption summaries for a bounded set of packet identities.
 *
 * `mesh.packet_decryptions` is keyed by the observation `packet_hash` that was
 * decrypted, which is not necessarily the observation the feed keeps as the
 * representative of a UID group (decryption backfill runs per stored hash, and
 * one wire content can be retained under several observation hashes). Joining
 * the decryption on the representative's exact hash therefore reports a NULL
 * summary for an identity that *is* decrypted.
 *
 * The subquery re-expands each identity to its observation hashes through
 * `mesh.packets.packet_uid` (indexed by `packets_packet_uid_idx`) and keys the
 * result with `packetIdentityKeySql`, the same expression the representative,
 * statistics and tag CTEs group by, so the summary is attributed to the identity
 * rather than to one arbitrary observation. Rows without a UID keep the hash
 * fallback identity, and `hash` read authority keeps its one-observation-per-
 * identity rollback shape untouched.
 *
 * Only identities already admitted by the caller's privacy-filtered CTE are
 * resolved, so this cannot surface a row that the representative scan excluded.
 */
function identityDecryptionCteSql(cteName: string, windowSql: string, scopeClause: string): string {
  // Resolve decryptions set-based over the same bounded window the reader
  // already uses. An earlier attempt expanded each identity through
  // mesh.packets.packet_uid, but with no predicate on the packets side the
  // planner hash-joined the entire hypertable: 3.67M rows and ~3.7 GiB of
  // buffers per call, 4.3 s p50 against 10 ms for the old exact-hash join.
  // Reading only the window keeps the packets_time_idx path (~25 ms p50) and
  // still attributes a sibling observation's decryption to the identity.
  const authority = packetIdentityReadAuthority();
  const key = packetIdentityKeySql('p', authority);
  return `${cteName} AS (
      SELECT DISTINCT ON (identity_key) identity_key, summary
        FROM (
          SELECT ${key} AS identity_key, pd.summary, pd.created_at, pd.packet_hash
            FROM mesh.packets p
            JOIN mesh.packet_decryptions pd ON pd.packet_hash = p.packet_hash
           WHERE p.time > ${windowSql}
             ${scopeClause}
        ) decryption_candidates
       ORDER BY identity_key, created_at DESC, packet_hash
    )`;
}

export async function getRecentPackets(
  limit = 200,
  network?: string,
  observer?: string,
  fields: 'full' | 'slim' = 'full',
  signal?: AbortSignal,
  readQuery: QueryFn = query,
  overlapHashes: string[] = [],
  region?: string,
) {
  const includeObservationIds = getRamIngestMode() === 'ram';
  const scope = buildScopePlaceholders(2, network, observer, region);
  const overlapParam = includeObservationIds && overlapHashes.length > 0 ? `$${2 + scope.params.length}` : null;
  const fiveMinAgo = 'NOW() - INTERVAL \'5 minutes\'';
  const res = await readQuery(
    `WITH recent_packets AS (
      SELECT DISTINCT ON (${packetIdentityKeySql('p')})
             ${includeObservationIds ? `(CASE WHEN p.payload ? 'appData' THEN 16 ELSE 0 END
               + CASE WHEN p.src_node_id IS NOT NULL THEN 8 ELSE 0 END
               + CASE WHEN p.advert_count IS NOT NULL THEN 4 ELSE 0 END
               + CASE WHEN p.packet_type = 4 THEN 2 ELSE 0 END) AS _representative_rank,` : ''}
             ${includeObservationIds ? 'p.observation_id::text AS _representative_id,' : ''}
             p.time, p.packet_hash, p.packet_uid,
             ${packetIdentityKeySql('p')} AS identity_key,
             COALESCE(pa_rx.canonical_node_id, upper(btrim(p.rx_node_id))) AS rx_node_id,
             COALESCE(pa_src.canonical_node_id, upper(btrim(p.src_node_id))) AS src_node_id, p.topic,
             p.topic_prefix, p.iata, p.packet_type, p.route_type, p.hop_count, p.rssi, p.snr,
             p.payload->>'_summary' AS summary,
             p.advert_count, p.path_hashes, p.path_hash_size_bytes,
             p.network, p.transport_codes, p.region_scope
      FROM mesh.packets p
      LEFT JOIN core.node_identity_aliases pa_rx ON pa_rx.source_node_id = upper(btrim(p.rx_node_id))
      LEFT JOIN core.node_identity_aliases pa_src ON pa_src.source_node_id = upper(btrim(p.src_node_id))
      WHERE p.time > ${fiveMinAgo}
        ${buildPacketScopeClause(scope, 'p', network)}
        ${buildPublicPacketPrivacyClause('p')}
      ORDER BY ${packetIdentityKeySql('p')},
               CASE WHEN p.payload ? 'appData' THEN 1 ELSE 0 END DESC,
               CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
               CASE WHEN p.advert_count IS NOT NULL THEN 1 ELSE 0 END DESC,
               CASE WHEN p.packet_type = 4 THEN 1 ELSE 0 END DESC,
               p.time DESC, p.observation_id DESC NULLS LAST
    ),
    packet_stats AS (
      SELECT
        ${packetIdentityKeySql('packets')} AS identity_key,
        ${includeObservationIds ? `ARRAY_AGG(DISTINCT packets.observation_id::text)
          FILTER (WHERE packets.observation_id IS NOT NULL) AS _observation_ids,` : ''}
        ARRAY_AGG(DISTINCT COALESCE(pa2.canonical_node_id, upper(btrim(packets.rx_node_id)))
                  ORDER BY COALESCE(pa2.canonical_node_id, upper(btrim(packets.rx_node_id))))
          FILTER (WHERE packets.rx_node_id IS NOT NULL) AS observer_node_ids,
        ARRAY_AGG(DISTINCT iata ORDER BY iata) FILTER (WHERE NULLIF(TRIM(iata), '') IS NOT NULL) AS observer_iatas,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') <> 'tx')::int AS rx_count,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') = 'tx')::int AS tx_count
      FROM mesh.packets
      LEFT JOIN core.node_identity_aliases pa2 ON pa2.source_node_id = upper(btrim(packets.rx_node_id))
      WHERE ${packetIdentityKeySql('packets')} = ANY(SELECT identity_key FROM recent_packets)
        AND time > ${fiveMinAgo}
        ${buildPacketScopeClause(scope, '', network)}
        ${buildPublicPacketPrivacyClause('packets')}
      GROUP BY ${packetIdentityKeySql('packets')}
    ),
    packet_tags AS (
      SELECT DISTINCT ON (${packetIdentityKeySql('p2')})
             ${packetIdentityKeySql('p2')} AS identity_key,
             mt2.tags, mt2.confidence
        FROM mesh.packets p2
        JOIN mesh.message_tags mt2 ON mt2.packet_hash = p2.packet_hash
       WHERE p2.time > ${fiveMinAgo}
         AND ${packetIdentityKeySql('p2')} = ANY(SELECT identity_key FROM recent_packets)
         AND mt2.tags IS NOT NULL
       ORDER BY ${packetIdentityKeySql('p2')}, mt2.tagged_at DESC
    ),
    ${identityDecryptionCteSql('identity_decryptions', fiveMinAgo, buildPacketScopeClause(scope, 'p', network))}
    SELECT
      rp.time, rp.packet_hash, rp.rx_node_id, rp.src_node_id,
      rp.packet_type, rp.hop_count, rp.rssi, rp.snr,
      COALESCE(rp.summary, ids.summary) AS summary, rp.advert_count, rp.path_hashes, rp.path_hash_size_bytes,
      ${fields === 'full'
        ? 'rp.topic, rp.topic_prefix, rp.iata, rp.route_type, rp.network, rp.transport_codes, rp.region_scope,'
        : ''}
      ${includeObservationIds ? `ps._observation_ids, rp.network AS _policy_network,
        (SELECT generation::text FROM ops.public_visibility_state WHERE singleton) AS _policy_generation,
        rp._representative_rank, rp._representative_id,` : ''}
      ps.observer_node_ids, ps.observer_iatas, ps.rx_count, ps.tx_count,
      mt.tags, mt.confidence AS tag_confidence
    FROM recent_packets rp
    LEFT JOIN packet_stats ps ON ps.identity_key = rp.identity_key
    LEFT JOIN packet_tags mt ON mt.identity_key = rp.identity_key
    LEFT JOIN identity_decryptions ids ON ids.identity_key = rp.identity_key
    ${overlapParam ? `WHERE rp.packet_hash = ANY(${overlapParam}::text[])
      OR rp.packet_hash IN (SELECT packet_hash FROM recent_packets ORDER BY time DESC, packet_hash LIMIT $1)` : ''}
    ORDER BY rp.time DESC, rp.packet_hash
    LIMIT $1${overlapParam ? ` + CARDINALITY(${overlapParam}::text[])` : ''}`,
    [limit, ...scope.params, ...(overlapParam ? [overlapHashes] : [])],
    signal,
  );
  return res.rows;
}

/**
 * Fetch the last N GroupText (type=5) messages from the last 24 hours.
 * Used to pre-populate the channel feed on page load so it isn't blank.
 * Returns rows with the same shape as getRecentPackets.
 */
export async function getRecentMessages(
  limit = 50,
  network?: string,
  observer?: string,
  signal?: AbortSignal,
  readQuery: QueryFn = query,
  overlapHashes: string[] = [],
  region?: string,
) {
  const includeObservationIds = getRamIngestMode() === 'ram';
  const scope = buildScopePlaceholders(2, network, observer, region);
  const overlapParam = includeObservationIds && overlapHashes.length > 0 ? `$${2 + scope.params.length}` : null;
  const res = await readQuery(
    `WITH recent_msgs AS (
      SELECT DISTINCT ON (${packetIdentityKeySql('p')})
             ${includeObservationIds ? 'CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END AS _representative_rank,' : ''}
             ${includeObservationIds ? 'p.observation_id::text AS _representative_id,' : ''}
             p.time, p.packet_hash, p.packet_uid,
             ${packetIdentityKeySql('p')} AS identity_key,
             COALESCE(pa_rx.canonical_node_id, upper(btrim(p.rx_node_id))) AS rx_node_id,
             COALESCE(pa_src.canonical_node_id, upper(btrim(p.src_node_id))) AS src_node_id, p.topic,
             p.iata,
             p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
             p.payload->>'_summary' AS summary,
             p.advert_count, p.path_hashes, p.path_hash_size_bytes,
             p.network
      FROM mesh.packets p
      LEFT JOIN core.node_identity_aliases pa_rx ON pa_rx.source_node_id = upper(btrim(p.rx_node_id))
      LEFT JOIN core.node_identity_aliases pa_src ON pa_src.source_node_id = upper(btrim(p.src_node_id))
      WHERE p.packet_type = 5
        AND p.time > NOW() - INTERVAL '24 hours'
        ${buildPacketScopeClause(scope, 'p', network)}
        ${buildPublicPacketPrivacyClause('p')}
      ORDER BY ${packetIdentityKeySql('p')},
               CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
               p.time DESC, p.observation_id DESC NULLS LAST
    ),
    selected_msgs AS MATERIALIZED (
      SELECT * FROM recent_msgs
      ${overlapParam ? `WHERE packet_hash = ANY(${overlapParam}::text[])
        OR packet_hash IN (SELECT packet_hash FROM recent_msgs ORDER BY time DESC, packet_hash LIMIT $1)` : ''}
      ORDER BY time DESC, packet_hash
      LIMIT $1${overlapParam ? ` + CARDINALITY(${overlapParam}::text[])` : ''}
    ),
    msg_stats AS (
      SELECT
        ${packetIdentityKeySql('packets')} AS identity_key,
        ${includeObservationIds ? `ARRAY_AGG(DISTINCT packets.observation_id::text)
          FILTER (WHERE packets.observation_id IS NOT NULL) AS _observation_ids,` : ''}
        ARRAY_AGG(DISTINCT COALESCE(pa2.canonical_node_id, upper(btrim(packets.rx_node_id)))
                  ORDER BY COALESCE(pa2.canonical_node_id, upper(btrim(packets.rx_node_id))))
          FILTER (WHERE packets.rx_node_id IS NOT NULL) AS observer_node_ids,
        ARRAY_AGG(DISTINCT iata ORDER BY iata) FILTER (WHERE NULLIF(TRIM(iata), '') IS NOT NULL) AS observer_iatas,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') <> 'tx')::int AS rx_count,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') = 'tx')::int AS tx_count
      FROM mesh.packets
      LEFT JOIN core.node_identity_aliases pa2 ON pa2.source_node_id = upper(btrim(packets.rx_node_id))
      WHERE ${packetIdentityKeySql('packets')} = ANY(SELECT identity_key FROM selected_msgs)
        AND time > NOW() - INTERVAL '24 hours'
        ${buildPacketScopeClause(scope, '', network)}
        ${buildPublicPacketPrivacyClause('packets')}
      GROUP BY ${packetIdentityKeySql('packets')}
    ),
    msg_tags AS (
      SELECT DISTINCT ON (${packetIdentityKeySql('p2')})
             ${packetIdentityKeySql('p2')} AS identity_key,
             mt2.tags, mt2.confidence
        FROM mesh.packets p2
        JOIN mesh.message_tags mt2 ON mt2.packet_hash = p2.packet_hash
       WHERE p2.time > NOW() - INTERVAL '24 hours'
         AND ${packetIdentityKeySql('p2')} = ANY(SELECT identity_key FROM selected_msgs)
         AND mt2.tags IS NOT NULL
       ORDER BY ${packetIdentityKeySql('p2')}, mt2.tagged_at DESC
    ),
    ${identityDecryptionCteSql('message_decryptions', "NOW() - INTERVAL '24 hours'", buildPacketScopeClause(scope, 'p', network))}
    SELECT
      m.time, m.packet_hash, m.rx_node_id, m.src_node_id, m.topic, m.iata,
      m.packet_type, m.hop_count, m.rssi, m.snr, m.payload,
      COALESCE(m.summary, md.summary) AS summary, m.advert_count, m.path_hashes, m.path_hash_size_bytes,
      ${includeObservationIds ? `ms._observation_ids, m.network AS _policy_network,
        (SELECT generation::text FROM ops.public_visibility_state WHERE singleton) AS _policy_generation,
        m._representative_rank, m._representative_id,` : ''}
      ms.observer_node_ids, ms.observer_iatas, ms.rx_count, ms.tx_count,
      mt.tags, mt.confidence AS tag_confidence
    FROM selected_msgs m
    LEFT JOIN msg_stats ms ON ms.identity_key = m.identity_key
    LEFT JOIN msg_tags mt ON mt.identity_key = m.identity_key
    LEFT JOIN message_decryptions md ON md.identity_key = m.identity_key
    ${overlapParam ? `WHERE m.packet_hash = ANY(${overlapParam}::text[])
      OR m.packet_hash IN (SELECT packet_hash FROM recent_msgs ORDER BY time DESC, packet_hash LIMIT $1)` : ''}
    ORDER BY m.time DESC, m.packet_hash
    LIMIT $1${overlapParam ? ` + CARDINALITY(${overlapParam}::text[])` : ''}`,
    [limit, ...scope.params, ...(overlapParam ? [overlapHashes] : [])],
    signal,
  );
  return res.rows;
}

/**
 * Recent message tags for live feed enrichment. Tags are produced by the
 * tagger-worker a few seconds after a packet arrives, so the live WS feed can
 * never carry them inline; the frontend polls this and patches rows by hash.
 * Only tags whose packet is publicly visible and in scope are returned.
 */
export async function getRecentMessageTags(limit = 200, network?: string) {
  const scope = buildScopePlaceholders(2, network);
  const res = await query(
    `SELECT mt.packet_hash, mt.tags, mt.confidence, mt.tagged_at
       FROM mesh.message_tags mt
      WHERE mt.tagged_at > NOW() - INTERVAL '24 hours'
        AND EXISTS (
          SELECT 1 FROM mesh.packets p
           WHERE p.packet_hash = mt.packet_hash
             AND p.time > NOW() - INTERVAL '24 hours'
             ${buildPacketScopeClause(scope, 'p', network)}
             ${buildPublicPacketPrivacyClause('p')}
        )
      ORDER BY mt.tagged_at DESC
      LIMIT $1`,
    [limit, ...scope.params],
  );
  return res.rows;
}

/**
 * Fetch one channel's decrypted GroupText history. The WebSocket snapshot stays
 * intentionally recent and bounded; this demand-driven read reaches back 90
 * days so quiet channels can still fill the feed's 50-row view.
 */
export async function getChannelMessageHistory(
  channel: string,
  limit = 50,
  network?: string,
  observer?: string,
) {
  const normalizedChannel = channel.trim().toLowerCase();
  if (!isPublicChannelName(normalizedChannel)) return [];
  const channelHashes = channelHashesForName(normalizedChannel);
  const scope = buildScopePlaceholders(6, network, observer);
  // A historical packet may retain the ingest-time "[encrypted]" marker in
  // payload while mesh.packet_decryptions has the channel-specific summary. Prefer
  // the durable decryption summary for channel matching and display.
  const summaryExpr = `COALESCE(NULLIF(BTRIM(pd.summary), ''), NULLIF(BTRIM(p.payload->>'_summary'), ''))`;
  const historyWindow = `NOW() - INTERVAL '90 days'`;
  const historyBatchLimit = 100;
  const selectionLimit = Math.min(100, Math.max(limit, Math.ceil(limit * 1.5)));
  const selected = new Set<string>();
  let cursorTime = new Date();
  let cursorHash = '\uffff';

  while (selected.size < selectionLimit) {
    const batch = await analyticsQuery<{ packet_hash: string; time: Date }>(
      `SELECT p.time, p.packet_hash
       FROM mesh.packets p
       LEFT JOIN mesh.packet_decryptions pd ON pd.packet_hash = p.packet_hash
       WHERE p.packet_type = 5
         AND p.time > ${historyWindow}
         AND (
           p.time < $2::timestamptz
           OR (p.time = $2::timestamptz AND p.packet_hash < $3)
         )
         ${buildPacketScopeClause(scope, 'p', network)}
         -- Apply the complete privacy predicate after this bounded candidate
         -- page is selected. The path/private-prefix checks are intentionally
         -- not evaluated for every historical packet observation.
         AND p.visibility_ok IS TRUE
         AND p.is_private IS NOT TRUE
         AND (
           LOWER(${summaryExpr}) LIKE ('[' || $4 || ']%')
           OR LOWER(COALESCE(
             p.payload->>'channelHash',
             p.payload->'decrypted'->>'channelHash',
             pd.decrypted->>'channelHash',
             ''
           )) = ANY($5::text[])
         )
       ORDER BY p.time DESC, p.packet_hash DESC
       LIMIT $1`,
      [historyBatchLimit, cursorTime, cursorHash, normalizedChannel, channelHashes, ...scope.params],
    );
    if (batch.rows.length === 0) break;

    for (const row of batch.rows) {
      if (typeof row.packet_hash !== 'string' || selected.has(row.packet_hash)) continue;
      selected.add(row.packet_hash);
      if (selected.size >= selectionLimit) break;
    }

    const last = batch.rows.at(-1);
    if (!last || batch.rows.length < historyBatchLimit) break;
    cursorTime = last.time;
    cursorHash = last.packet_hash;
  }

  const selectedHashes = Array.from(selected.keys());
  if (selectedHashes.length === 0) return [];

  const detailScope = buildScopePlaceholders(2, network, observer);
  const res = await analyticsQuery(
    `WITH channel_candidates AS MATERIALIZED (
      SELECT DISTINCT ON (p.packet_hash)
             p.time, p.packet_hash,
             COALESCE(rx_alias.canonical_node_id, UPPER(BTRIM(p.rx_node_id))) AS rx_node_id,
             COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id))) AS src_node_id, p.topic,
             p.iata,
             p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
             ${summaryExpr} AS summary,
             p.advert_count, p.path_hashes, p.path_hash_size_bytes,
             p.network, p.visibility_ok, p.is_private
      FROM mesh.packets p
      LEFT JOIN mesh.packet_decryptions pd ON pd.packet_hash = p.packet_hash
      LEFT JOIN core.node_identity_aliases rx_alias
        ON rx_alias.source_node_id = UPPER(BTRIM(p.rx_node_id))
      LEFT JOIN core.node_identity_aliases src_alias
        ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
      WHERE p.packet_hash = ANY($1::text[])
        AND p.packet_type = 5
        AND p.time > ${historyWindow}
        ${buildPacketScopeClause(detailScope, 'p', network)}
        AND p.visibility_ok IS TRUE
        AND p.is_private IS NOT TRUE
      ORDER BY p.packet_hash,
               CASE WHEN ${summaryExpr} IS NOT NULL THEN 1 ELSE 0 END DESC,
               CASE WHEN p.payload ? 'decrypted' THEN 1 ELSE 0 END DESC,
               CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
               p.time DESC
    )
    SELECT
      m.time, m.packet_hash, m.rx_node_id, m.src_node_id, m.topic, m.iata,
      m.packet_type, m.hop_count, m.rssi, m.snr, m.payload,
      m.summary, m.advert_count, m.path_hashes, m.path_hash_size_bytes,
      CASE WHEN m.rx_node_id IS NOT NULL THEN ARRAY[m.rx_node_id]::text[] ELSE ARRAY[]::text[] END AS observer_node_ids,
      CASE WHEN NULLIF(TRIM(m.iata), '') IS NOT NULL THEN ARRAY[m.iata]::text[] ELSE ARRAY[]::text[] END AS observer_iatas,
      CASE WHEN COALESCE(m.payload->>'direction', 'rx') <> 'tx' THEN 1 ELSE 0 END::int AS rx_count,
      CASE WHEN COALESCE(m.payload->>'direction', 'rx') = 'tx' THEN 1 ELSE 0 END::int AS tx_count,
      mt.tags, mt.confidence AS tag_confidence
    FROM channel_candidates m
    LEFT JOIN mesh.message_tags mt ON mt.packet_hash = m.packet_hash
    WHERE 1 = 1
      ${buildPublicPacketPrivacyClause('m')}
    ORDER BY m.time DESC`,
    [selectedHashes, ...detailScope.params],
  );
  const channelPrefix = `[${normalizedChannel}]`;
  return res.rows.slice(0, limit).map((row) => {
    const summary = typeof row.summary === 'string' ? row.summary.trim() : '';
    if (summary.toLowerCase().startsWith(channelPrefix)) return row;
    // A channel-hash match can carry the generic live "[encrypted]" marker;
    // normalize it to the selected channel so the existing feed scope matcher
    // includes the row without changing the live packet path.
    return { ...row, summary: `${channelPrefix}${summary ? ` ${summary}` : ''}` };
  });
}

export async function getRecentPacketEvents(limit = 200, network?: string, observer?: string, region?: string) {
  const scope = buildScopePlaceholders(2, network, observer, region);
  const params: unknown[] = [limit, ...scope.params];
  const res = await pool.query(
    `SELECT
        p.time, p.packet_hash,
        COALESCE(rx_alias.canonical_node_id, UPPER(BTRIM(p.rx_node_id))) AS rx_node_id,
        COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id))) AS src_node_id,
        p.topic, p.iata,
        p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
        p.payload->>'_summary' AS summary,
        p.advert_count, p.path_hashes, p.path_hash_size_bytes,
        p.region_scope
     FROM mesh.packets p
     LEFT JOIN core.node_identity_aliases rx_alias
       ON rx_alias.source_node_id = UPPER(BTRIM(p.rx_node_id))
     LEFT JOIN core.node_identity_aliases src_alias
       ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
     WHERE p.time > NOW() - INTERVAL '24 hours'
         ${buildPacketScopeClause(scope, 'p', network)}
         ${buildPublicPacketPrivacyClause('p')}
     ORDER BY p.time DESC
     LIMIT $1`,
    params,
  );
  return res.rows;
}

export async function getPacketDetail(hash: string, network = 'ukmesh') {
  const scope = buildScopePlaceholders(2, network);
  const [primary, observations] = await Promise.all([
    pool.query(
      `SELECT p.time, p.packet_hash, p.rx_node_id, p.src_node_id, p.topic, p.iata,
              p.packet_type, p.route_type, p.hop_count, p.rssi, p.snr,
              p.payload, p.path_hashes, p.path_hash_size_bytes, p.raw_hex,
              mt.tags, mt.confidence AS tag_confidence
       FROM mesh.packets p
       LEFT JOIN mesh.message_tags mt ON mt.packet_hash = p.packet_hash
       WHERE p.packet_hash = $1
         ${buildPacketScopeClause(scope, 'p', network)}
         ${buildPublicPacketPrivacyClause('p')}
       ORDER BY
         CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
         CASE WHEN p.raw_hex IS NOT NULL THEN 1 ELSE 0 END DESC,
         p.time DESC
       LIMIT 1`,
      [hash, ...scope.params],
    ),
    pool.query(
      `SELECT COALESCE(rx_alias.canonical_node_id, UPPER(BTRIM(p.rx_node_id))) AS rx_node_id,
              p.iata, p.time, p.rssi, p.snr, p.hop_count
       FROM mesh.packets p
       LEFT JOIN core.node_identity_aliases rx_alias
         ON rx_alias.source_node_id = UPPER(BTRIM(p.rx_node_id))
       WHERE p.packet_hash = $1
         ${buildPacketScopeClause(scope, 'p', network)}
         ${buildPublicPacketPrivacyClause('p')}
       ORDER BY p.time ASC`,
      [hash, ...scope.params],
    ),
  ]);
  const row = primary.rows[0];
  if (!row) return null;
  return {
    time: row.time as Date,
    packetHash: row.packet_hash as string,
    rxNodeId: row.rx_node_id as string | null,
    srcNodeId: row.src_node_id as string | null,
    topic: row.topic as string,
    iata: row.iata as string | null,
    packetType: row.packet_type as number | null,
    routeType: row.route_type as number | null,
    hopCount: row.hop_count as number | null,
    rssi: row.rssi as number | null,
    snr: row.snr as number | null,
    payload: sanitizePublicPacketPayload(row.payload as Record<string, unknown> | null),
    pathHashes: row.path_hashes as string[] | null,
    pathHashSizeBytes: row.path_hash_size_bytes as number | null,
    rawHex: row.raw_hex as string | null,
    tags: (row.tags as Record<string, unknown> | null) ?? null,
    tagConfidence: (row.tag_confidence as Record<string, unknown> | null) ?? null,
    observations: observations.rows.map((r) => ({
      rxNodeId: r.rx_node_id as string | null,
      iata: r.iata as string | null,
      time: r.time as Date,
      rssi: r.rssi as number | null,
      snr: r.snr as number | null,
      hopCount: r.hop_count as number | null,
    })),
  };
}
