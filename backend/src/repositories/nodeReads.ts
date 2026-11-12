import { pool } from '../db/runtime.js';
import { query } from '../db/runtime.js';
import { nodeAliasArraySql } from '../platform/query/networkFilters.js';
import { nodeEffectiveLastSeenSql } from '../nodes/presence.js';
import { nodeEffectiveOnlineSql } from '../nodes/presence.js';
import { regionNodeSourceSql } from '../regions/nodeSource.js';
import { mapVisiblePredicate } from '../nodes/publicMap.js';
import { publicMapBasePredicate } from '../nodes/publicMap.js';
import { buildScopePlaceholders } from '../db/readScope.js';
import { buildPacketScopeClause } from '../db/readScope.js';
import { buildPublicPacketPrivacyClause } from '../db/readScope.js';
import { buildNodeScopeClause } from '../db/readScope.js';

export type GetNodesOptions = {
  mapVisible?: boolean;
  /** Initial packets/links may reference nodes whose presence timestamp lags.
   * Keep those public, positioned endpoints in the initial map union. */
  referencedNodeIds?: readonly string[];
};

export async function getNodes(
  network?: string,
  observer?: string,
  fields: 'full' | 'slim' = 'full',
  signal?: AbortSignal,
  region?: string,
  options: GetNodesOptions = {},
) {
  const scope = buildScopePlaceholders(1, network, observer, region);
  const params: unknown[] = [...scope.params];
  let mapConditions = '';
  if (options.mapVisible) {
    const visible = `(${mapVisiblePredicate('n')}
      AND NOT EXISTS (SELECT 1 FROM core.node_map_suppressions s WHERE s.node_id = n.node_id))`;
    if (options.referencedNodeIds?.length) {
      params.push([...new Set(options.referencedNodeIds.map((id) => id.trim().toUpperCase()))]);
      // The explicit safety-net union bypasses age/suppression only. Never
      // bypass public coordinates, roles, privacy, or the requested scope.
      mapConditions = ` AND (${visible} OR (${publicMapBasePredicate('n')}
        AND n.node_id = ANY($${params.length}::text[])))`;
    } else {
      mapConditions = ` AND ${visible}`;
    }
  }
  const slimConditions = fields === 'slim' && !options.mapVisible
    ? ` AND n.lat BETWEEN -90 AND 90
        AND n.lon BETWEEN -180 AND 180
        AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
        AND (n.role IS NULL OR n.role = 2)`
    : '';
  const whereClause = `WHERE 1=1${buildNodeScopeClause(scope, 'n')}${slimConditions}${mapConditions}`;
  // public_key is what the map's per-repeater RF footprint action needs; the
  // slim projection kept it out, so only nodes that arrived via a live
  // node_upsert ever exposed the on-demand calculation button.
  const optionalFields = fields === 'full'
    ? `, n.hardware_model, n.public_key, n.elevation_m`
    : ', n.elevation_m, n.public_key';
  const res = await query<{
    node_id: string;
    name: string | null;
    lat: number | null;
    lon: number | null;
    role: number | null;
  }>(
    `SELECT
       n.node_id,
       n.name,
       n.lat,
       n.lon,
       COALESCE(n.observer_iata, n.iata) AS iata,
       n.role,
       -- Advert/status writes, direct observer reception, and trustworthy
       -- multibyte relay evidence are independent proofs of presence. Always
       -- expose the newest proof; preferring observer metadata with COALESCE
       -- could make a freshly advertised node appear days older on the map.
       ${nodeEffectiveLastSeenSql('n')} AS last_seen,
       ${nodeEffectiveOnlineSql('n')} AS is_online,
       n.advert_count
       ${optionalFields}
     FROM ${scope.regionParam ? regionNodeSourceSql(scope.regionParam) : 'core.node_identity_nodes'} n
     ${whereClause}
     ORDER BY ${nodeEffectiveLastSeenSql('n')} DESC`,
    params,
    signal,
  );
  return res.rows;
}

export async function getNodeHistory(nodeId: string, hours = 24, network = 'ukmesh') {
  const scope = buildScopePlaceholders(3, network);
  const aliases = nodeAliasArraySql('$1');
  // Both consumers describe general activity, including for observers that
  // are also repeaters. Include receptions and known origins for every role.
  // Drive each direction with the canonical alias array and an equality
  // lookup: a combined OR/ANY lets LIMIT choose a scan of time-ordered traffic
  // for quiet nodes. Per-alias limits keep both node/time indexes useful even
  // at 28 days; the final limit retains the newest 500 observations overall.
  const packetsFor = (column: 'rx_node_id' | 'src_node_id') => `
    SELECT p.time, p.packet_hash,
           COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id))) AS src_node_id,
           topic, packet_type, hop_count, rssi, snr, payload
    FROM mesh.packets p
    LEFT JOIN core.node_identity_aliases src_alias
      ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
    WHERE p.${column} = requested.node_id
      ${column === 'src_node_id' ? `AND NOT COALESCE(p.rx_node_id = ANY(${aliases}), FALSE)` : ''}
      AND p.time > NOW() - INTERVAL '1 hour' * $2
      AND p.time <= NOW()
      ${buildPacketScopeClause(scope, 'p', network)}
      ${buildPublicPacketPrivacyClause('p')}
    ORDER BY time DESC LIMIT 500`;
  const res = await pool.query(
    `SELECT recent.*
     FROM unnest(${aliases}) requested(node_id)
     CROSS JOIN LATERAL (
       (${packetsFor('rx_node_id')})
       UNION ALL
       (${packetsFor('src_node_id')})
     ) recent
     ORDER BY time DESC LIMIT 500`,
    [nodeId, hours, ...scope.params]
  );
  return res.rows;
}

export async function getNodeAdverts(nodePublicKey: string, hours = 24, limit = 100, network = 'ukmesh') {
  const scope = buildScopePlaceholders(4, network);
  // Get location packets (packet_type = 4) where payload->>'publicKey' = this public key
  // Location packets are sent as part of the advert broadcast
  const res = await pool.query(
    `SELECT time, packet_hash
     FROM mesh.packets p
     WHERE p.packet_type = 4
       AND UPPER(BTRIM(COALESCE(p.src_node_id, p.payload->>'publicKey')))
             = ANY(${nodeAliasArraySql('$1')})
       AND p.time > NOW() - INTERVAL '1 hour' * $2
       ${buildPacketScopeClause(scope, 'p', network)}
       ${buildPublicPacketPrivacyClause('p')}
     ORDER BY time DESC
     LIMIT $3`,
    [nodePublicKey, hours, limit, ...scope.params]
  );
  return res.rows;
}

/**
 * Top adverting repeaters over a rolling window. Counts come from
 * core.node_counted_adverts (deduplicated per canonical advert hash, ~16k rows),
 * joined to the node identity view for display fields. Cheap enough to run
 * directly; the route wraps it in a 1h in-memory cache.
 */
export async function getTopAdvertingRepeaters(hours = 24, limit = 10, network = 'ukmesh') {
  const scope = buildScopePlaceholders(3, network);
  const res = await pool.query(
    `SELECT
       nca.node_id,
       n.name,
       COALESCE(n.observer_iata, n.iata) AS iata,
       n.role,
       n.lat,
       n.lon,
       ${nodeEffectiveLastSeenSql('n')} AS last_seen,
       ${nodeEffectiveOnlineSql('n')} AS is_online,
       n.advert_count,
       COUNT(*) AS adverts_in_window,
       MAX(nca.counted_at) AS last_advert_at
     FROM core.node_counted_adverts nca
     JOIN core.node_identity_nodes n ON n.node_id = nca.node_id
     WHERE nca.counted_at > NOW() - INTERVAL '1 hour' * $1
       AND n.role = 2
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       ${buildNodeScopeClause(scope, 'n')}
     GROUP BY nca.node_id, n.name, COALESCE(n.observer_iata, n.iata), n.role,
              n.lat, n.lon, ${nodeEffectiveLastSeenSql('n')},
              ${nodeEffectiveOnlineSql('n')}, n.advert_count
     ORDER BY adverts_in_window DESC, n.name ASC NULLS LAST
     LIMIT $2`,
    [hours, limit, ...scope.params]
  );
  return res.rows;
}
