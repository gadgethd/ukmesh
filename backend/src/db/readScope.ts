import { UKMESH_NETWORKS } from '../networks.js';
import { nodeAliasArraySql } from '../platform/query/networkFilters.js';
import { publicPacketPrivacySql } from '../platform/query/networkFilters.js';
import { regionMemberIdsSql } from '../regions/filter.js';

export type ScopePlaceholders = {
  params: unknown[];
  networkParam: string | null;
  networkIsMulti: boolean;
  observerParam: string | null;
  regionParam: string | null;
};

export function buildScopePlaceholders(startIndex: number, network?: string, observer?: string, region?: string): ScopePlaceholders {
  const params: unknown[] = [];
  let idx = startIndex;
  let networkParam: string | null = null;
  let networkIsMulti = false;
  if (network) {
    networkParam = `$${idx++}`;
    if (network === 'ukmesh') {
      params.push(UKMESH_NETWORKS);
      networkIsMulti = true;
    } else {
      params.push(network);
    }
  }
  const observerParam = observer ? `$${idx++}` : null;
  if (observer) params.push(observer);
  const regionParam = region ? `$${idx++}` : null;
  if (region) params.push(region);
  return { params, networkParam, networkIsMulti, observerParam, regionParam };
}

export function buildPacketScopeClause(
  placeholders: ScopePlaceholders,
  alias?: string,
  network?: string,
): string {
  const prefix = alias ? `${alias}.` : '';
  const conditions: string[] = [];
  if (placeholders.regionParam) {
    conditions.push(`${prefix}region_scope = '#' || ${placeholders.regionParam}::text`);
  }
  if (placeholders.networkParam) {
    const netCond = placeholders.networkIsMulti
      ? `${prefix}network = ANY(${placeholders.networkParam})`
      : `${prefix}network = ${placeholders.networkParam}`;
    conditions.push(netCond);
    if (network !== 'test') {
      conditions.push(
        `COALESCE(NULLIF(${prefix}topic_prefix, ''), split_part(${prefix}topic, '/', 1)) <> 'meshcore-test'`,
      );
    }
  } else {
    conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
    conditions.push(
      `COALESCE(NULLIF(${prefix}topic_prefix, ''), split_part(${prefix}topic, '/', 1)) <> 'meshcore-test'`,
    );
  }
  if (placeholders.observerParam) {
    conditions.push(
      `${prefix}rx_node_id = ANY(${nodeAliasArraySql(placeholders.observerParam)})`,
    );
  }
  return conditions.length > 0 ? ` AND ${conditions.join(' AND ')}` : '';
}

export function buildPublicPacketPrivacyClause(alias?: string): string {
  return ` AND ${publicPacketPrivacySql(alias)}`;
}

export function buildNodeScopeClause(
  placeholders: ScopePlaceholders,
  alias?: string,
): string {
  const prefix = alias ? `${alias}.` : '';
  // The outer node_id reference inside EXISTS must be qualified: an unqualified
  // node_id there resolves to s.node_id (always true). Callers without an alias
  // must be querying the nodes table directly.
  const nodeRef = alias ? `${alias}.node_id` : 'nodes.node_id';
  const conditions: string[] = [];
  if (placeholders.regionParam) {
    conditions.push(`${nodeRef} IN (${regionMemberIdsSql(placeholders.regionParam)})`);
  }

  if (placeholders.networkParam) {
    const netMatch = placeholders.networkIsMulti
      ? `= ANY(${placeholders.networkParam})`
      : `= ${placeholders.networkParam}`;
    // nodes.network is last-writer-wins across observers, so a node heard on
    // two networks flip-flops between them and randomly drops off each map.
    // node_network_sightings records every network a node is active on, so
    // also include nodes recently sighted on the requested network(s).
    conditions.push(
      `(
        ${prefix}network ${netMatch}
        OR (
          ${prefix}network IS DISTINCT FROM 'test'
          AND EXISTS (
            SELECT 1
            FROM core.node_identity_sightings s
            WHERE s.node_id = ${nodeRef}
              AND s.network ${netMatch}
              AND s.last_seen_at > NOW() - INTERVAL '30 days'
          )
        )
      )`,
    );
  } else {
    conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
  }

  if (placeholders.observerParam) {
    const netCond = placeholders.networkParam
      ? (placeholders.networkIsMulti
          ? `AND p.network = ANY(${placeholders.networkParam})`
          : `AND p.network = ${placeholders.networkParam}`)
      : '';
    // IN (uncorrelated subquery) so the planner hashes the observer's heard-node
    // set once; a correlated EXISTS here runs per node row (minutes, not ms).
    // 7-day window matches the observer_meta lookback and keeps the packet
    // scan inside recent chunks (~700ms vs 5min unbounded).
    const observerNodeScope = [
      `${nodeRef} = ANY(${nodeAliasArraySql(placeholders.observerParam)})`,
      `OR ${nodeRef} IN (
         SELECT COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id)))
         FROM mesh.packets p
         LEFT JOIN core.node_identity_aliases src_alias
           ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
         WHERE p.rx_node_id = ANY(${nodeAliasArraySql(placeholders.observerParam)})
           AND p.time > NOW() - INTERVAL '7 days'
           AND p.src_node_id IS NOT NULL
           AND ${publicPacketPrivacySql('p')}`,
      netCond,
      `)`,
    ].filter(Boolean).join(' ');
    conditions.push(`(${observerNodeScope})`);
  }

  return conditions.length > 0 ? ` AND ${conditions.join(' AND ')}` : '';
}
