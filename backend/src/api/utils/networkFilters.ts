import { UKMESH_NETWORKS } from '../../networks.js';
import type { VisibilityScope } from '../../http/requestScope.js';

export type NetworkFilters = {
  params: unknown[];
  packets: string;
  packetsAlias: (alias: string) => string;
  nodes: string;
  nodesAlias: (alias: string) => string;
};

/** Resolve one requested identity into its canonical id and every stored alias.
 * The subquery is uncorrelated, so PostgreSQL evaluates it once while retaining
 * an indexable `packet_node_id = ANY(...)` predicate on packet scans. */
export function nodeAliasArraySql(requestedNodeParam: string): string {
  return `ARRAY(
    WITH requested_identity AS MATERIALIZED (
      SELECT COALESCE(
        (SELECT alias.canonical_node_id
           FROM node_identity_aliases alias
          WHERE alias.source_node_id = UPPER(BTRIM(${requestedNodeParam}))),
        UPPER(BTRIM(${requestedNodeParam}))
      ) AS canonical_node_id
    )
    SELECT canonical_node_id FROM requested_identity
    UNION
    SELECT alias.source_node_id
      FROM node_identity_aliases alias
      JOIN requested_identity requested
        ON requested.canonical_node_id = alias.canonical_node_id
  )`;
}

export function publicPacketPrivacySql(alias?: string): string {
  const prefix = alias ? `${alias}.` : 'packets.';
  return `(
    ${prefix}visibility_ok IS TRUE
    AND ${prefix}is_private IS NOT TRUE
    AND EXISTS (
      SELECT 1
      FROM packet_visibility_materialization_state cached_visibility
      JOIN public_visibility_state current_visibility
        ON current_visibility.singleton = cached_visibility.singleton
      WHERE cached_visibility.singleton = TRUE
        AND cached_visibility.visibility_generation = current_visibility.generation
    )
  )`;
}

/** Durable packet paths outlive packet privacy bits. Derive visibility from
 * the current private-prefix tables whenever a retained path is consumed. */
export function publicPacketPathPrivacySql(alias = 'packet_paths'): string {
  const prefix = alias ? `${alias}.` : '';
  return `(
    meshcore_path_is_valid(${prefix}path_hashes, ${prefix}path_hash_size_bytes)
    AND NOT meshcore_path_matches_private(
      ${prefix}network,
      ${prefix}rx_node_id,
      ${prefix}src_node_id,
      ${prefix}path_hashes,
      ${prefix}path_hash_size_bytes
    )
  )`;
}

function publicPacketPrivacyConditions(prefix: string): string[] {
  const alias = prefix.endsWith('.') ? prefix.slice(0, -1) : prefix;
  return [publicPacketPrivacySql(alias || undefined)];
}

function excludesLegacyTestTopic(prefix: string): string {
  return `COALESCE(NULLIF(${prefix}topic_prefix, ''), split_part(${prefix}topic, '/', 1)) <> 'meshcore-test'`;
}

export function networkFilters(
  network?: string,
  observer?: string,
  opts?: { includePrivacy?: boolean },
): NetworkFilters {
  const params: unknown[] = [];
  let networkParam: string | null = null;
  let networkIsMulti = false;
  let observerParam: string | null = null;

  if (network) {
    networkParam = `$${params.length + 1}`;
    if (network === 'ukmesh') {
      params.push(UKMESH_NETWORKS);
      networkIsMulti = true;
    } else {
      params.push(network);
    }
  }

  if (observer) {
    observerParam = `$${params.length + 1}`;
    params.push(observer);
  }

  const netEq = networkParam
    ? (networkIsMulti ? `network = ANY(${networkParam})` : `network = ${networkParam}`)
    : null;

  const packetConditions: string[] = [];
  if (netEq) {
    packetConditions.push(netEq);
    if (network !== 'test') {
      packetConditions.push(excludesLegacyTestTopic(''));
    }
  } else {
    packetConditions.push(`network IS DISTINCT FROM 'test'`);
    packetConditions.push(excludesLegacyTestTopic(''));
    packetConditions.push(`COALESCE(rx_node_id, '') NOT IN (SELECT node_id FROM nodes WHERE network = 'test')`);
  }
  if (observerParam) {
    packetConditions.push(
      `rx_node_id = ANY(${nodeAliasArraySql(observerParam)})`,
    );
  }
  if (opts?.includePrivacy !== false) {
    packetConditions.push(...publicPacketPrivacyConditions(''));
  }

  const nodeConditions = (alias?: string) => {
    const prefix = alias ? `${alias}.` : '';
    // The outer node_id reference inside EXISTS must be qualified: an unqualified
    // node_id there resolves to the inner table's column. Callers without an
    // alias must be querying the nodes table directly.
    const nodeRef = alias ? `${alias}.node_id` : 'nodes.node_id';
    const conditions: string[] = [];
    if (networkParam) {
      const netMatch = networkIsMulti ? `= ANY(${networkParam})` : `= ${networkParam}`;
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
              FROM node_identity_sightings s
              WHERE s.node_id = ${nodeRef}
                AND s.network ${netMatch}
                AND s.last_seen_at > NOW() - INTERVAL '30 days'
            )
          )
        )`,
      );
    } else conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
    if (observerParam) {
      const pNetCond = networkParam
        ? (networkIsMulti ? `AND p.network = ANY(${networkParam})` : `AND p.network = ${networkParam}`)
        : '';
      // IN (uncorrelated subquery) so the planner hashes the observer's
      // heard-node set once; a correlated EXISTS here runs per node row.
      // 7-day window keeps the packet scan inside recent chunks.
      conditions.push(
        `(
          ${nodeRef} = ANY(${nodeAliasArraySql(observerParam)})
          OR ${nodeRef} IN (
            SELECT COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id)))
            FROM packets p
            LEFT JOIN node_identity_aliases src_alias
              ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
            WHERE p.rx_node_id = ANY(${nodeAliasArraySql(observerParam)})
              AND p.time > NOW() - INTERVAL '7 days'
              AND p.src_node_id IS NOT NULL
              ${pNetCond}
          )
        )`,
      );
    }
    return conditions;
  };

  return {
    params,
    packets: packetConditions.length > 0 ? `AND ${packetConditions.join(' AND ')}` : '',
    packetsAlias: (alias: string) => {
      const prefix = `${alias}.`;
      const conditions: string[] = [];
      if (networkParam) {
        conditions.push(networkIsMulti
          ? `${prefix}network = ANY(${networkParam})`
          : `${prefix}network = ${networkParam}`);
        // `test` explicitly requests test traffic. Public scopes exclude the
        // legacy test topic marker as a defence-in-depth check for old rows.
        if (network !== 'test') {
          conditions.push(excludesLegacyTestTopic(prefix));
        }
      } else {
        conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
        conditions.push(excludesLegacyTestTopic(prefix));
        conditions.push(`COALESCE(${prefix}rx_node_id, '') NOT IN (SELECT node_id FROM nodes WHERE network = 'test')`);
      }
      if (observerParam) {
        conditions.push(
          `${prefix}rx_node_id = ANY(${nodeAliasArraySql(observerParam)})`,
        );
      }
      conditions.push(...publicPacketPrivacyConditions(prefix));
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
    nodes: nodeConditions().length > 0 ? `AND ${nodeConditions().join(' AND ')}` : '',
    nodesAlias: (alias: string) => {
      const conditions = nodeConditions(alias);
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
  };
}

export function publicNetworkFilters(scope: VisibilityScope): NetworkFilters {
  return networkFilters(scope.network, scope.observer);
}
