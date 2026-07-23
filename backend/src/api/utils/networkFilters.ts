import { UKMESH_NETWORKS } from '../../networks.js';

export type NetworkFilters = {
  params: unknown[];
  packets: string;
  packetsAlias: (alias: string) => string;
  nodes: string;
  nodesAlias: (alias: string) => string;
};

export function networkFilters(network?: string, observer?: string): NetworkFilters {
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
  if (netEq) packetConditions.push(netEq);
  else {
    packetConditions.push(`network IS DISTINCT FROM 'test'`);
    packetConditions.push(`COALESCE(rx_node_id, '') NOT IN (SELECT node_id FROM nodes WHERE network = 'test')`);
  }
  if (observerParam) packetConditions.push(`rx_node_id = ${observerParam}`);

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
              FROM node_network_sightings s
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
          ${prefix}node_id = ${observerParam}
          OR ${nodeRef} IN (
            SELECT p.src_node_id
            FROM packets p
            WHERE p.rx_node_id = ${observerParam}
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
          conditions.push(`split_part(${prefix}topic, '/', 1) <> 'meshcore-test'`);
        }
      } else {
        conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
        conditions.push(`split_part(${prefix}topic, '/', 1) <> 'meshcore-test'`);
        conditions.push(`COALESCE(${prefix}rx_node_id, '') NOT IN (SELECT node_id FROM nodes WHERE network = 'test')`);
      }
      if (observerParam) conditions.push(`${prefix}rx_node_id = ${observerParam}`);
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
    nodes: nodeConditions().length > 0 ? `AND ${nodeConditions().join(' AND ')}` : '',
    nodesAlias: (alias: string) => {
      const conditions = nodeConditions(alias);
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
  };
}
