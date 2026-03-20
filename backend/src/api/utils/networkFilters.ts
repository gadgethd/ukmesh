export type NetworkFilters = {
  params: string[];
  packets: string;
  packetsAlias: (alias: string) => string;
  nodes: string;
  nodesAlias: (alias: string) => string;
};

export function networkFilters(network?: string, observer?: string): NetworkFilters {
  const params: string[] = [];
  let networkParam: string | null = null;
  let observerParam: string | null = null;

  if (network) {
    networkParam = `$${params.length + 1}`;
    params.push(network);
  }

  if (observer) {
    observerParam = `$${params.length + 1}`;
    params.push(observer);
  }

  const packetConditions: string[] = [];
  if (networkParam) packetConditions.push(`network = ${networkParam}`);
  else {
    packetConditions.push(`network IS DISTINCT FROM 'test'`);
    packetConditions.push(`COALESCE(rx_node_id, '') NOT IN (SELECT node_id FROM nodes WHERE network = 'test')`);
  }
  if (observerParam) packetConditions.push(`rx_node_id = ${observerParam}`);

  const nodeConditions = (alias?: string) => {
    const prefix = alias ? `${alias}.` : '';
    const conditions: string[] = [];
    if (networkParam) conditions.push(`${prefix}network = ${networkParam}`);
    else conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
    if (observerParam) {
      conditions.push(
        `(
          ${prefix}node_id = ${observerParam}
          OR EXISTS (
            SELECT 1
            FROM packets p
            WHERE p.rx_node_id = ${observerParam}
              ${networkParam ? `AND p.network = ${networkParam}` : ''}
              AND p.src_node_id = ${prefix}node_id
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
        conditions.push(`${prefix}network = ${networkParam}`);
        conditions.push(`split_part(${prefix}topic, '/', 1) <> 'meshcore-test'`);
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
