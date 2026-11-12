import { prefixCountKey, positionPrefixKey, corridorKey, transitionKey, positionTransitionKey } from './priorKeys.js';

/** Count-based evidence published by the multibyte path learner. */
export type ChampionPriorRows = {
  positionPrefixes: { prefix: string; position: number; node_id: string; count: number | string }[];
  corridors: { src_node_id: string; rx_node_id: string; position: number; node_id: string; count: number | string }[];
  transitions: { from_node_id: string; to_node_id: string; count: number | string }[];
  positionTransitions: { position: number; from_node_id: string; to_node_id: string; count: number | string }[];
};

export type ChampionPriors = {
  prefixCounts: Map<string, number>;
  positionPrefixCounts: Map<string, number>;
  corridorCounts: Map<string, number>;
  transitionCounts: Map<string, number>;
  positionTransitionCounts: Map<string, number>;
  verifiedEdgeCounts: Map<string, number>;
};

export function buildChampionPriors(rows: ChampionPriorRows): ChampionPriors {
  const result: ChampionPriors = {
    prefixCounts: new Map(),
    positionPrefixCounts: new Map(),
    corridorCounts: new Map(),
    transitionCounts: new Map(),
    positionTransitionCounts: new Map(),
    verifiedEdgeCounts: new Map(),
  };
  const add = (map: Map<string, number>, key: string, value: number | string) => {
    const count = Number(value);
    if (Number.isFinite(count) && count > 0) map.set(key, (map.get(key) ?? 0) + count);
  };
  for (const row of rows.positionPrefixes) {
    const prefix = row.prefix.toUpperCase().slice(0, 2);
    add(result.prefixCounts, prefixCountKey(prefix, row.node_id), row.count);
    add(result.positionPrefixCounts, positionPrefixKey(prefix, row.position, row.node_id), row.count);
  }
  for (const row of rows.corridors) {
    add(result.corridorCounts, corridorKey(row.src_node_id, row.rx_node_id, row.position, row.node_id), row.count);
  }
  for (const row of rows.transitions) {
    add(result.transitionCounts, transitionKey(row.from_node_id, row.to_node_id), row.count);
  }
  for (const row of rows.positionTransitions) {
    add(result.positionTransitionCounts, positionTransitionKey(row.position, row.from_node_id, row.to_node_id), row.count);
    add(result.verifiedEdgeCounts, transitionKey(row.from_node_id, row.to_node_id), row.count);
  }
  return result;
}
