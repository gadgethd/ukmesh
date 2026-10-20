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

type PriorQuery = <T extends Record<string, unknown>>(
  sql: string, params: unknown[], signal?: AbortSignal,
) => Promise<{ rows: T[] }>;

/** Loaded once per cached resolver context, never once per packet. */
export async function loadChampionPriors(network: string, query: PriorQuery, signal?: AbortSignal): Promise<ChampionPriors> {
  // Refuse an incomplete model instead of silently removing rare choices.
  const limit = 100_000;
  const read = async <T extends Record<string, unknown>>(sql: string): Promise<T[]> => {
    const result = await query<T>(`${sql} LIMIT $2`, [network, limit + 1], signal);
    if (result.rows.length > limit) throw new Error('PATH_CHAMPION_PRIOR_LIMIT');
    return result.rows;
  };
  const [positionPrefixes, corridors, transitions, positionTransitions] = await Promise.all([
    read<ChampionPriorRows['positionPrefixes'][number]>(
      'SELECT prefix, position, node_id, count FROM path_position_prefix_priors WHERE network = $1'),
    read<ChampionPriorRows['corridors'][number]>(
      'SELECT src_node_id, rx_node_id, position, node_id, count FROM path_corridor_priors WHERE network = $1'),
    read<ChampionPriorRows['transitions'][number]>(
      'SELECT from_node_id, to_node_id, count FROM path_transition_priors WHERE network = $1'),
    read<ChampionPriorRows['positionTransitions'][number]>(
      'SELECT position, from_node_id, to_node_id, count FROM path_position_transition_priors WHERE network = $1'),
  ]);
  return buildChampionPriors({ positionPrefixes, corridors, transitions, positionTransitions });
}

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
    add(result.prefixCounts, `${prefix}|${row.node_id}`, row.count);
    add(result.positionPrefixCounts, `${prefix}|${row.position}|${row.node_id}`, row.count);
  }
  for (const row of rows.corridors) {
    add(result.corridorCounts, `${row.src_node_id}|${row.rx_node_id}|${row.position}|${row.node_id}`, row.count);
  }
  for (const row of rows.transitions) {
    add(result.transitionCounts, `${row.from_node_id}|${row.to_node_id}`, row.count);
  }
  for (const row of rows.positionTransitions) {
    add(result.positionTransitionCounts, `${row.position}|${row.from_node_id}|${row.to_node_id}`, row.count);
    add(result.verifiedEdgeCounts, `${row.from_node_id}|${row.to_node_id}`, row.count);
  }
  return result;
}
