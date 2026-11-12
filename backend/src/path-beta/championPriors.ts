import { buildChampionPriors, type ChampionPriorRows, type ChampionPriors } from '../path-shared/priors.js';

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
      'SELECT prefix, position, node_id, count FROM analytics.path_position_prefix_priors WHERE network = $1'),
    read<ChampionPriorRows['corridors'][number]>(
      'SELECT src_node_id, rx_node_id, position, node_id, count FROM analytics.path_corridor_priors WHERE network = $1'),
    read<ChampionPriorRows['transitions'][number]>(
      'SELECT from_node_id, to_node_id, count FROM analytics.path_transition_priors WHERE network = $1'),
    read<ChampionPriorRows['positionTransitions'][number]>(
      'SELECT position, from_node_id, to_node_id, count FROM analytics.path_position_transition_priors WHERE network = $1'),
  ]);
  return buildChampionPriors({ positionPrefixes, corridors, transitions, positionTransitions });
}
