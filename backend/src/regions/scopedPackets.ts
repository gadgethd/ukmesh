import { publicPacketPrivacySql } from '../platform/query/networkFilters.js';
import { UKMESH_NETWORK_SQL_LIST } from '../networks.js';

export interface ScopedPacketRow {
  packet_hash: string;
  t0: Date;
  t1: Date;
  raw: string | null;
  elems: string[] | null;
  observers: string[] | null;
}

// Keep each packet's path union local instead of globally sorting expanded paths.
// Native array text round-trips variable lengths, dimensions, nulls and escaping.
export const SCOPED_PACKETS_SQL = `WITH scoped_observations AS (
  SELECT p.packet_hash, p.time, p.raw_hex, p.rx_node_id, p.path_hashes, p.route_type
    FROM mesh.packets p
        WHERE p.network IN (${UKMESH_NETWORK_SQL_LIST})
          AND ${publicPacketPrivacySql('p')}
          AND p.route_type IN (0, 3)
          AND p.time > now() - make_interval(days => $1)
), packet_observation_groups AS (
  SELECT p.packet_hash,
         array_agg(p.path_hashes::text) AS paths,
         min(p.time) AS t0,
         max(p.time) AS t1,
         (array_agg(p.raw_hex ORDER BY p.time DESC)
                 FILTER (WHERE p.route_type IN (0, 3) AND p.raw_hex <> ''))[1] AS raw,
         (array_agg(DISTINCT p.rx_node_id) FILTER (WHERE p.rx_node_id IS NOT NULL))[1:40] AS observers
    FROM scoped_observations p
   GROUP BY p.packet_hash
)
SELECT observations.packet_hash, observations.t0, observations.t1,
       observations.raw,
       (SELECT (array_agg(DISTINCT e) FILTER (WHERE e IS NOT NULL))[1:80]
          FROM unnest(observations.paths) AS path(value)
          CROSS JOIN LATERAL unnest(path.value::text[]) AS e) AS elems,
       observations.observers
  FROM packet_observation_groups observations
 ORDER BY observations.packet_hash
 LIMIT $2
`;
