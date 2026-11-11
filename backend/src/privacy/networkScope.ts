import { UKMESH_NETWORKS } from '../networks.js';

const SQL_ALIAS = /^[A-Za-z_][A-Za-z0-9_]*$/;

function checkedAlias(value: string): string {
  if (!SQL_ALIAS.test(value)) throw new Error('INVALID_SQL_ALIAS');
  return value;
}

/**
 * Correlate a private node with a packet without allowing isolated test
 * identities to suppress production traffic. Historical production labels
 * remain one compatibility family.
 */
export function privateNodePacketNetworkMatchSql(
  privateNodeAlias: string,
  packetAlias: string,
): string {
  checkedAlias(privateNodeAlias);
  const packet = checkedAlias(packetAlias);
  return `${packet}.is_private IS TRUE`;
}

export function networksSharePrivacyScope(
  nodeNetwork: string,
  packetNetwork: string,
): boolean {
  if (nodeNetwork === packetNetwork) return true;
  const production = new Set<string>(UKMESH_NETWORKS);
  return production.has(nodeNetwork) && production.has(packetNetwork);
}

/** SQL predicate excluding packets that involve a private (🚫) node.
 *  Shared by every resolver so a privacy rule change cannot drift. */
export function packetExcludesPrivateNodesSql(packetAlias: string): string {
  const packet = checkedAlias(packetAlias);
  return `NOT EXISTS (
    SELECT 1 FROM core.nodes private_node
    WHERE private_node.name LIKE '%🚫%'
      AND ${privateNodePacketNetworkMatchSql('private_node', packet)}
      AND (
        private_node.node_id IN (${packet}.rx_node_id, ${packet}.src_node_id)
        OR EXISTS (
          SELECT 1
          FROM unnest(COALESCE(${packet}.path_hashes, ARRAY[]::text[])) AS path_hash
          WHERE ${packet}.path_hash_size_bytes BETWEEN 1 AND 3
            AND UPPER(private_node.node_id) LIKE UPPER(path_hash) || '%'
        )
      )
  )`;
}
