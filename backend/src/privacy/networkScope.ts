const SQL_ALIAS = /^[A-Za-z_][A-Za-z0-9_]*$/;
const PRODUCTION_NETWORKS_SQL = "'ukmesh', 'northeast', 'teesside'";

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
  const privateNode = checkedAlias(privateNodeAlias);
  const packet = checkedAlias(packetAlias);
  return `(
    ${privateNode}.network = ${packet}.network
    OR (
      ${privateNode}.network IN (${PRODUCTION_NETWORKS_SQL})
      AND ${packet}.network IN (${PRODUCTION_NETWORKS_SQL})
    )
  )`;
}

export function networksSharePrivacyScope(
  nodeNetwork: string,
  packetNetwork: string,
): boolean {
  if (nodeNetwork === packetNetwork) return true;
  const production = new Set(['ukmesh', 'northeast', 'teesside']);
  return production.has(nodeNetwork) && production.has(packetNetwork);
}
