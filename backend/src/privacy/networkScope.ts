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
  const production = new Set(['ukmesh', 'northeast', 'teesside']);
  return production.has(nodeNetwork) && production.has(packetNetwork);
}
