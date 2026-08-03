function safeAlias(alias: string): string {
  if (!/^[a-z][a-z0-9_]*$/i.test(alias)) {
    throw new Error(`invalid SQL alias: ${alias}`);
  }
  return alias;
}

/** Newest direct or path-derived proof that a node was present. */
export function nodeEffectiveLastSeenSql(alias = 'n'): string {
  const table = safeAlias(alias);
  return `GREATEST(
    ${table}.last_seen,
    ${table}.last_rx_at,
    ${table}.last_status_at,
    ${table}.last_path_evidence_at
  )`;
}

/**
 * Path evidence is an independent online signal. It must take precedence over
 * an older direct-RX/status timestamp rather than being hidden by it.
 */
export function nodeEffectiveOnlineSql(
  alias = 'n',
  referenceSql = 'NOW()',
): string {
  const table = safeAlias(alias);
  return `CASE
    WHEN ${table}.last_path_evidence_at IS NOT NULL
      AND ${table}.last_path_evidence_at > ${referenceSql} - INTERVAL '60 minutes'
    THEN TRUE
    WHEN GREATEST(${table}.last_rx_at, ${table}.last_status_at) IS NOT NULL
    THEN GREATEST(${table}.last_rx_at, ${table}.last_status_at)
      > ${referenceSql} - INTERVAL '15 minutes'
    ELSE ${table}.is_online
  END`;
}
