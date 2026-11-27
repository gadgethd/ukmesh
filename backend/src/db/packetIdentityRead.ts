/**
 * Packet identity read authority.
 *
 * `uid` groups by the canonical 32-byte content UID and falls back to the
 * stored hash when a historical row has no UID. `hash` is the rollback
 * authority and preserves the pre-P2-10 SQL shape. Both produce the same
 * public row shape; `packet_hash` remains the public contract field.
 */
export type PacketIdentityReadAuthority = 'uid' | 'hash';

export function packetIdentityReadAuthority(
  env: NodeJS.ProcessEnv = process.env,
): PacketIdentityReadAuthority {
  const raw = String(env['PACKET_IDENTITY_READ_AUTHORITY'] ?? 'uid').trim().toLowerCase();
  if (raw === 'uid' || raw === 'hash') return raw;
  throw new Error(
    `PACKET_IDENTITY_READ_AUTHORITY must be "uid" or "hash" (received ${JSON.stringify(raw)})`,
  );
}

export function packetIdentityKeySql(
  alias: string,
  authority: PacketIdentityReadAuthority = packetIdentityReadAuthority(),
): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) {
    throw new Error(`unsafe SQL alias: ${JSON.stringify(alias)}`);
  }
  return authority === 'uid'
    ? `UPPER(COALESCE(encode(${alias}.packet_uid, 'hex'), ${alias}.packet_hash))`
    : `${alias}.packet_hash`;
}

/** Select the latest eligible sibling decryption with an exact-hash fallback.
 * Callers supply the UID expression and scope for their bounded read of p.
 * Avoid a window-wide scan when the reader already has a small candidate set.
 */
export function packetDecryptionJoinSql(
  uidSql: string,
  siblingScopeSql: string,
  authority: PacketIdentityReadAuthority = packetIdentityReadAuthority(),
): { joins: string; summary: string; decrypted: string } {
  const exact = 'LEFT JOIN mesh.packet_decryptions exact_dec ON exact_dec.packet_hash = p.packet_hash';
  if (authority === 'hash') {
    return { joins: exact, summary: 'exact_dec.summary', decrypted: 'exact_dec.decrypted' };
  }

  return {
    joins: `LEFT JOIN LATERAL (${packetSiblingDecryptionSql(uidSql, siblingScopeSql)}) identity_dec ON TRUE
    ${exact}`,
    summary: 'COALESCE(identity_dec.summary, exact_dec.summary)',
    decrypted: 'COALESCE(identity_dec.decrypted, exact_dec.decrypted)',
  };
}

export function packetSiblingDecryptionSql(uidSql: string, siblingScopeSql: string): string {
  return `SELECT pd.summary, pd.decrypted
    FROM mesh.packets sib
    JOIN mesh.packet_decryptions pd ON pd.packet_hash = sib.packet_hash
    WHERE sib.packet_uid = ${uidSql}
      ${siblingScopeSql}
    ORDER BY pd.created_at DESC, pd.packet_hash
    LIMIT 1`;
}
