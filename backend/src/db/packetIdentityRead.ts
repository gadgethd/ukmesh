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
    ? `COALESCE(encode(${alias}.packet_uid, 'hex'), ${alias}.packet_hash)`
    : `${alias}.packet_hash`;
}
