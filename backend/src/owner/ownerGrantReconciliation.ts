// newuser writes the durable map and grant before replacing the backend. The
// still-running container therefore needs enough time to observe a fresh row
// without treating it as a removal from its older in-memory configuration.
export const OWNER_GRANT_REVOCATION_GRACE_MS = 120_000;

export type ConfiguredOwnerGrant = {
  mqttUsername: string;
  nodeId: string;
};

export type CurrentOwnerGrant = {
  mqttUsername: string;
  nodeId: string;
  revokedAt: string | Date | null;
  verificationMethod: string | null;
  grantGeneration: string | null;
  updatedAt: string | Date | null;
};

export type OwnerGrantReconciliationAction =
  | {
    type: 'upsert';
    grant: ConfiguredOwnerGrant;
    reauthorize: boolean;
  }
  | {
    type: 'revoke';
    grant: ConfiguredOwnerGrant;
  };

function grantKey(grant: ConfiguredOwnerGrant): string {
  return `${grant.mqttUsername}\0${grant.nodeId}`;
}

function isWithinRevocationGrace(
  updatedAt: string | Date | null,
  nowMs: number,
  graceMs: number,
): boolean {
  if (!updatedAt || graceMs <= 0) return false;
  const updatedAtMs = updatedAt instanceof Date ? updatedAt.getTime() : Date.parse(updatedAt);
  return Number.isFinite(updatedAtMs)
    && Math.abs(nowMs - updatedAtMs) < graceMs;
}

export function planOperatorConfiguredOwnerGrantSync(
  desired: ConfiguredOwnerGrant[],
  current: CurrentOwnerGrant[],
  generation: string,
  options: { nowMs?: number; revocationGraceMs?: number } = {},
): OwnerGrantReconciliationAction[] {
  const nowMs = options.nowMs ?? Date.now();
  const revocationGraceMs = options.revocationGraceMs ?? OWNER_GRANT_REVOCATION_GRACE_MS;
  const desiredKeys = new Set(desired.map(grantKey));
  const currentByKey = new Map(current.map((grant) => [grantKey(grant), grant]));
  const actions: OwnerGrantReconciliationAction[] = [];

  for (const grant of desired) {
    const row = currentByKey.get(grantKey(grant));
    if (row?.verificationMethod === 'operator-database' && !row.revokedAt) continue;
    if (row?.verificationMethod === 'operator-config'
      && !row.revokedAt
      && row.grantGeneration === generation) continue;
    // A configured grant is authoritative even if an older reconcile tick
    // tombstoned it during rollout.
    actions.push({ type: 'upsert', grant, reauthorize: Boolean(row?.revokedAt) });
  }

  for (const row of current) {
    if (row.verificationMethod !== 'operator-config'
      || row.revokedAt
      || desiredKeys.has(grantKey(row))
      || isWithinRevocationGrace(row.updatedAt, nowMs, revocationGraceMs)) continue;
    actions.push({
      type: 'revoke',
      grant: { mqttUsername: row.mqttUsername, nodeId: row.nodeId },
    });
  }

  return actions;
}
