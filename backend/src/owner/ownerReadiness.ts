import {
  buildOwnerInventoryBaseline,
  type OwnerAclGenerationState,
  type OwnerInventoryAccountRow,
  type OwnerInventoryBaseline,
} from './ownerInventoryBaseline.js';
import { validateOwnerInventoryReadiness, type AppliedOwnerAclArtifact } from './ownerInventoryReadiness.js';
import type { ConfiguredOwnerGrant } from './ownerGrantConfig.js';

export const OWNER_READINESS_FORMAT = 'meshcore-owner-readiness-v1';

/** Shared evidence for public readiness and the authenticated operator endpoint. */
export function createOwnerAuthorizationReadiness(input: {
  config: () => { mode: string; aclMode: string; baselinePath: string; configuredGrants: ConfiguredOwnerGrant[] };
  readState: () => Promise<OwnerAclGenerationState & { lastVerifiedAt: string | null }>;
  readInventory: () => Promise<{ accounts: OwnerInventoryAccountRow[] }>;
  readAcl: () => string;
  readBaseline: (path: string) => OwnerInventoryBaseline;
  loadAppliedArtifact: (generation: string) => Promise<AppliedOwnerAclArtifact | null>;
}) {
  return async () => {
    const config = input.config();
    const aclState = await input.readState();
    let inventoryBaseline = {
      required: Boolean(config.baselinePath),
      ok: !config.baselinePath,
      mismatches: [] as string[],
      reason: 'owner-baseline-not-required',
    };
    if (config.baselinePath) {
      const inventory = await input.readInventory();
      const aclContent = input.readAcl();
      const current = buildOwnerInventoryBaseline({
        accounts: inventory.accounts,
        configuredGrants: config.configuredGrants,
        aclContent,
        aclState,
      });
      inventoryBaseline = {
        required: true,
        ...await validateOwnerInventoryReadiness(input.readBaseline(config.baselinePath), current, {
          aclContent, loadAppliedArtifact: input.loadAppliedArtifact,
        }),
      };
    }
    return {
      format: OWNER_READINESS_FORMAT,
      ownerAuthorization: { mode: config.mode, aclMode: config.aclMode, ...aclState, inventoryBaseline },
    };
  };
}

export function publicReadinessPayload(ready: boolean, database: boolean, mqttState: string, ts = Date.now()) {
  return { status: ready ? 'ready' : 'degraded', checks: { database, mqtt: { state: mqttState } }, ts };
}
