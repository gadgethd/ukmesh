import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { getNodeIdsForUserInAcl } from '../mqtt/aclManager.js';
import {
  ownerGrantConfigGeneration,
  type ConfiguredOwnerGrant,
} from './ownerGrantConfig.js';

export const OWNER_INVENTORY_BASELINE_FORMAT = 'meshcore-owner-inventory-v1';

export type OwnerInventoryAccountRow = {
  mqttUsername: string;
  isActive: boolean;
  nodeId: string | null;
  verificationMethod: string | null;
  verifiedAt: string | null;
  revokedAt: string | null;
};

export type OwnerAclGenerationState = {
  desiredGeneration: string | null;
  renderedGeneration: string | null;
  appliedGeneration: string | null;
  lastError: string | null;
};

export type ActiveOwnerGrant = {
  mqttUsername: string;
  nodeId: string;
  verificationMethod: string | null;
};

export type OwnerInventoryBaseline = {
  format: typeof OWNER_INVENTORY_BASELINE_FORMAT;
  generatedAt: string;
  legacyDisposition: 'preserve-unmodified-pending-owner-review';
  counts: {
    activeAccounts: number;
    activeGrants: number;
    operatorConfig: number;
    operatorDatabase: number;
    legacyOrNullMethod: number;
    configuredGrants: number;
    aclGrants: number;
  };
  configuredGeneration: string;
  aclState: OwnerAclGenerationState;
  activeGrants: ActiveOwnerGrant[];
  aclGrants: Array<{ mqttUsername: string; nodeId: string }>;
  contentSha256: string;
};

type BaselineContent = Omit<OwnerInventoryBaseline, 'contentSha256'>;

function canonicalJson(value: unknown): string {
  return JSON.stringify(value);
}

function contentSha256(value: BaselineContent): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

function withoutChecksum(baseline: OwnerInventoryBaseline): BaselineContent {
  const { contentSha256: _checksum, ...content } = baseline;
  return content;
}

export function buildOwnerInventoryBaseline(input: {
  accounts: readonly OwnerInventoryAccountRow[];
  configuredGrants: readonly ConfiguredOwnerGrant[];
  aclContent: string;
  aclState: OwnerAclGenerationState;
  generatedAt?: string;
}): OwnerInventoryBaseline {
  const activeGrants = input.accounts
    .filter((row): row is OwnerInventoryAccountRow & { nodeId: string } =>
      row.isActive
      && row.nodeId !== null
      && /^[0-9A-F]{64}$/.test(row.nodeId)
      && row.revokedAt === null)
    .map((row) => ({
      mqttUsername: row.mqttUsername,
      nodeId: row.nodeId,
      verificationMethod: row.verificationMethod,
    }))
    .sort((a, b) => a.mqttUsername.localeCompare(b.mqttUsername)
      || a.nodeId.localeCompare(b.nodeId));
  const activeUsernames = [...new Set(activeGrants.map((grant) => grant.mqttUsername))].sort();
  const aclGrants = activeUsernames.flatMap((mqttUsername) =>
    getNodeIdsForUserInAcl(input.aclContent, mqttUsername)
      .map((nodeId) => ({ mqttUsername, nodeId })))
    .sort((a, b) => a.mqttUsername.localeCompare(b.mqttUsername)
      || a.nodeId.localeCompare(b.nodeId));
  const content: BaselineContent = {
    format: OWNER_INVENTORY_BASELINE_FORMAT,
    generatedAt: input.generatedAt ?? new Date().toISOString(),
    legacyDisposition: 'preserve-unmodified-pending-owner-review',
    counts: {
      activeAccounts: activeUsernames.length,
      activeGrants: activeGrants.length,
      operatorConfig: activeGrants.filter((grant) => grant.verificationMethod === 'operator-config').length,
      operatorDatabase: activeGrants.filter((grant) => grant.verificationMethod === 'operator-database').length,
      legacyOrNullMethod: activeGrants.filter((grant) =>
        grant.verificationMethod !== 'operator-config'
        && grant.verificationMethod !== 'operator-database').length,
      configuredGrants: input.configuredGrants.length,
      aclGrants: aclGrants.length,
    },
    configuredGeneration: ownerGrantConfigGeneration([...input.configuredGrants]),
    aclState: input.aclState,
    activeGrants,
    aclGrants,
  };
  return { ...content, contentSha256: contentSha256(content) };
}

export function loadOwnerInventoryBaseline(filePath: string): OwnerInventoryBaseline {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8')) as OwnerInventoryBaseline;
  if (parsed.format !== OWNER_INVENTORY_BASELINE_FORMAT) {
    throw new Error('OWNER_INVENTORY_BASELINE_FORMAT_INVALID');
  }
  if (!/^[0-9a-f]{64}$/.test(parsed.contentSha256)
    || contentSha256(withoutChecksum(parsed)) !== parsed.contentSha256) {
    throw new Error('OWNER_INVENTORY_BASELINE_CHECKSUM_INVALID');
  }
  return parsed;
}

export function writeOwnerInventoryBaseline(
  filePath: string,
  baseline: OwnerInventoryBaseline,
): void {
  const descriptor = fs.openSync(filePath, 'wx', 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(baseline, null, 2)}\n`, 'utf8');
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
}

export function validateOwnerInventoryBaseline(
  baseline: OwnerInventoryBaseline,
  current: OwnerInventoryBaseline,
): { ok: boolean; mismatches: string[] } {
  const mismatches: string[] = [];
  const compare = (name: string, expected: unknown, actual: unknown) => {
    if (canonicalJson(expected) !== canonicalJson(actual)) mismatches.push(name);
  };
  compare('counts', baseline.counts, current.counts);
  compare('activeGrants', baseline.activeGrants, current.activeGrants);
  compare('configuredGeneration', baseline.configuredGeneration, current.configuredGeneration);
  compare('aclDesiredGeneration', baseline.aclState.desiredGeneration, current.aclState.desiredGeneration);
  compare('aclRenderedGeneration', baseline.aclState.renderedGeneration, current.aclState.renderedGeneration);
  compare('aclAppliedGeneration', baseline.aclState.appliedGeneration, current.aclState.appliedGeneration);
  compare('aclLastError', baseline.aclState.lastError, current.aclState.lastError);
  compare('aclReadback', baseline.aclGrants, current.aclGrants);
  return { ok: mismatches.length === 0, mismatches };
}
