import fs from 'node:fs';
import { query } from '../db/runtime.js';
import {
  getAppliedOwnerAclArtifact,
  getOwnerAclReadiness,
  getOwnerAuthorizationSnapshot,
  initOwnerAuthDb,
  saveOwnerAclArtifact,
  syncOperatorConfiguredOwnerGrants,
  updateOwnerAclState,
} from './ownerAuth.js';
import {
  readAclFile,
  reloadMosquitto,
  renderOwnerAcl,
  writeAclAtomically,
} from '../mqtt/aclManager.js';
import { ownerGrantConfigGeneration, parseOwnerGrantConfig } from './ownerGrantConfig.js';
import { convergeOwnerAcl, createOwnerAclReadinessRecovery, createOwnerAclReconciliation } from './ownerAclConvergence.js';
import { createOwnerAclRuntime } from './ownerAclRuntime.js';
import { getPublicFeedConfig } from './publicFeedConfig.js';
import {
  getActivePublicFeedReaderUsernames,
} from './publicFeedRepository.js';

export type OwnerAclMode = 'shadow' | 'apply';

function ownerAclMode(): OwnerAclMode {
  const value = String(process.env['OWNER_ACL_MODE'] ?? 'apply').trim().toLowerCase();
  if (value !== 'shadow' && value !== 'apply') throw new Error(`INVALID_OWNER_ACL_MODE:${value}`);
  return value;
}

function unmanagedAclUsers(): string[] {
  return String(process.env['OWNER_ACL_UNMANAGED_USERS'] ?? 'backend,test,test2')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

function allowedEmptyAclUsers(): string[] {
  return String(process.env['OWNER_ACL_ALLOW_EMPTY_USERS'] ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

function configuredPublicFeedReaders(): string[] {
  return String(process.env['OWNER_ACL_PUBLIC_FEED_READ_USERS'] ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

function systemReadwriteAclTopics(): string[] {
  return String(process.env['OWNER_ACL_SYSTEM_READWRITE_TOPICS'] ?? 'public/#')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

const reconciliation = createOwnerAclReconciliation(reconcileOwnerAuthorizationPass);
const runtime = createOwnerAclRuntime({
  initialize: initOwnerAuthDb,
  reconcile: reconcileOwnerAuthorization,
  refreshIntervalMs: () => {
    const interval = Number(process.env['OWNER_ACL_RECONCILE_INTERVAL_MS'] ?? 60_000);
    return Number.isFinite(interval) ? Math.max(10_000, Math.min(interval, 2_147_483_647)) : 60_000;
  },
  reportError: (error) => console.error('[owner-acl] reconciliation failed:', error instanceof Error ? error.message : String(error)),
});

const recoverReadiness = createOwnerAclReadinessRecovery({
  readState: getOwnerAclReadiness,
  reconcile: reconcileOwnerAuthorization,
  reportError: (error) => console.error('[owner-acl] readiness recovery failed:', error instanceof Error ? error.message : String(error)),
});

export function getConvergedOwnerAclReadiness(): ReturnType<typeof getOwnerAclReadiness> {
  return ownerAclMode() === 'apply' ? recoverReadiness() : getOwnerAclReadiness();
}

export function reconcileOwnerAuthorization(): Promise<void> {
  return reconciliation.reconcile();
}

function reconcileOwnerAuthorizationPass(): Promise<void> {
  return (async () => {
    const mode = ownerAclMode();
    const configured = parseOwnerGrantConfig(String(process.env['OWNER_MQTT_USERNAME_MAP'] ?? ''));
    const configGeneration = ownerGrantConfigGeneration(configured);
    await syncOperatorConfiguredOwnerGrants(configured, configGeneration);

    const snapshot = await getOwnerAuthorizationSnapshot();
    const publicFeedConfig = getPublicFeedConfig();
    const publicFeedReaders = publicFeedConfig.configured
      ? await getActivePublicFeedReaderUsernames(query)
      : new Set<string>();
    const explicitPublicFeedReaders = configuredPublicFeedReaders();
    const explicitPublicFeedReaderSet = new Set(explicitPublicFeedReaders);
    const grants = snapshot.map((account) => {
      const publicFeedRead = explicitPublicFeedReaderSet.has(account.mqttUsername)
        || (account.isActive && publicFeedReaders.has(account.mqttUsername));
      return {
        mqttUsername: account.mqttUsername,
        nodeIds: account.isActive ? account.nodeIds : [],
        ...(publicFeedRead ? { publicFeedRead: true } : {}),
      };
    });
    const snapshotUsernames = new Set(grants.map((grant) => grant.mqttUsername));
    for (const mqttUsername of explicitPublicFeedReaders) {
      if (snapshotUsernames.has(mqttUsername)) continue;
      // An explicit subscriber is managed even when the public feed is
      // temporarily empty or disabled. That removes any stale broad ACL and
      // leaves the account read-only with no topic access until a node is
      // explicitly opted in and active.
      grants.push({
        mqttUsername,
        nodeIds: [],
        publicFeedRead: true,
      });
    }
    const currentContent = readAclFile();
    const rendered = renderOwnerAcl(
      currentContent,
      grants,
      unmanagedAclUsers(),
      [...allowedEmptyAclUsers(), ...explicitPublicFeedReaders],
      [String(process.env['MQTT_USERNAME'] ?? 'backend').trim() || 'backend'],
      systemReadwriteAclTopics(),
    );
    const outcome = await convergeOwnerAcl({
      mode, currentContent, rendered,
      allowEmptyCutover: String(process.env['OWNER_ACL_ALLOW_EMPTY_CUTOVER'] ?? '') === '1',
    }, {
      readState: getOwnerAclReadiness,
      loadAppliedArtifact: getAppliedOwnerAclArtifact,
      updateState: updateOwnerAclState,
      saveArtifact: saveOwnerAclArtifact,
      readAcl: readAclFile,
      writeAcl: writeAclAtomically,
      writeShadow: (content) => fs.writeFileSync(
        process.env['OWNER_ACL_SHADOW_PATH'] ?? '/tmp/meshcore-owner-acl.shadow',
        content, { encoding: 'utf8', mode: 0o600 },
      ),
      reload: reloadMosquitto,
    });
    console.log('[owner-acl] convergence', { generation: rendered.generation, outcome });
  })().catch(async (error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    await updateOwnerAclState({ lastError: message }).catch(() => undefined);
    throw error;
  });
}

export function startOwnerAuthorizationReconciler(onReady: () => void): void {
  runtime.start(onReady);
}

export function ownerAuthorizationAvailable(): boolean {
  return runtime.isAvailable();
}

export async function stopOwnerAuthorizationReconciler(): Promise<void> {
  await runtime.stop();
  await reconciliation.waitForIdle();
}
