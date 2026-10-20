import fs from 'node:fs';
import { query } from '../db/index.js';
import {
  getOwnerAuthorizationSnapshot,
  saveOwnerAclArtifact,
  syncOperatorConfiguredOwnerGrants,
  updateOwnerAclState,
} from '../db/ownerAuth.js';
import {
  OWNER_ACL_RENDERER_VERSION,
  readAclFile,
  reloadMosquitto,
  renderOwnerAcl,
  validateRenderedOwnerAcl,
  writeAclAtomically,
} from '../mqtt/aclManager.js';
import { ownerGrantConfigGeneration, parseOwnerGrantConfig } from './ownerGrantConfig.js';
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

let inFlight: Promise<void> | null = null;
let reconcileTimer: NodeJS.Timeout | null = null;

export function reconcileOwnerAuthorization(): Promise<void> {
  if (inFlight) return inFlight;
  const run = (async () => {
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
    await updateOwnerAclState({ desiredGeneration: rendered.generation });
    await saveOwnerAclArtifact({
      generation: rendered.generation,
      rendererVersion: OWNER_ACL_RENDERER_VERSION,
      mode,
      contentSha256: rendered.contentSha256,
      content: rendered.content,
      semantic: rendered.semantic,
      validation: rendered.validation,
    });
    await updateOwnerAclState({
      renderedGeneration: rendered.generation,
      lastError: rendered.validation.ok ? null : JSON.stringify(rendered.validation),
    });

    const shadowPath = process.env['OWNER_ACL_SHADOW_PATH'] ?? '/tmp/meshcore-owner-acl.shadow';
    fs.writeFileSync(shadowPath, rendered.content, { encoding: 'utf8', mode: 0o600 });
    if (mode === 'shadow') {
      console.log('[owner-acl] shadow policy rendered', {
        generation: rendered.generation,
        valid: rendered.validation.ok,
        ambiguousUsers: rendered.validation.ambiguousUsers.length,
      });
      return;
    }
    if (!rendered.validation.ok) throw new Error(`OWNER_ACL_CUTOVER_BLOCKED:${JSON.stringify(rendered.validation)}`);
    if (!rendered.semantic.some((account) => account.nodeIds.length > 0)
      && String(process.env['OWNER_ACL_ALLOW_EMPTY_CUTOVER'] ?? '') !== '1') {
      throw new Error('OWNER_ACL_EMPTY_CUTOVER_BLOCKED');
    }

    let aclMutated = false;
    try {
      writeAclAtomically(rendered.content);
      aclMutated = true;
      validateRenderedOwnerAcl(readAclFile(), rendered);
      await reloadMosquitto();
      validateRenderedOwnerAcl(readAclFile(), rendered);
    } catch (error) {
      // A failed readback or reload must not leave an unverified policy ready
      // to become active on the broker's next restart.
      if (aclMutated) writeAclAtomically(currentContent);
      throw error;
    }
    await saveOwnerAclArtifact({
      generation: rendered.generation,
      rendererVersion: OWNER_ACL_RENDERER_VERSION,
      mode,
      contentSha256: rendered.contentSha256,
      content: rendered.content,
      semantic: rendered.semantic,
      validation: rendered.validation,
      applied: true,
    });
    await updateOwnerAclState({
      appliedGeneration: rendered.generation,
      lastError: null,
      verified: true,
    });
    console.log('[owner-acl] applied and verified policy', { generation: rendered.generation });
  })().catch(async (error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    await updateOwnerAclState({ lastError: message }).catch(() => undefined);
    throw error;
  }).finally(() => {
    if (inFlight === run) inFlight = null;
  });
  inFlight = run;
  return run;
}

export async function startOwnerAuthorizationReconciler(): Promise<void> {
  const intervalMs = Number(process.env['OWNER_ACL_RECONCILE_INTERVAL_MS'] ?? 60_000);
  const execute = () => {
    void reconcileOwnerAuthorization().catch((error: Error) => {
      console.error('[owner-acl] reconciliation failed:', error.message);
    });
  };
  // The persisted ACL may reflect an older deployment. Reconcile it before
  // the HTTP listener can serve owner-protected data; subsequent refreshes may
  // retry in the background because the startup invariant is already current.
  await reconcileOwnerAuthorization();
  reconcileTimer = setInterval(execute, Math.max(10_000, intervalMs));
  reconcileTimer.unref();
}

export async function stopOwnerAuthorizationReconciler(): Promise<void> {
  if (reconcileTimer) {
    clearInterval(reconcileTimer);
    reconcileTimer = null;
  }
  await inFlight;
}
