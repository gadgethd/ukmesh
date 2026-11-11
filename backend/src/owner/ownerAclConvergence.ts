import {
  OWNER_ACL_RENDERER_VERSION,
  validateRenderedOwnerAcl,
  type OwnerAclRenderResult,
} from '../mqtt/aclManager.js';
import type { OwnerAclStateUpdate } from '../db/ownerAuth.js';
import type { OwnerAclGenerationState } from './ownerInventoryBaseline.js';
import type { AppliedOwnerAclArtifact } from './ownerInventoryReadiness.js';
import { isOwnerAclGenerationSettled } from './ownerInventoryReadiness.js';
import type { OwnerAclArtifactInput } from './ownerAclArtifacts.js';

/** A caller must await a pass whose input reads start after its request. Calls
 * during a pass require a trailing pass; bursts before its start coalesce.
 */
export function createOwnerAclReconciliation(pass: () => Promise<void>) {
  let inFlight: Promise<void> | null = null;
  let requested = 0;
  return {
    reconcile(): Promise<void> {
      requested++;
      if (inFlight) return inFlight;
      const run = Promise.resolve().then(async () => {
        try {
          let observed: number;
          do {
            observed = requested;
            await pass();
          } while (observed !== requested);
        } finally {
          // Clear synchronously with the last revision check. A later request
          // must not join an already-drained promise awaiting its finalizer.
          if (inFlight === run) inFlight = null;
        }
      });
      inFlight = run;
      return run;
    },
    async waitForIdle(): Promise<void> { await inFlight; },
  };
}

/** Readiness can request the same startup/periodic pass. Coalesce callers and
 * throttle failed attempts so probes cannot create a broker reload loop.
 */
export function createOwnerAclReadinessRecovery<T extends OwnerAclGenerationState>(input: {
  readState: () => Promise<T>;
  reconcile: () => Promise<void>;
  reportError: (error: unknown) => void;
  now?: () => number;
}): () => Promise<T> {
  const now = input.now ?? Date.now;
  let nextAttemptAt = 0;
  let inFlight: Promise<void> | null = null;
  return async () => {
    const state = await input.readState();
    if (isOwnerAclGenerationSettled(state)) return state;
    if (!inFlight && now() >= nextAttemptAt) {
      nextAttemptAt = now() + 10_000;
      const run = Promise.resolve().then(input.reconcile).catch(input.reportError).finally(() => {
        if (inFlight === run) inFlight = null;
      });
      inFlight = run;
    }
    if (!inFlight) return state;
    await inFlight;
    return input.readState();
  };
}

export type OwnerAclConvergencePorts = {
  readState: () => Promise<OwnerAclGenerationState>;
  loadAppliedArtifact: (generation: string) => Promise<AppliedOwnerAclArtifact | null>;
  updateState: (update: OwnerAclStateUpdate) => Promise<void>;
  saveArtifact: (artifact: OwnerAclArtifactInput) => Promise<void>;
  readAcl: () => string;
  writeAcl: (content: string) => void;
  writeShadow: (content: string) => void;
  reload: () => Promise<void>;
};

/** Converge to the authoritative render, never to a copied baseline generation.
 * The caller serializes passes, including policy loading, across all triggers.
 */
export async function convergeOwnerAcl(
  input: {
    mode: 'shadow' | 'apply';
    currentContent: string;
    rendered: OwnerAclRenderResult;
    allowEmptyCutover: boolean;
  },
  ports: OwnerAclConvergencePorts,
): Promise<'unchanged' | 'recovered' | 'applied' | 'shadow'> {
  const { mode, currentContent, rendered } = input;
  if (mode === 'apply') {
    if (!rendered.validation.ok) throw new Error(`OWNER_ACL_CUTOVER_BLOCKED:${JSON.stringify(rendered.validation)}`);
    if (!rendered.semantic.some((account) => account.nodeIds.length > 0) && !input.allowEmptyCutover) {
      throw new Error('OWNER_ACL_EMPTY_CUTOVER_BLOCKED');
    }
    const state = await ports.readState();
    const applied = await ports.loadAppliedArtifact(rendered.generation);
    // A desired/rendered-only bump is repairable without broker I/O, but only
    // when the recorded applied generation, durable artifact and file agree
    // with a fresh authoritative render. Failed/incomplete applies must retry.
    if (state.appliedGeneration === rendered.generation && state.lastError === null
      && currentContent === rendered.content
      && applied?.generation === rendered.generation
      && applied.rendererVersion === OWNER_ACL_RENDERER_VERSION
      && applied.contentSha256 === rendered.contentSha256
      && applied.content === rendered.content) {
      validateRenderedOwnerAcl(ports.readAcl(), rendered);
      if (state.desiredGeneration === rendered.generation && state.renderedGeneration === rendered.generation) {
        return 'unchanged';
      }
      await ports.updateState({
        desiredGeneration: rendered.generation,
        renderedGeneration: rendered.generation,
        lastError: null,
        verified: true,
      });
      return 'recovered';
    }
  }

  const lastError = mode === 'apply' ? 'OWNER_ACL_APPLY_PENDING'
    : rendered.validation.ok ? null : JSON.stringify(rendered.validation);
  await ports.updateState({ desiredGeneration: rendered.generation, lastError });
  const artifact = {
    generation: rendered.generation,
    rendererVersion: OWNER_ACL_RENDERER_VERSION,
    mode,
    contentSha256: rendered.contentSha256,
    content: rendered.content,
    semantic: rendered.semantic,
    validation: rendered.validation,
  };
  await ports.saveArtifact(artifact);
  await ports.updateState({ renderedGeneration: rendered.generation, lastError });
  ports.writeShadow(rendered.content);
  if (mode === 'shadow') return 'shadow';

  let aclMutated = false;
  try {
    ports.writeAcl(rendered.content);
    aclMutated = true;
    validateRenderedOwnerAcl(ports.readAcl(), rendered);
    await ports.reload();
    validateRenderedOwnerAcl(ports.readAcl(), rendered);
  } catch (error) {
    // Preserve the last file on apply failure. The pending/error fence remains
    // closed even if the broker had already reloaded before readback failed.
    if (aclMutated) ports.writeAcl(currentContent);
    throw error;
  }
  await ports.saveArtifact({ ...artifact, applied: true });
  await ports.updateState({
    desiredGeneration: rendered.generation,
    renderedGeneration: rendered.generation,
    appliedGeneration: rendered.generation,
    lastError: null,
    verified: true,
  });
  return 'applied';
}
