import { createHash } from 'node:crypto';
import { parseAcl } from '../mqtt/aclManager.js';
import {
  validateOwnerInventoryBaseline,
  type OwnerAclGenerationState,
  type OwnerInventoryBaseline,
} from './ownerInventoryBaseline.js';

export type AppliedOwnerAclArtifact = {
  generation: string;
  rendererVersion: string;
  contentSha256: string;
  content: string;
};

const PUBLIC_FEED_READ = 'topic read public/+/+/packets';
const GENERATION_MISMATCHES = new Set([
  'aclDesiredGeneration', 'aclRenderedGeneration', 'aclAppliedGeneration',
]);

/** Compare all rendered directives in their user scope, including unmanaged users. */
export function classifyRenderedOwnerAclDelta(before: string, after: string): {
  kind: 'empty' | 'public-feed-only' | 'other';
  reason: string;
} {
  const previous = parseAcl(before);
  const current = parseAcl(after);
  for (const parsed of [previous, current]) {
    if (parsed.errors.length || parsed.duplicateUsers.length
      || parsed.stanzas.some((stanza) =>
        stanza.directives.some((directive) => directive.classification === 'malformed'))) {
      return { kind: 'other', reason: 'rendered-acl-invalid' };
    }
  }
  const policyLines = (lines: string[]) => lines.map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'));
  if (JSON.stringify(policyLines(previous.preamble)) !== JSON.stringify(policyLines(current.preamble))) {
    return { kind: 'other', reason: 'non-feed-global-directive-changed' };
  }
  const previousUsers = new Map(previous.stanzas.map((stanza) => [stanza.username, stanza]));
  const currentUsers = new Map(current.stanzas.map((stanza) => [stanza.username, stanza]));
  let feedChanged = false;
  for (const username of new Set([...previousUsers.keys(), ...currentUsers.keys()])) {
    const oldStanza = previousUsers.get(username);
    const newStanza = currentUsers.get(username);
    const oldLines = oldStanza?.directives.map((directive) => directive.line) ?? [];
    const newLines = newStanza?.directives.map((directive) => directive.line) ?? [];
    const nonFeed = (lines: string[]) => lines.filter((line) => line !== PUBLIC_FEED_READ);
    if (JSON.stringify(nonFeed(oldLines)) !== JSON.stringify(nonFeed(newLines))) {
      return { kind: 'other', reason: 'non-feed-user-directive-changed' };
    }
    // A new/removed stanza is sanctioned only when it contains feed reads alone.
    if ((!oldStanza || !newStanza) && oldLines.length + newLines.length === 0) {
      return { kind: 'other', reason: 'non-feed-user-stanza-changed' };
    }
    if (oldLines.filter((line) => line === PUBLIC_FEED_READ).length
      !== newLines.filter((line) => line === PUBLIC_FEED_READ).length) feedChanged = true;
  }
  return feedChanged
    ? { kind: 'public-feed-only', reason: 'only-public-feed-read-grants-changed' }
    : { kind: 'empty', reason: 'rendered-acl-policy-unchanged' };
}

function settled(state: OwnerAclGenerationState): boolean {
  return Boolean(state.desiredGeneration)
    && state.desiredGeneration === state.renderedGeneration
    && state.desiredGeneration === state.appliedGeneration
    && state.lastError === null;
}

function validArtifact(artifact: AppliedOwnerAclArtifact, generation: string): boolean {
  const markers = artifact.content.split('\n')
    .filter((line) => line.startsWith('# BEGIN MESHCORE OWNER ACL'));
  return artifact.generation === generation
    && createHash('sha256').update(artifact.content).digest('hex') === artifact.contentSha256
    && markers.length === 1
    && markers[0] === `# BEGIN MESHCORE OWNER ACL v1 generation=${generation}`;
}

/** Readiness alone may tolerate proven feed-only drift; inventory exports stay strict. */
export async function validateOwnerInventoryReadiness(
  baseline: OwnerInventoryBaseline,
  current: OwnerInventoryBaseline,
  input: {
    aclContent: string;
    loadAppliedArtifact: (generation: string) => Promise<AppliedOwnerAclArtifact | null>;
  },
): Promise<{ ok: boolean; mismatches: string[]; reason: string }> {
  const validation = validateOwnerInventoryBaseline(baseline, current);
  const reject = (reason: string) => ({ ...validation, reason });
  if (validation.ok) return { ...validation, reason: 'owner-baseline-current' };
  if (validation.mismatches.some((name) => !GENERATION_MISMATCHES.has(name))) {
    return reject('owner-inventory-scope-changed');
  }
  if (!settled(baseline.aclState) || !settled(current.aclState)) {
    return reject('owner-acl-generation-not-settled');
  }
  const previousGeneration = baseline.aclState.appliedGeneration!;
  const currentGeneration = current.aclState.appliedGeneration!;
  let artifacts: Array<AppliedOwnerAclArtifact | null>;
  try {
    artifacts = await Promise.all([
      input.loadAppliedArtifact(previousGeneration),
      input.loadAppliedArtifact(currentGeneration),
    ]);
  } catch {
    return reject('applied-acl-artifact-unavailable');
  }
  const [previous, latest] = artifacts;
  if (!previous || !latest) return reject('applied-acl-artifact-missing');
  if (!validArtifact(previous, previousGeneration) || !validArtifact(latest, currentGeneration)) {
    return reject('applied-acl-artifact-invalid');
  }
  if (previous.rendererVersion !== latest.rendererVersion) {
    return reject('owner-acl-renderer-changed');
  }
  if (latest.content !== input.aclContent) return reject('applied-acl-readback-mismatch');
  const delta = classifyRenderedOwnerAclDelta(previous.content, latest.content);
  if (delta.kind !== 'public-feed-only') return reject(delta.reason);
  return { ok: true, mismatches: [], reason: delta.reason };
}
