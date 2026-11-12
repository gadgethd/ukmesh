import { createHash } from 'node:crypto';

export type AppliedOwnerAclArtifact = {
  generation: string;
  rendererVersion: string;
  contentSha256: string;
  content: string;
};

/** Validate the same evidence whether read from SQL or a checksummed baseline. */
export function validAppliedOwnerAclArtifact(artifact: AppliedOwnerAclArtifact, generation: string): boolean {
  if (typeof artifact.content !== 'string' || typeof artifact.rendererVersion !== 'string'
    || artifact.rendererVersion.length === 0) return false;
  const markers = artifact.content.split('\n')
    .filter((line) => line.startsWith('# BEGIN MESHCORE OWNER ACL'));
  return artifact.generation === generation
    && createHash('sha256').update(artifact.content).digest('hex') === artifact.contentSha256
    && markers.length === 1
    && markers[0] === `# BEGIN MESHCORE OWNER ACL v1 generation=${generation}`;
}
