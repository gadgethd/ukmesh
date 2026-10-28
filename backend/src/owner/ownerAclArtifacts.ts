import type { QueryResultRow } from 'pg';
import type { AppliedOwnerAclArtifact } from './ownerInventoryReadiness.js';

export type OwnerAclArtifactInput = {
  generation: string;
  rendererVersion: string;
  mode: 'shadow' | 'apply';
  contentSha256: string;
  content: string;
  semantic: unknown;
  validation: unknown;
  applied?: boolean;
};

type ArtifactQuery = <T extends QueryResultRow>(sql: string, params: unknown[]) => Promise<{ rows: T[] }>;

/** An applied certificate belongs to exact validated bytes and renderer/mode,
 * not merely to the artifact key. Never transfer it to an unverified rewrite.
 */
export function createOwnerAclArtifactStore(query: ArtifactQuery) {
  return {
    async getApplied(generation: string): Promise<AppliedOwnerAclArtifact | null> {
      const result = await query<{
        generation: string; renderer_version: string; content_sha256: string; content: string;
      }>(
        `SELECT generation, renderer_version, content_sha256, content
           FROM owner_acl_artifacts
          WHERE generation = $1 AND mode = 'apply' AND applied_at IS NOT NULL
            AND validation_json->>'ok' = 'true'`,
        [generation],
      );
      const row = result.rows[0];
      return row ? {
        generation: row.generation, rendererVersion: row.renderer_version,
        contentSha256: row.content_sha256, content: row.content,
      } : null;
    },
    async save(input: OwnerAclArtifactInput): Promise<void> {
      await query(
        `INSERT INTO owner_acl_artifacts (
           generation, renderer_version, mode, content_sha256, content,
           semantic_json, validation_json, applied_at
         ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)
         ON CONFLICT (generation) DO UPDATE SET
           renderer_version = EXCLUDED.renderer_version,
           mode = EXCLUDED.mode,
           content_sha256 = EXCLUDED.content_sha256,
           content = EXCLUDED.content,
           semantic_json = EXCLUDED.semantic_json,
           validation_json = EXCLUDED.validation_json,
           applied_at = CASE
             WHEN EXCLUDED.applied_at IS NOT NULL THEN EXCLUDED.applied_at
             WHEN owner_acl_artifacts.mode = 'apply' AND EXCLUDED.mode = 'apply'
               AND owner_acl_artifacts.renderer_version = EXCLUDED.renderer_version
               AND owner_acl_artifacts.content_sha256 = EXCLUDED.content_sha256
               AND owner_acl_artifacts.content = EXCLUDED.content
               AND owner_acl_artifacts.validation_json->>'ok' = 'true'
               AND EXCLUDED.validation_json->>'ok' = 'true'
             THEN owner_acl_artifacts.applied_at
             ELSE NULL END`,
        [
          input.generation, input.rendererVersion, input.mode, input.contentSha256, input.content,
          JSON.stringify(input.semantic), JSON.stringify(input.validation), input.applied ? new Date() : null,
        ],
      );
    },
  };
}
