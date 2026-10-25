import { ApiInputError } from '../api/errors.js';
import { envFlagEnabled } from '../features.js';

/** Phase A stores bare, case-sensitive member names and #prefixed packet scopes. */
export function parseRegionFilter(
  value: unknown,
  enabled = envFlagEnabled(process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'], false),
): string | undefined {
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value !== 'string') {
    throw new ApiInputError('region must be supplied once', 'AMBIGUOUS_PARAMETER');
  }
  const name = value.trim().replace(/^#/, '');
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(name)) {
    throw new ApiInputError('region must be a 1–64 character public region name');
  }
  if (!enabled) throw new ApiInputError('Region scoping is disabled', 'FEATURE_DISABLED');
  return name;
}

/** Uncorrelated, region-first lookup uses the Phase A (region, node_id) key.
 * Resolve stored aliases to match the canonical node view without scanning packets. */
export function regionMemberIdsSql(regionParam: string): string {
  return `SELECT COALESCE(a.canonical_node_id, m.node_id)
    FROM region_scope_members m
    LEFT JOIN node_identity_aliases a ON a.source_node_id = m.node_id
    WHERE m.region = ${regionParam}`;
}
