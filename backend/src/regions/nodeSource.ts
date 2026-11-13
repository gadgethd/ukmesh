import { regionMemberIdsSql } from './filter.js';
import { candidateIdentityNodeSourceSql } from '../nodes/identitySource.js';

/** Match core.node_identity_nodes' representative/aggregate semantics on just the
 * region's identities. The global view materializes every node before filtering;
 * this region-first path instead probes nodes_pkey and the alias indexes. */
export function regionNodeSourceSql(regionParam: string): string {
  return candidateIdentityNodeSourceSql(`SELECT member.node_id
    FROM (${regionMemberIdsSql(regionParam)}) AS member(node_id)`);
}
