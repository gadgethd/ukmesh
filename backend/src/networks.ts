// UKMesh is the single production scope, but historical rows still carry the
// pre-unification labels until the large relabel migration is explicitly run.
// Keep those rows in scope so an application deployment cannot make existing
// packet history, stats, or resolver evidence disappear. Test traffic remains
// deliberately separate.
export const UKMESH_NETWORKS = ['ukmesh', 'northeast', 'teesside'] as const;
const UKMESH_NETWORK_SET = new Set<string>(UKMESH_NETWORKS);

export function networkMatchesScope(packetNetwork: string | undefined, scopeNetwork: string): boolean {
  return scopeNetwork === 'ukmesh'
    ? UKMESH_NETWORK_SET.has(packetNetwork ?? '')
    : packetNetwork === scopeNetwork;
}

// Path resolution uses the same production compatibility scope. New writes use
// `ukmesh`; this only preserves the evidence stored before the cutover.
export const MESH_RESOLVER_NETWORKS = UKMESH_NETWORKS;

/**
 * Expand a network into the node-search scope used by the path-lazy resolvers
 * and accuracy harness. The public UKMesh scope includes historical production
 * labels, while isolated test traffic resolves only against itself.
 */
export function expandResolverScope(network: string): string[] {
  return network === 'ukmesh'
    ? [...MESH_RESOLVER_NETWORKS]
    : [network];
}
