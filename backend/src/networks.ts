// Networks that belong to the same logical mesh and should be shown together.
export const UKMESH_NETWORKS = ['ukmesh', 'northeast'] as const;

export function networkMatchesScope(packetNetwork: string | undefined, scopeNetwork: string): boolean {
  if (packetNetwork === scopeNetwork) return true;
  if (scopeNetwork === 'ukmesh' && packetNetwork === 'northeast') return true;
  return false;
}

// Path resolution is a physical-RF question, not a display question. The UK
// mesh is one contiguous network split into display labels (ukmesh / northeast /
// teesside) mostly by which site's observers ingested a packet, and since
// nodes.network is last-writer-wins, boundary relays flip between labels. A
// packet that arrived under ANY of these labels is routinely relayed by nodes
// labelled with the others — e.g. teesside-received multibyte packets resolve
// their relays to ukmesh/northeast nodes ~99% of the time (teesside has only
// ~140 nodes). The resolver must therefore search the full combined node set.
// This is intentionally BROADER than the website's display scope
// (UKMESH_NETWORKS = ukmesh+northeast, with teesside shown as its own site).
export const MESH_RESOLVER_NETWORKS = ['ukmesh', 'northeast', 'teesside'] as const;

/**
 * Expand a network into the node-search scope used by the path-lazy resolvers
 * and accuracy harness. Any member of the UK physical mesh expands to the full
 * mesh; anything else (e.g. 'test') maps to itself.
 */
export function expandResolverScope(network: string): string[] {
  return (MESH_RESOLVER_NETWORKS as readonly string[]).includes(network)
    ? [...MESH_RESOLVER_NETWORKS]
    : [network];
}
