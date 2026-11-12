export type TopologyLinkEndpoints = {
  source: string;
  target: string;
};

export function filterTopologyLinks<T extends TopologyLinkEndpoints>(
  nodeIds: Iterable<string>,
  links: readonly T[],
): T[] {
  const knownNodeIds = new Set(nodeIds);
  return links.filter((link) => (
    knownNodeIds.has(link.source)
    && knownNodeIds.has(link.target)
  ));
}
