const PROHIBITED_NODE_MARKER = '🚫';

function isProhibitedMapNode(node: { name?: string | null } | null | undefined): boolean {
  return Boolean(node?.name?.includes(PROHIBITED_NODE_MARKER));
}

export function maskDecodedPathNodes(
  rawNodes: Array<{
    ord: number;
    node_id: string | null;
    name: string | null;
    lat: number | null;
    lon: number | null;
    last_seen?: string | null;
  }> | null | undefined,
): Array<{
  ord: number;
  node_id: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
}> {
  if (!Array.isArray(rawNodes)) return [];
  // A hop-preserving placeholder still reveals that a private relay
  // participated and where it appeared. Suppress the path atomically.
  if (rawNodes.some(isProhibitedMapNode)) return [];
  return rawNodes.map((node) => {
    if (!node || typeof node !== 'object') return node;
    return {
      ord: Number(node.ord ?? 0),
      node_id: node.node_id ?? null,
      name: node.name ?? null,
      lat: node.lat ?? null,
      lon: node.lon ?? null,
    };
  });
}
