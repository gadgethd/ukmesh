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
  return rawNodes
    .filter((node) => node && typeof node === 'object' && !isProhibitedMapNode(node))
    .map((node) => ({
      ord: Number(node.ord ?? 0),
      node_id: node.node_id ?? null,
      name: node.name ?? null,
      lat: node.lat ?? null,
      lon: node.lon ?? null,
    }));
}
