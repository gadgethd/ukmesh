// Nodes with 🚫 in their name have opted out of anonymous public display.
// Public geometry must never be derived from their exact coordinates.

export function isPrivateNode(name: string | null | undefined): boolean {
  return typeof name === 'string' && name.includes('🚫');
}

export type PrivateNodePrefix = {
  prefixSizeBytes: 1 | 2 | 3;
  prefix: string;
};

export function materializePrivateNodePrefixes(nodeId: string): PrivateNodePrefix[] {
  const normalized = nodeId.trim().toUpperCase();
  if (!/^[0-9A-F]{64}$/.test(normalized)) return [];
  return ([1, 2, 3] as const).map((prefixSizeBytes) => ({
    prefixSizeBytes,
    prefix: normalized.slice(0, prefixSizeBytes * 2),
  }));
}

export function redactPrivateNode<T extends {
  node_id: string;
  name?: string | null;
  lat?: number | null;
  lon?: number | null;
  iata?: string | null;
  public_key?: string | null;
}>(node: T): T {
  if (!isPrivateNode(node.name)) return node;
  return {
    // Private rows should be omitted from public collections. This fallback is
    // deliberately allowlisted so future database fields cannot leak by spread.
    node_id:    'private',
    name:       'Private Node',
    iata:       null,
    public_key: null,
    lat:        null,
    lon:        null,
  } as T;
}
