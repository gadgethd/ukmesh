// Nodes with 🚫 in their name have opted out of public display.
// Their name is replaced with "Private Node", coordinates are fuzzed
// deterministically (~500 m), and identifying fields are cleared.

function deterministicFuzz(nodeId: string): { dlat: number; dlon: number } {
  const a = parseInt(nodeId.slice(0, 6), 16);
  const b = parseInt(nodeId.slice(6, 12), 16);
  return {
    dlat: ((a % 1000) - 500) / 100000,
    dlon: ((b % 1000) - 500) / 100000,
  };
}

export function isPrivateNode(name: string | null | undefined): boolean {
  return typeof name === 'string' && name.includes('🚫');
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
  const { dlat, dlon } = deterministicFuzz(node.node_id);
  return {
    ...node,
    name:       'Private Node',
    iata:       null,
    public_key: null,
    lat:  node.lat != null ? node.lat  + dlat : node.lat,
    lon:  node.lon != null ? node.lon  + dlon : node.lon,
  };
}
