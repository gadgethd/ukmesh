// Nodes with 🚫 in their name have opted out of anonymous public display.
// Public geometry must never be derived from their exact coordinates.

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
  return {
    // Private rows are omitted from current public collections. Keep this
    // fallback fail-closed for future callers: no stable identifier, activity
    // metadata, or newly added database field crosses the boundary.
    node_id:    'private',
    name:       'Private Node',
    iata:       null,
    public_key: null,
    lat:        null,
    lon:        null,
  } as T;
}
