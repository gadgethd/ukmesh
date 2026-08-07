export function extractNeighborNodes(value: unknown): unknown[] | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const nodes = (value as Record<string, unknown>)['nodes'];
  return Array.isArray(nodes) ? nodes : null;
}
