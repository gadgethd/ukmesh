export function extractNeighborNodes(value: unknown): unknown[] | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const record = value as Record<string, unknown>;
  // Official MeshCore firmware publishes the UK spelling "neighbours" (matching
  // its /neighbours topic suffix); our MQTT fork publishes "nodes". Accept both.
  const nodes = record['nodes'] ?? record['neighbours'];
  return Array.isArray(nodes) ? nodes : null;
}
