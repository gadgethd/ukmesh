type NodeWithId = {
  node_id: string;
};

export type NodePathHashIndex<T extends NodeWithId> = Map<string, T[]>;

export const SUPPORTED_PATH_HASH_HEX_LENGTHS = [2, 4, 6] as const;

export function normalizePathHash(pathHash: string | null | undefined): string {
  return String(pathHash ?? '').trim().toUpperCase();
}

export function nodePathHash(nodeId: string, pathHashOrLength: string | number): string {
  const length = typeof pathHashOrLength === 'number'
    ? pathHashOrLength
    : normalizePathHash(pathHashOrLength).length;
  if (!Number.isFinite(length) || length <= 0) return '';
  return nodeId.slice(0, length).toUpperCase();
}

export function buildNodePathHashIndex<T extends NodeWithId>(
  nodes: Iterable<T>,
  hashLengths: Iterable<number> = SUPPORTED_PATH_HASH_HEX_LENGTHS,
): NodePathHashIndex<T> {
  const index: NodePathHashIndex<T> = new Map();
  const lengths = Array.from(new Set(
    Array.from(hashLengths)
      .map((length) => Number(length))
      .filter((length) => Number.isInteger(length) && length > 0),
  )).sort((a, b) => a - b);

  for (const node of nodes) {
    for (const length of lengths) {
      if (node.node_id.length < length) continue;
      const key = nodePathHash(node.node_id, length);
      const bucket = index.get(key);
      if (bucket) bucket.push(node);
      else index.set(key, [node]);
    }
  }

  return index;
}

export function getNodesForPathHash<T extends NodeWithId>(
  index: NodePathHashIndex<T>,
  pathHash: string | null | undefined,
): T[] {
  return index.get(normalizePathHash(pathHash)) ?? [];
}
