import type { MeshNode } from '../../types/domain.js';

type State = {
  epoch: number;
  nodes: Map<string, MeshNode>;
  nodeChanges: { full: boolean; ids: readonly string[]; previous: Map<string, MeshNode> | null };
};

function sameMetadata(a: MeshNode | undefined, b: MeshNode | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  const keys = (node: MeshNode) => Object.keys(node).filter(key => key !== 'last_seen') as (keyof MeshNode)[];
  const fields = keys(a);
  return fields.length === keys(b).length && fields.every(key => Object.is(a[key], b[key]));
}

/** Name/geometry consumers retain their snapshot across timestamp-only updates.
 * Presence and expiry consumers must use the live node map instead. */
export function createNodeMetadataSnapshot(getState: () => State) {
  let input: Map<string, MeshNode> | undefined;
  let snapshot: Map<string, MeshNode>;
  let epoch: number | undefined;
  return () => {
    const next = getState();
    if (next.nodes === input && next.epoch === epoch) return snapshot;
    const changed = next.nodeChanges;
    const ids = !changed.full && changed.previous === input ? changed.ids : next.nodes.keys();
    const equivalent = input && epoch === next.epoch && !changed.full && input.size === next.nodes.size
      && Array.from(ids).every(id => sameMetadata(input!.get(id), next.nodes.get(id)));
    input = next.nodes;
    epoch = next.epoch;
    if (!equivalent) snapshot = input;
    return snapshot;
  };
}
