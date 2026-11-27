import type { MeshNode } from '../../types/domain.js';
import { isProhibitedMapNode } from '../../utils/pathing.js';

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

function sameGeometry(a: MeshNode | undefined, b: MeshNode | undefined): boolean {
  if (a === b) return true;
  return !!a && !!b && a.node_id === b.node_id && a.lat === b.lat && a.lon === b.lon
    && (a.elevation_m ?? 0) === (b.elevation_m ?? 0)
    && isProhibitedMapNode(a) === isProhibitedMapNode(b);
}

function createNodeSnapshot(getState: () => State, equal: typeof sameMetadata) {
  let input: Map<string, MeshNode> | undefined;
  let snapshot: Map<string, MeshNode>;
  let epoch: number | undefined;
  return () => {
    const next = getState();
    if (next.nodes === input && next.epoch === epoch) return snapshot;
    const changed = next.nodeChanges;
    let equivalent = input !== undefined && epoch === next.epoch && !changed.full && input.size === next.nodes.size;
    if (equivalent && input) {
      const ids = changed.previous === input ? changed.ids : next.nodes.keys();
      for (const id of ids) {
        if (equal(input.get(id), next.nodes.get(id))) continue;
        equivalent = false;
        break;
      }
    }
    input = next.nodes;
    epoch = next.epoch;
    if (!equivalent) snapshot = input;
    return snapshot;
  };
}

/** Name consumers retain their snapshot across timestamp-only updates. */
export function createNodeMetadataSnapshot(getState: () => State) {
  return createNodeSnapshot(getState, sameMetadata);
}

/** Overlays also retain geometry across unrelated metadata changes. The existing
 * last-seen seed for a private point is refreshed when its geometry is rebuilt. */
export function createNodeGeometrySnapshot(getState: () => State) {
  return createNodeSnapshot(getState, sameGeometry);
}
