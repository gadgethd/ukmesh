import { useMemo, useSyncExternalStore } from 'react';
import { nodeStore } from '../features/realtime/nodeStore.js';
import { createNodeMetadataSnapshot } from '../features/realtime/nodeMetadataSnapshot.js';
export { nodeStore } from '../features/realtime/nodeStore.js';

export function useNodeMap() {
  return useSyncExternalStore(nodeStore.subscribe, () => nodeStore.getState().nodes);
}
/** May retain last_seen; only use for names, search and geometry. */
export function useNodeMetadata() {
  const snapshot = useMemo(() => createNodeMetadataSnapshot(nodeStore.getState), []);
  return useSyncExternalStore(nodeStore.subscribe, snapshot);
}
export function usePackets() {
  return useSyncExternalStore(nodeStore.subscribe, () => nodeStore.getState().packets);
}
export function useMessages() {
  return useSyncExternalStore(nodeStore.subscribe, () => nodeStore.getState().messages);
}
export function useArcs() {
  return useSyncExternalStore(nodeStore.subscribe, () => nodeStore.getState().arcs);
}
