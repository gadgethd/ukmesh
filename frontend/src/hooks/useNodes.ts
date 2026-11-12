import { useSyncExternalStore } from 'react';
import { nodeStore } from '../features/realtime/nodeStore.js';
export { nodeStore } from '../features/realtime/nodeStore.js';

export function useNodeMap() {
  return useSyncExternalStore(nodeStore.subscribe, () => nodeStore.getState().nodes);
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
