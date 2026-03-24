import { create } from 'zustand';
import type { AggregatedPacket } from '../hooks/useNodes.js';
import type { LosProfile } from '../components/Map/types.js';
import type { CustomLosPoint, CustomLosSegment } from '../components/Map/types.js';

type OverlayStoreState = {
  pinnedPacketId: string | null;
  pinnedPacketSnapshot: AggregatedPacket | null;
  pathNodeIds: Set<string> | null;
  betaPathConfidence: number | null;
  betaPermutationCount: number | null;
  betaRemainingHops: number | null;
  // Multi-node LOS support
  losNodeIds: Set<string>;
  losLoadingIds: Set<string>;
  losProfilesByNodeId: Record<string, LosProfile[]>;
  togglePinnedPacket: (packet: AggregatedPacket) => void;
  clearPinnedPacket: () => void;
  setPathNodeIds: (nodeIds: Set<string> | null) => void;
  setBetaMetrics: (metrics: {
    betaPathConfidence: number | null;
    betaPermutationCount: number | null;
    betaRemainingHops: number | null;
  }) => void;
  addLosLoading: (nodeId: string) => void;
  setLosProfilesForNode: (nodeId: string, profiles: LosProfile[]) => void;
  removeLosNode: (nodeId: string) => void;
  customLosMode: boolean;
  customLosStart: CustomLosPoint | null;
  customLosSegments: CustomLosSegment[];
  setCustomLosMode: (active: boolean) => void;
  setCustomLosStart: (point: CustomLosPoint | null) => void;
  setCustomLosResult: (segments: CustomLosSegment[]) => void;
  clearCustomLos: () => void;
};

export const useOverlayStore = create<OverlayStoreState>((set) => ({
  pinnedPacketId: null,
  pinnedPacketSnapshot: null,
  pathNodeIds: null,
  betaPathConfidence: null,
  betaPermutationCount: null,
  betaRemainingHops: null,
  losNodeIds: new Set(),
  losLoadingIds: new Set(),
  losProfilesByNodeId: {},
  togglePinnedPacket: (packet) => set((state) => (
    state.pinnedPacketId === packet.id
      ? {
          pinnedPacketId: null,
          pinnedPacketSnapshot: null,
        }
      : {
          pinnedPacketId: packet.id,
          pinnedPacketSnapshot: packet,
        }
  )),
  clearPinnedPacket: () => set({
    pinnedPacketId: null,
    pinnedPacketSnapshot: null,
  }),
  setPathNodeIds: (pathNodeIds) => set({ pathNodeIds }),
  setBetaMetrics: (metrics) => set(metrics),
  addLosLoading: (nodeId) => set((state) => ({
    losNodeIds: new Set([...state.losNodeIds, nodeId]),
    losLoadingIds: new Set([...state.losLoadingIds, nodeId]),
  })),
  setLosProfilesForNode: (nodeId, profiles) => set((state) => {
    const loadingIds = new Set(state.losLoadingIds);
    loadingIds.delete(nodeId);
    return {
      losLoadingIds: loadingIds,
      losProfilesByNodeId: { ...state.losProfilesByNodeId, [nodeId]: profiles },
    };
  }),
  removeLosNode: (nodeId) => set((state) => {
    const nodeIds = new Set(state.losNodeIds);
    const loadingIds = new Set(state.losLoadingIds);
    nodeIds.delete(nodeId);
    loadingIds.delete(nodeId);
    const profilesByNodeId = { ...state.losProfilesByNodeId };
    delete profilesByNodeId[nodeId];
    return { losNodeIds: nodeIds, losLoadingIds: loadingIds, losProfilesByNodeId: profilesByNodeId };
  }),
  customLosMode: false,
  customLosStart: null,
  customLosSegments: [],
  setCustomLosMode: (active) => set({ customLosMode: active }),
  setCustomLosStart: (point) => set({ customLosStart: point }),
  setCustomLosResult: (segments) => set({ customLosSegments: segments }),
  clearCustomLos: () => set({ customLosMode: false, customLosStart: null, customLosSegments: [] }),
}));
