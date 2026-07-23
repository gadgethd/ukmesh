import { create } from 'zustand';
import type { AggregatedPacket } from '../hooks/useNodes.js';
import type { ClashPathLine, LosProfile, PlannedRepeater } from '../components/Map/types.js';
import type { CustomLosPoint, CustomLosSegment } from '../components/Map/types.js';

export type { PlannedRepeater };

type OverlayStoreState = {
  pinnedPacketId: string | null;
  pinnedPacketSnapshot: AggregatedPacket | null;
  pathNodeIds: Set<string> | null;
  clashPathLines: ClashPathLine[];
  betaPathConfidence: number | null;
  betaPermutationCount: number | null;
  betaRemainingHops: number | null;
  replayNodeIds: Set<string> | null;
  replayTime: string | null;
  pathExplanation: { evidenceLevel: string; summary: string; reasons: string[]; limitations?: string[] } | null;
  // Multi-node LOS support
  losNodeIds: Set<string>;
  losLoadingIds: Set<string>;
  losProfilesByNodeId: Record<string, LosProfile[]>;
  togglePinnedPacket: (packet: AggregatedPacket) => void;
  clearPinnedPacket: () => void;
  setPathNodeIds: (nodeIds: Set<string> | null) => void;
  setClashPathLines: (lines: ClashPathLine[]) => void;
  setBetaMetrics: (metrics: {
    betaPathConfidence: number | null;
    betaPermutationCount: number | null;
    betaRemainingHops: number | null;
  }) => void;
  setReplayBucket: (time: string, nodeIds: string[]) => void;
  clearReplay: () => void;
  setPathExplanation: (explanation: OverlayStoreState['pathExplanation']) => void;
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
  // Planned repeater placement
  planRepeaterMode: boolean;
  plannedRepeaters: PlannedRepeater[];
  requestedPlanCoordinates: Array<{ lat: number; lon: number }>;
  setPlanRepeaterMode: (active: boolean) => void;
  addPlannedRepeater: (repeater: PlannedRepeater) => void;
  updatePlannedRepeater: (id: string, patch: Partial<PlannedRepeater>) => void;
  removePlannedRepeater: (id: string) => void;
  requestPlanRestore: (coordinates: Array<{ lat: number; lon: number }>) => void;
  clearPlanRestoreRequest: () => void;
};

export const useOverlayStore = create<OverlayStoreState>((set) => ({
  pinnedPacketId: null,
  pinnedPacketSnapshot: null,
  pathNodeIds: null,
  clashPathLines: [],
  betaPathConfidence: null,
  betaPermutationCount: null,
  betaRemainingHops: null,
  replayNodeIds: null,
  replayTime: null,
  pathExplanation: null,
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
  setClashPathLines: (clashPathLines) => set({ clashPathLines }),
  setBetaMetrics: (metrics) => set(metrics),
  setReplayBucket: (replayTime, nodeIds) => set({ replayTime, replayNodeIds: new Set(nodeIds.map((id) => id.toLowerCase())) }),
  clearReplay: () => set({ replayTime: null, replayNodeIds: null }),
  setPathExplanation: (pathExplanation) => set({ pathExplanation }),
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
  // Planned repeater placement
  planRepeaterMode: false,
  plannedRepeaters: [],
  requestedPlanCoordinates: [],
  setPlanRepeaterMode: (active) => set({ planRepeaterMode: active }),
  addPlannedRepeater: (repeater) => set((state) => ({
    plannedRepeaters: [...state.plannedRepeaters, repeater],
  })),
  updatePlannedRepeater: (id, patch) => set((state) => ({
    plannedRepeaters: state.plannedRepeaters.map((r) => r.id === id ? { ...r, ...patch } : r),
  })),
  removePlannedRepeater: (id) => set((state) => ({
    plannedRepeaters: state.plannedRepeaters.filter((r) => r.id !== id),
  })),
  requestPlanRestore: (requestedPlanCoordinates) => set({ requestedPlanCoordinates: requestedPlanCoordinates.slice(0, 5) }),
  clearPlanRestoreRequest: () => set({ requestedPlanCoordinates: [] }),
}));
