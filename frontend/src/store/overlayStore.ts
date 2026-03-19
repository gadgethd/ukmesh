import { create } from 'zustand';
import type { AggregatedPacket } from '../hooks/useNodes.js';

type OverlayStoreState = {
  pinnedPacketId: string | null;
  pinnedPacketSnapshot: AggregatedPacket | null;
  pathNodeIds: Set<string> | null;
  betaPathConfidence: number | null;
  betaPermutationCount: number | null;
  betaRemainingHops: number | null;
  togglePinnedPacket: (packet: AggregatedPacket) => void;
  clearPinnedPacket: () => void;
  setPathNodeIds: (nodeIds: Set<string> | null) => void;
  setBetaMetrics: (metrics: {
    betaPathConfidence: number | null;
    betaPermutationCount: number | null;
    betaRemainingHops: number | null;
  }) => void;
};

export const useOverlayStore = create<OverlayStoreState>((set) => ({
  pinnedPacketId: null,
  pinnedPacketSnapshot: null,
  pathNodeIds: null,
  betaPathConfidence: null,
  betaPermutationCount: null,
  betaRemainingHops: null,
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
}));
