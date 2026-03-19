import { useCallback, useRef, useEffect } from 'react';
import type { WSMessage } from './useWebSocket.js';
import type { LivePacketData, MeshNode } from './useNodes.js';
import type { ViableLinkSnapshot } from './useLinkState.js';



type PendingPacket = LivePacketData;
type PendingNodeUpdate = { nodeId: string; ts: number };
type PendingNodeUpsert = Partial<MeshNode> & { node_id: string };
type PendingLinkUpdate = {
  node_a_id: string;
  node_b_id: string;
  observed_count: number;
  itm_viable: boolean | null;
  itm_path_loss_db?: number | null;
  count_a_to_b?: number;
  count_b_to_a?: number;
};
type PendingCoverageUpdate = {
  node_id: string;
  geom: { type: string; coordinates: unknown };
  strength_geoms?: Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
};

interface PendingBatches {
  packets: PendingPacket[];
  nodeUpdates: PendingNodeUpdate[];
  nodeUpserts: PendingNodeUpsert[];
  linkUpdates: PendingLinkUpdate[];
  coverageUpdates: PendingCoverageUpdate[];
  packetObserved: boolean;
}

type UseAppMessageHandlerParams = {
  handleInitialState: (data: {
    nodes: MeshNode[];
    packets: Array<{
      time: string;
      packet_hash: string;
      rx_node_id?: string;
      src_node_id?: string;
      packet_type?: number;
      hop_count?: number;
      path_hash_size_bytes?: number;
      summary?: string | null;
      payload?: Record<string, unknown>;
      advert_count?: number | null;
      path_hashes?: string[] | null;
    }>;
  }) => void;
  handlePacket: (data: LivePacketData | LivePacketData[]) => void;
  handleNodeUpdate: (data: { nodeId: string; ts: number }) => void;
  handleNodeUpdateBatch?: (data: { nodeId: string; ts: number }[]) => void;
  handleNodeUpsert: (data: Partial<MeshNode> & { node_id: string }) => void;
  handleNodeUpsertBatch?: (data: (Partial<MeshNode> & { node_id: string })[]) => void;
  handleCoverageUpdate: (data: {
    node_id: string;
    geom: { type: string; coordinates: unknown };
    strength_geoms?: Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
  }) => void;
  handleCoverageUpdateBatch?: (data: {
    node_id: string;
    geom: { type: string; coordinates: unknown };
    strength_geoms?: Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
  }[]) => void;
  applyInitialViablePairs: (pairs?: [string, string][]) => void;
  applyInitialViableLinks: (links?: ViableLinkSnapshot[]) => void;
  applyLinkUpdate: (update: {
    node_a_id: string;
    node_b_id: string;
    observed_count: number;
    itm_viable: boolean | null;
    itm_path_loss_db?: number | null;
    count_a_to_b?: number;
    count_b_to_a?: number;
  }) => void;
  applyLinkUpdateBatch?: (updates: {
    node_a_id: string;
    node_b_id: string;
    observed_count: number;
    itm_viable: boolean | null;
    itm_path_loss_db?: number | null;
    count_a_to_b?: number;
    count_b_to_a?: number;
  }[]) => void;
  onPacketObserved?: () => void;
};

export function useAppMessageHandler({
  handleInitialState,
  handlePacket,
  handleNodeUpdate,
  handleNodeUpdateBatch,
  handleNodeUpsert,
  handleNodeUpsertBatch,
  handleCoverageUpdate,
  handleCoverageUpdateBatch,
  applyInitialViablePairs,
  applyInitialViableLinks,
  applyLinkUpdate,
  applyLinkUpdateBatch,
  onPacketObserved,
}: UseAppMessageHandlerParams) {
  const pendingRef = useRef<PendingBatches>({
    packets: [],
    nodeUpdates: [],
    nodeUpserts: [],
    linkUpdates: [],
    coverageUpdates: [],
    packetObserved: false,
  });
  const rafRef = useRef<number | null>(null);
  const flushRef = useRef<() => void>(() => {});

  // Flush pending updates - runs at most once per animation frame
  const flushPending = useCallback(() => {
    const pending = pendingRef.current;
    if (!pending.packets.length && !pending.nodeUpdates.length && 
        !pending.nodeUpserts.length && !pending.linkUpdates.length &&
        !pending.coverageUpdates.length && !pending.packetObserved) {
      return;
    }

    // Flush ALL packets at once - single state update for entire batch
    if (pending.packets.length > 0) {
      handlePacket(pending.packets);
    }

    // Flush node updates (dedupe by nodeId, keep latest)
    const latestNodeUpdates = new Map<string, PendingNodeUpdate>();
    for (const update of pending.nodeUpdates) {
      latestNodeUpdates.set(update.nodeId, update);
    }
    if (latestNodeUpdates.size > 0) {
      const arr = Array.from(latestNodeUpdates.values());
      if (handleNodeUpdateBatch) {
        handleNodeUpdateBatch(arr);
      } else {
        arr.forEach(handleNodeUpdate);
      }
    }

    // Flush node upserts (dedupe by node_id, keep latest)
    const latestNodeUpserts = new Map<string, PendingNodeUpsert>();
    for (const upsert of pending.nodeUpserts) {
      latestNodeUpserts.set(upsert.node_id, upsert);
    }
    if (latestNodeUpserts.size > 0) {
      const arr = Array.from(latestNodeUpserts.values());
      if (handleNodeUpsertBatch) {
        handleNodeUpsertBatch(arr);
      } else {
        arr.forEach(handleNodeUpsert);
      }
    }

    // Flush link updates
    if (pending.linkUpdates.length > 0) {
      if (applyLinkUpdateBatch) {
        applyLinkUpdateBatch(pending.linkUpdates);
      } else {
        pending.linkUpdates.forEach(applyLinkUpdate);
      }
    }

    // Flush coverage updates (dedupe by node_id, keep latest)
    const latestCoverage = new Map<string, PendingCoverageUpdate>();
    for (const update of pending.coverageUpdates) {
      latestCoverage.set(update.node_id, update);
    }
    if (latestCoverage.size > 0) {
      const arr = Array.from(latestCoverage.values());
      if (handleCoverageUpdateBatch) {
        handleCoverageUpdateBatch(arr);
      } else {
        arr.forEach(handleCoverageUpdate);
      }
    }

    // Fire packet observed event once if any packets were processed
    if (pending.packetObserved || pending.packets.length > 0) {
      onPacketObserved?.();
    }

    // Reset pending
    pendingRef.current = {
      packets: [],
      nodeUpdates: [],
      nodeUpserts: [],
      linkUpdates: [],
      coverageUpdates: [],
      packetObserved: false,
    };
    rafRef.current = null;
  }, [handlePacket, handleNodeUpdate, handleNodeUpsert, handleCoverageUpdate, applyLinkUpdate, onPacketObserved]);

  // Throttle flush — batches bursts from the WebSocket into single React renders
  const BATCH_INTERVAL_MS = 16;
  const scheduleFlush = useCallback(() => {
    if (rafRef.current !== null) return;
    rafRef.current = window.setTimeout(() => {
      rafRef.current = null;
      flushPending();
    }, BATCH_INTERVAL_MS);
  }, [flushPending]);

  // Cleanup pending flush on unmount
  useEffect(() => {
    flushRef.current = flushPending;
    return () => {
      if (rafRef.current !== null) {
        clearTimeout(rafRef.current);
      }
      // Flush any remaining pending updates
      flushPending();
    };
  }, [flushPending]);

  return useCallback((msg: WSMessage) => {
    if (msg.type === 'initial_state') {
      const data = msg.data as Parameters<typeof handleInitialState>[0] & {
        viable_pairs?: [string, string][];
        viable_links?: ViableLinkSnapshot[];
      };
      handleInitialState(data);
      if (data.viable_links && data.viable_links.length > 0) {
        applyInitialViableLinks(data.viable_links);
      } else {
        applyInitialViablePairs(data.viable_pairs);
      }
      return;
    }

    const pending = pendingRef.current;

    if (msg.type === 'packet') {
      const packet = msg.data as LivePacketData;
      pending.packets.push(packet);
      pending.packetObserved = true;
      scheduleFlush();
      return;
    }

    if (msg.type === 'node_update') {
      pending.nodeUpdates.push(msg.data as PendingNodeUpdate);
      scheduleFlush();
      return;
    }

    if (msg.type === 'node_upsert') {
      pending.nodeUpserts.push(msg.data as PendingNodeUpsert);
      scheduleFlush();
      return;
    }

    if (msg.type === 'coverage_update') {
      pending.coverageUpdates.push(msg.data as PendingCoverageUpdate);
      scheduleFlush();
      return;
    }

    if (msg.type === 'link_update') {
      pending.linkUpdates.push(msg.data as PendingLinkUpdate);
      scheduleFlush();
    }
  }, [
    handleInitialState,
    applyInitialViablePairs,
    applyInitialViableLinks,
    scheduleFlush,
  ]);
}
