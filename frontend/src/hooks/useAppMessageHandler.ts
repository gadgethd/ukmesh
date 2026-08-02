import { useCallback, useEffect, useRef } from 'react';
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
interface PendingBatches {
  packets: PendingPacket[];
  nodeUpdates: PendingNodeUpdate[];
  nodeUpserts: PendingNodeUpsert[];
  linkUpdates: PendingLinkUpdate[];
}

type InitialState = {
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
  viable_pairs?: [string, string][];
  viable_links?: ViableLinkSnapshot[];
};

export type RealtimeMessageActions = {
  handleInitialState: (data: InitialState) => void;
  handlePacket: (data: LivePacketData | LivePacketData[]) => void;
  handleNodeUpdate: (data: PendingNodeUpdate) => void;
  handleNodeUpdateBatch?: (data: PendingNodeUpdate[]) => void;
  handleNodeUpsert: (data: PendingNodeUpsert) => void;
  handleNodeUpsertBatch?: (data: PendingNodeUpsert[]) => void;
  applyInitialViablePairs: (pairs?: [string, string][]) => void;
  applyInitialViableLinks: (links?: ViableLinkSnapshot[]) => void;
  applyLinkUpdate: (update: PendingLinkUpdate) => void;
  applyLinkUpdateBatch?: (updates: PendingLinkUpdate[]) => void;
  onPacketObserved?: (count: number) => void;
};

function emptyPending(): PendingBatches {
  return {
    packets: [],
    nodeUpdates: [],
    nodeUpserts: [],
    linkUpdates: [],
  };
}

/**
 * Buffers live events until the connection's authoritative initial snapshot is
 * applied. That makes "live then snapshot" converge with "snapshot then live"
 * while retaining animation-frame batching after initialization.
 */
export function createRealtimeMessageCoordinator(
  getActions: () => RealtimeMessageActions,
  scheduleFlush: () => void = () => {},
  cancelScheduledFlush: () => void = () => {},
) {
  let pending = emptyPending();
  let snapshotReceived = false;

  const flush = (): void => {
    if (!snapshotReceived) return;
    const batch = pending;
    pending = emptyPending();
    const actions = getActions();

    // Resolve/update endpoints before packets so packet arcs can only be built
    // from the current authoritative, privacy-safe node map.
    const latestNodeUpdates = new Map<string, PendingNodeUpdate>();
    for (const update of batch.nodeUpdates) {
      const existing = latestNodeUpdates.get(update.nodeId);
      if (!existing || update.ts >= existing.ts) latestNodeUpdates.set(update.nodeId, update);
    }
    const nodeUpdates = Array.from(latestNodeUpdates.values());
    if (nodeUpdates.length > 0) {
      if (actions.handleNodeUpdateBatch) actions.handleNodeUpdateBatch(nodeUpdates);
      else nodeUpdates.forEach(actions.handleNodeUpdate);
    }

    const latestNodeUpserts = new Map<string, PendingNodeUpsert>();
    for (const upsert of batch.nodeUpserts) latestNodeUpserts.set(upsert.node_id, upsert);
    const nodeUpserts = Array.from(latestNodeUpserts.values());
    if (nodeUpserts.length > 0) {
      if (actions.handleNodeUpsertBatch) actions.handleNodeUpsertBatch(nodeUpserts);
      else nodeUpserts.forEach(actions.handleNodeUpsert);
    }

    if (batch.packets.length > 0) actions.handlePacket(batch.packets);

    if (batch.linkUpdates.length > 0) {
      if (actions.applyLinkUpdateBatch) actions.applyLinkUpdateBatch(batch.linkUpdates);
      else batch.linkUpdates.forEach(actions.applyLinkUpdate);
    }

    if (batch.packets.length > 0) actions.onPacketObserved?.(batch.packets.length);
  };

  const handle = (msg: WSMessage): void => {
    if (msg.type === 'initial_state') {
      cancelScheduledFlush();
      const data = msg.data as InitialState;
      const actions = getActions();
      actions.handleInitialState(data);
      if (Object.prototype.hasOwnProperty.call(data, 'viable_links')) {
        actions.applyInitialViableLinks(data.viable_links ?? []);
      } else {
        actions.applyInitialViablePairs(data.viable_pairs ?? []);
      }
      snapshotReceived = true;
      flush();
      return;
    }

    if (msg.type === 'packet') {
      const packet = msg.data as PendingPacket;
      pending.packets.push(packet);
      if (snapshotReceived && packet.packetType === 5) {
        cancelScheduledFlush();
        flush();
      } else if (snapshotReceived) {
        scheduleFlush();
      }
      return;
    }
    if (msg.type === 'node_update') pending.nodeUpdates.push(msg.data as PendingNodeUpdate);
    else if (msg.type === 'node_upsert') pending.nodeUpserts.push(msg.data as PendingNodeUpsert);
    else if (msg.type === 'link_update') pending.linkUpdates.push(msg.data as PendingLinkUpdate);
    else return;

    if (snapshotReceived) scheduleFlush();
  };

  const reset = (): void => {
    cancelScheduledFlush();
    pending = emptyPending();
    snapshotReceived = false;
  };

  return {
    handle,
    flush,
    reset,
    hasSnapshot: () => snapshotReceived,
    pendingPacketCount: () => pending.packets.length,
  };
}

type UseAppMessageHandlerParams = RealtimeMessageActions & {
  epoch: number;
};

export function useAppMessageHandler(params: UseAppMessageHandlerParams) {
  const { epoch, ...actions } = params;
  const actionsRef = useRef<RealtimeMessageActions>(actions);
  actionsRef.current = actions;
  const rafRef = useRef<number | null>(null);
  const coordinatorRef = useRef<ReturnType<typeof createRealtimeMessageCoordinator> | null>(null);

  const cancelScheduledFlush = useCallback(() => {
    if (rafRef.current === null) return;
    cancelAnimationFrame(rafRef.current);
    rafRef.current = null;
  }, []);

  const scheduleFlush = useCallback(() => {
    if (rafRef.current !== null) return;
    rafRef.current = window.requestAnimationFrame(() => {
      rafRef.current = null;
      coordinatorRef.current?.flush();
    });
  }, []);

  if (coordinatorRef.current === null) {
    coordinatorRef.current = createRealtimeMessageCoordinator(
      () => actionsRef.current,
      scheduleFlush,
      cancelScheduledFlush,
    );
  }

  useEffect(() => {
    coordinatorRef.current?.reset();
    return () => coordinatorRef.current?.reset();
  }, [epoch]);

  return useCallback((msg: WSMessage) => {
    if (msg.scopeEpoch !== undefined && msg.scopeEpoch !== epoch) return;
    coordinatorRef.current?.handle(msg);
  }, [epoch]);
}
