import type { LivePacket } from '../types/index.js';

type PacketCallback = (packet: LivePacket, upstreamEnvelope?: Record<string, unknown>) => void;
type NodeCallback = (nodeId: string, meta?: { network?: string; observerId?: string }) => void;
type NodeUpsertCallback = (node: Record<string, unknown>) => void;

export function createIngestEvents() {
  let closed = false;
  const subscribers = new Set<PacketCallback>();
  const nodeSubscribers = new Set<NodeCallback>();
  const upsertSubscribers = new Set<NodeUpsertCallback>();

  function subscribe<T extends (...args: never[]) => void>(listeners: Set<T>, callback: T) {
    if (closed) return () => {};
    // Registrations are independent even when the same callback is supplied twice.
    const listener = ((...args: Parameters<T>) => callback(...args)) as T;
    listeners.add(listener);
    return () => { listeners.delete(listener); };
  }

  function emitCommittedPacket(packet: LivePacket, upstreamEnvelope?: Record<string, unknown>) {
    if (!closed) for (const callback of subscribers) callback(packet, upstreamEnvelope);
  }

  // Coalesce active advert bursts within the existing 500 ms window.
  const pendingNodeUpdates = new Map<string, { network?: string; observerId?: string }>();
  const pendingNodeUpserts = new Map<string, Record<string, unknown>>();
  let nodeFlushTimer: ReturnType<typeof setTimeout> | null = null;

  function flushNodes(): void {
    if (nodeFlushTimer) clearTimeout(nodeFlushTimer);
    nodeFlushTimer = null;
    const updates = [...pendingNodeUpdates];
    const upserts = [...pendingNodeUpserts.values()];
    pendingNodeUpdates.clear();
    pendingNodeUpserts.clear();
    for (const [nodeId, meta] of updates) {
      for (const callback of nodeSubscribers) callback(nodeId, meta);
    }
    for (const node of upserts) {
      for (const callback of upsertSubscribers) callback(node);
    }
  }

  function scheduleNodeFlush(): void {
    if (nodeFlushTimer === null) nodeFlushTimer = setTimeout(flushNodes, 500);
  }

  function emitNode(nodeId: string, meta?: { network?: string; observerId?: string }): void {
    if (closed) return;
    pendingNodeUpdates.set(nodeId, meta ?? {});
    scheduleNodeFlush();
  }

  function emitNodeUpsert(node: Record<string, unknown>): void {
    if (closed) return;
    const nodeId = String(node['node_id'] ?? '');
    if (nodeId) pendingNodeUpserts.set(nodeId, node);
    else for (const callback of upsertSubscribers) callback(node);
    scheduleNodeFlush();
  }

  return {
    onPacket: (callback: PacketCallback) => subscribe(subscribers, callback),
    onNodeSeen: (callback: NodeCallback) => subscribe(nodeSubscribers, callback),
    onNodeUpsert: (callback: NodeUpsertCallback) => subscribe(upsertSubscribers, callback),
    emitCommittedPacket, emitNode, emitNodeUpsert, flushNodes,
    close() {
      if (closed) return;
      closed = true;
      try { flushNodes(); }
      finally { subscribers.clear(); nodeSubscribers.clear(); upsertSubscribers.clear(); }
    },
  };
}
export type IngestEvents = ReturnType<typeof createIngestEvents>;
