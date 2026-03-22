import { useSyncExternalStore } from 'react';
import {
  createAggregatedPacketFromLive,
  extractPacketSummary,
  mapMessageRows,
  mapRecentRows,
  mergeAggregatedPacket,
  mergePackets,
  packetInfoScore,
  type RecentPacketRow,
  FEED_MAX_MESSAGES,
  FEED_MAX_PACKETS,
  mergeMessages,
} from './packetFeed.js';

export interface MeshNode {
  node_id:        string;
  name?:          string;
  lat?:           number;
  lon?:           number;
  iata?:          string;
  role?:          number;  // 1=ChatNode, 2=Repeater, 3=RoomServer, 4=Sensor
  last_seen:      string;
  is_online:      boolean;
  hardware_model?: string;
  public_key?:    string;
  advert_count?:  number;
  elevation_m?:   number;
  is_inferred?:   boolean;
  inferred_prefix?: string;
  inferred_hash_size_bytes?: number;
  inferred_observations?: number;
  inferred_packet_count?: number;
  inferred_prev_name?: string | null;
  inferred_next_name?: string | null;
}

export interface LivePacketData {
  id:           string;
  packetHash:   string;
  rxNodeId?:    string;
  srcNodeId?:   string;
  topic:        string;
  packetType?:  number;
  hopCount?:    number;
  pathHashSizeBytes?: number;
  direction?:   string;
  summary?:     string;
  payload?:     Record<string, unknown>;
  path?:        string[];
  advertCount?: number;
  ts:           number;
}

export interface AggregatedPacket {
  id:           string;
  packetHash:   string;
  packetType?:  number;
  rxNodeId?:    string;
  observerIds:  string[];
  srcNodeId?:   string;
  topic?:       string;
  summary?:     string;
  hopCount?:    number;
  pathHashSizeBytes?: number;
  path?:        string[];
  rxCount:      number;
  txCount:      number;
  ts:           number;
  advertCount?: number;
}

export interface PacketArc {
  id:         string;
  from:       [number, number];
  to:         [number, number];
  hopCount:   number;
  ts:         number;
  packetHash: string;
}

type NodeStoreState = {
  nodes: Map<string, MeshNode>;
  packets: AggregatedPacket[];
  messages: AggregatedPacket[]; // type=5 GRP only, protected from ADV eviction
  arcs: PacketArc[];
  activeNodes: Set<string>;
};

let state: NodeStoreState = {
  nodes: new Map(),
  packets: [],
  messages: [],
  arcs: [],
  activeNodes: new Set(),
};

const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function setState(next: NodeStoreState): void {
  state = next;
  emit();
}

function getState(): NodeStoreState {
  return state;
}

function handleInitialState(data: { nodes: MeshNode[]; packets: RecentPacketRow[] }) {
  const nodeMap = new Map<string, MeshNode>();
  for (const n of data.nodes) nodeMap.set(n.node_id, n);
  const serverMessages = mapMessageRows(data.packets);
  // Merge with any messages already in state so that a WS reconnect with a
  // stale server-side cache doesn't wipe messages received via live packets.
  const messages = state.messages.length > 0
    ? mergeMessages(state.messages, serverMessages)
    : serverMessages;
  setState({
    ...state,
    nodes: nodeMap,
    packets: mapRecentRows(data.packets),
    messages,
  });
}

function replaceRecentPackets(rows: RecentPacketRow[]) {
  const mapped = mapRecentRows(rows);
  setState({
    ...state,
    packets: mergePackets(state.packets, mapped),
  });
}

function matchesObserverPathHash(observerId: string | undefined, hash: string | undefined): boolean {
  if (!observerId || !hash) return false;
  const normalizedHash = hash.trim().toUpperCase();
  if (!normalizedHash) return false;
  return observerId.slice(0, normalizedHash.length).toUpperCase() === normalizedHash;
}

function isObserverSelfEchoLoop(packet: LivePacketData, nodes: Map<string, MeshNode>): boolean {
  if (!packet.rxNodeId || !packet.path || packet.path.length < 3) return false;
  const observer = nodes.get(packet.rxNodeId);
  if (!observer || observer.role !== 2) return false;
  return matchesObserverPathHash(packet.rxNodeId, packet.path[0]) && matchesObserverPathHash(packet.rxNodeId, packet.path[packet.path.length - 1]);
}

function handlePacket(packetOrArray: LivePacketData | LivePacketData[]) {
  const incomingPackets = Array.isArray(packetOrArray) ? packetOrArray : [packetOrArray];
  if (incomingPackets.length === 0) return;

  let next = state.packets;
  for (const packet of incomingPackets) {
    const idx = next.findIndex((p) => p.packetHash === packet.packetHash);

    if (idx >= 0) {
      const current = next[idx]!;
      if (packet.rxNodeId && current.observerIds.includes(packet.rxNodeId) && isObserverSelfEchoLoop(packet, state.nodes)) {
        continue;
      }
      const observerIds = packet.rxNodeId
        ? [packet.rxNodeId, ...current.observerIds.filter((id) => id !== packet.rxNodeId)]
        : current.observerIds;
      const candidate: AggregatedPacket = {
        ...current,
        packetType: packet.packetType ?? current.packetType,
        rxNodeId: packet.rxNodeId ?? current.rxNodeId,
        observerIds,
        srcNodeId: packet.srcNodeId ?? current.srcNodeId,
        summary: packet.summary ?? extractPacketSummary(packet.payload) ?? current.summary,
        hopCount: packet.hopCount ?? current.hopCount,
        pathHashSizeBytes: packet.pathHashSizeBytes ?? current.pathHashSizeBytes,
        path: packet.path ?? current.path,
        advertCount: Math.max(current.advertCount ?? 0, packet.advertCount ?? 0) || undefined,
        rxCount: current.rxCount + (packet.direction !== 'tx' ? 1 : 0),
        txCount: current.txCount + (packet.direction === 'tx' ? 1 : 0),
        ts: packet.ts,
      };
      const entry: AggregatedPacket = {
        ...(packetInfoScore(candidate) >= packetInfoScore(current)
          ? candidate
          : mergeAggregatedPacket(current, {
              ...createAggregatedPacketFromLive(packet),
              observerIds,
              rxCount: current.rxCount + (packet.direction !== 'tx' ? 1 : 0),
              txCount: current.txCount + (packet.direction === 'tx' ? 1 : 0),
            })),
        rxCount: current.rxCount + (packet.direction !== 'tx' ? 1 : 0),
        txCount: current.txCount + (packet.direction === 'tx' ? 1 : 0),
        ts: packet.ts,
      };
      next = next.map((p, i) => i === idx ? entry : p);
    } else {
      const entry = createAggregatedPacketFromLive(packet);
      next = [entry, ...next].slice(0, FEED_MAX_PACKETS);
    }
  }

  // Also maintain the messages (type=5 only) array separately so GRP messages
  // are never evicted by a flood of ADV packets.
  let nextMessages = state.messages;
  for (const packet of incomingPackets) {
    if (packet.packetType !== 5) continue;
    const msgIdx = nextMessages.findIndex((m) => m.packetHash === packet.packetHash);
    if (msgIdx >= 0) {
      const cur = nextMessages[msgIdx]!;
      const updated: AggregatedPacket = {
        ...cur,
        summary: packet.summary ?? extractPacketSummary(packet.payload) ?? cur.summary,
        rxNodeId: packet.rxNodeId ?? cur.rxNodeId,
        observerIds: packet.rxNodeId
          ? [packet.rxNodeId, ...cur.observerIds.filter((id) => id !== packet.rxNodeId)]
          : cur.observerIds,
        rxCount: cur.rxCount + (packet.direction !== 'tx' ? 1 : 0),
        txCount: cur.txCount + (packet.direction === 'tx' ? 1 : 0),
        ts: packet.ts,
      };
      nextMessages = nextMessages.map((m, i) => (i === msgIdx ? updated : m));
    } else {
      const entry = createAggregatedPacketFromLive(packet);
      nextMessages = [entry, ...nextMessages].slice(0, FEED_MAX_MESSAGES);
    }
  }

  setState({
    ...state,
    packets: next,
    messages: nextMessages,
  });
}

function handleNodeUpdate(data: { nodeId: string; ts: number }) {
  const existing = state.nodes.get(data.nodeId);
  const next = new Map(state.nodes);
  next.set(data.nodeId, {
    node_id: data.nodeId,
    ...(existing ?? {}),
    last_seen: new Date(data.ts).toISOString(),
    is_online: true,
  });
  setState({
    ...state,
    nodes: next,
  });
}

function handleNodeUpdateBatch(updates: { nodeId: string; ts: number }[]) {
  if (updates.length === 0) return;
  const next = new Map(state.nodes);
  for (const data of updates) {
    const existing = state.nodes.get(data.nodeId);
    next.set(data.nodeId, {
      node_id: data.nodeId,
      ...(existing ?? {}),
      last_seen: new Date(data.ts).toISOString(),
      is_online: true,
    });
  }
  setState({
    ...state,
    nodes: next,
  });
}

function handleNodeUpsert(node: Partial<MeshNode> & { node_id: string }) {
  const existing = state.nodes.get(node.node_id) ?? {
    node_id: node.node_id,
    last_seen: new Date().toISOString(),
    is_online: true,
  };
  const updates = Object.fromEntries(
    Object.entries(node).filter(([, value]) => value !== undefined),
  ) as Partial<MeshNode> & { node_id: string };
  const next = new Map(state.nodes);
  next.set(node.node_id, { ...existing, ...updates });
  setState({
    ...state,
    nodes: next,
  });
}

function handleNodeUpsertBatch(nodes: (Partial<MeshNode> & { node_id: string })[]) {
  if (nodes.length === 0) return;
  const next = new Map(state.nodes);
  const nowIso = new Date().toISOString();
  for (const node of nodes) {
    const existing = state.nodes.get(node.node_id) ?? {
      node_id: node.node_id,
      last_seen: nowIso,
      is_online: true,
    };
    const updates = Object.fromEntries(
      Object.entries(node).filter(([, value]) => value !== undefined),
    ) as Partial<MeshNode> & { node_id: string };
    next.set(node.node_id, { ...existing, ...updates });
  }
  setState({
    ...state,
    nodes: next,
  });
}

export const nodeStore = {
  subscribe,
  getState,
  handleInitialState,
  replaceRecentPackets,
  handlePacket,
  handleNodeUpdate,
  handleNodeUpdateBatch,
  handleNodeUpsert,
  handleNodeUpsertBatch,
};

export function useNodeMap(): Map<string, MeshNode> {
  return useSyncExternalStore(subscribe, () => state.nodes);
}

export function usePackets(): AggregatedPacket[] {
  return useSyncExternalStore(subscribe, () => state.packets);
}

export function useMessages(): AggregatedPacket[] {
  return useSyncExternalStore(subscribe, () => state.messages);
}

export function useArcs(): PacketArc[] {
  return useSyncExternalStore(subscribe, () => state.arcs);
}

export function useActiveNodes(): Set<string> {
  return useSyncExternalStore(subscribe, () => state.activeNodes);
}

export function useNodes() {
  return {
    nodes: useNodeMap(),
    packets: usePackets(),
    arcs: useArcs(),
    activeNodes: useActiveNodes(),
    handleInitialState,
    replaceRecentPackets,
    handlePacket,
    handleNodeUpdate,
    handleNodeUpdateBatch,
    handleNodeUpsert,
    handleNodeUpsertBatch,
  };
}
