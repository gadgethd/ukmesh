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
} from './packetFeed.js';
import { hasCoords, isProhibitedMapNode } from '../utils/pathing.js';
import {
  canonicalNodeId,
  canonicalOptionalNodeId,
} from '../utils/nodeIds.js';
export { canonicalNodeId } from '../utils/nodeIds.js';

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
}

export interface LivePacketData {
  id:           string;
  packetHash:   string;
  rxNodeId?:    string;
  srcNodeId?:   string;
  topic:        string;
  iata?:        string;
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
  firstSeenTs?: number;
  rxNodeId?:    string;
  observerIds:  string[];
  observerIatas: string[];
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

export const PACKET_ARC_TTL_MS = 5_000;
export const PACKET_ARC_MAX = 200;

type NodeStoreState = {
  scopeKey: string | null;
  epoch: number;
  nodes: Map<string, MeshNode>;
  packets: AggregatedPacket[];
  messages: AggregatedPacket[]; // type=5 GRP only, protected from ADV eviction
  arcs: PacketArc[];
  arcCollectionEnabled: boolean;
  activeNodes: Set<string>;
};

let state: NodeStoreState = {
  scopeKey: null,
  epoch: 0,
  nodes: new Map(),
  packets: [],
  messages: [],
  arcs: [],
  arcCollectionEnabled: false,
  activeNodes: new Set(),
};

function normalizeNode<T extends Partial<MeshNode> & { node_id: string }>(
  node: T,
): T {
  const publicKey = typeof node.public_key === 'string'
    ? canonicalNodeId(node.public_key)
    : node.public_key;
  return {
    ...node,
    node_id: canonicalNodeId(node.node_id),
    ...(publicKey !== undefined ? { public_key: publicKey } : {}),
  };
}

function normalizeLivePacket(packet: LivePacketData): LivePacketData {
  return {
    ...packet,
    packetHash: packet.packetHash.trim().toUpperCase(),
    rxNodeId: canonicalOptionalNodeId(packet.rxNodeId),
    srcNodeId: canonicalOptionalNodeId(packet.srcNodeId),
    iata: packet.iata?.trim().toUpperCase(),
    path: packet.path?.map((value) => value.trim().toUpperCase()).filter(Boolean),
  };
}

function seenAtMs(node: Pick<MeshNode, 'last_seen'>): number {
  const value = Date.parse(node.last_seen);
  return Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
}

/**
 * Initial-state snapshots can be up to a minute old and may arrive after live
 * WebSocket events. Keep the most recently seen representation while filling
 * any fields it lacks from the other copy.
 */
function mergeNode(existing: MeshNode, incoming: MeshNode): MeshNode {
  const newer = seenAtMs(existing) > seenAtMs(incoming) ? existing : incoming;
  const older = newer === existing ? incoming : existing;
  const advertCount = Math.max(existing.advert_count ?? 0, incoming.advert_count ?? 0);
  return normalizeNode({
    ...older,
    ...newer,
    ...(advertCount > 0 ? { advert_count: advertCount } : {}),
  });
}

const listeners = new Set<() => void>();
let arcExpiryTimer: ReturnType<typeof setTimeout> | null = null;

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

function acceptsEpoch(epoch: number | undefined): boolean {
  return epoch === undefined || epoch === state.epoch;
}

function reset(scopeKey: string): number {
  clearArcExpiryTimer();
  const epoch = state.epoch + 1;
  setState({
    scopeKey,
    epoch,
    nodes: new Map(),
    packets: [],
    messages: [],
    arcs: [],
    arcCollectionEnabled: state.arcCollectionEnabled,
    activeNodes: new Set(),
  });
  return epoch;
}

function clearArcExpiryTimer(): void {
  if (arcExpiryTimer === null) return;
  clearTimeout(arcExpiryTimer);
  arcExpiryTimer = null;
}

function scheduleArcExpiry(): void {
  clearArcExpiryTimer();
  if (state.arcs.length === 0) return;
  const expiresAt = Math.min(...state.arcs.map((arc) => arc.ts + PACKET_ARC_TTL_MS));
  const delay = Math.max(1, expiresAt - Date.now() + 1);
  arcExpiryTimer = setTimeout(() => {
    arcExpiryTimer = null;
    pruneExpiredArcs();
  }, delay);
}

function pruneExpiredArcs(now = Date.now()): void {
  const arcs = state.arcs.filter((arc) => now - arc.ts < PACKET_ARC_TTL_MS);
  if (arcs.length !== state.arcs.length) {
    setState({ ...state, arcs });
  }
  scheduleArcExpiry();
}

function setArcCollectionEnabled(enabled: boolean): void {
  if (state.arcCollectionEnabled === enabled && (enabled || state.arcs.length === 0)) return;
  if (!enabled) clearArcExpiryTimer();
  setState({
    ...state,
    arcCollectionEnabled: enabled,
    arcs: enabled ? state.arcs : [],
  });
}

function packetArc(
  packet: LivePacketData,
  nodes: Map<string, MeshNode>,
): PacketArc | null {
  if (!packet.srcNodeId || !packet.rxNodeId || packet.srcNodeId === packet.rxNodeId) return null;
  const source = nodes.get(packet.srcNodeId);
  const receiver = nodes.get(packet.rxNodeId);
  if (!hasCoords(source) || !hasCoords(receiver)) return null;
  // New overlays never retain or expose an endpoint that is subject to the
  // private-node mask. The ordinary node layer owns its separate masked marker.
  if (isProhibitedMapNode(source) || isProhibitedMapNode(receiver)) return null;
  return {
    id: `${packet.packetHash}:${packet.rxNodeId}:${packet.ts}`,
    from: [source.lon, source.lat],
    to: [receiver.lon, receiver.lat],
    hopCount: packet.hopCount ?? 0,
    ts: packet.ts,
    packetHash: packet.packetHash,
  };
}

function handleInitialState(
  data: { nodes: MeshNode[]; packets: RecentPacketRow[]; messages?: RecentPacketRow[] },
  epoch?: number,
) {
  if (!acceptsEpoch(epoch)) return;
  const nodeMap = new Map<string, MeshNode>();
  for (const serverNode of data.nodes) {
    const normalized = normalizeNode(serverNode);
    const existing = nodeMap.get(normalized.node_id);
    nodeMap.set(normalized.node_id, existing ? mergeNode(existing, normalized) : normalized);
  }
  const serverPackets = mapRecentRows(data.packets);
  const serverMessages = mapMessageRows(data.messages ?? data.packets);
  setState({
    ...state,
    nodes: nodeMap,
    packets: serverPackets,
    messages: serverMessages,
    arcs: [],
    activeNodes: new Set(),
  });
}

function replaceRecentPackets(rows: RecentPacketRow[], epoch?: number) {
  if (!acceptsEpoch(epoch)) return;
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
  return packet.path.every((hash) => matchesObserverPathHash(packet.rxNodeId, hash));
}

function handlePacket(packetOrArray: LivePacketData | LivePacketData[], epoch?: number) {
  if (!acceptsEpoch(epoch)) return;
  const incomingPackets = (Array.isArray(packetOrArray) ? packetOrArray : [packetOrArray])
    .map(normalizeLivePacket);
  if (incomingPackets.length === 0) return;

  let next = state.packets.slice();
  const packetIndex = new Map(next.map((packet, index) => [packet.packetHash, index]));
  for (const packet of incomingPackets) {
    const idx = packetIndex.get(packet.packetHash) ?? -1;

    if (idx >= 0) {
      const current = next[idx]!;
      if (packet.rxNodeId && current.observerIds.includes(packet.rxNodeId) && isObserverSelfEchoLoop(packet, state.nodes)) {
        continue;
      }
      const observerIds = packet.rxNodeId
        ? [packet.rxNodeId, ...current.observerIds.filter((id) => id !== packet.rxNodeId)]
        : current.observerIds;
      const observerIatas = packet.iata
        ? [packet.iata, ...current.observerIatas.filter((iata) => iata !== packet.iata)]
        : current.observerIatas;
      const candidate: AggregatedPacket = {
        ...current,
        packetType: packet.packetType ?? current.packetType,
        firstSeenTs: current.firstSeenTs ?? current.ts,
        rxNodeId: packet.rxNodeId ?? current.rxNodeId,
        observerIds,
        observerIatas,
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
              observerIatas,
              rxCount: current.rxCount + (packet.direction !== 'tx' ? 1 : 0),
              txCount: current.txCount + (packet.direction === 'tx' ? 1 : 0),
            })),
        rxCount: current.rxCount + (packet.direction !== 'tx' ? 1 : 0),
        txCount: current.txCount + (packet.direction === 'tx' ? 1 : 0),
        firstSeenTs: current.firstSeenTs ?? current.ts,
        ts: packet.ts,
      };
      next[idx] = entry;
    } else {
      const entry = createAggregatedPacketFromLive(packet);
      packetIndex.set(packet.packetHash, next.length);
      next.push(entry);
    }
  }
  next.sort((a, b) => b.ts - a.ts);
  if (next.length > FEED_MAX_PACKETS) next.length = FEED_MAX_PACKETS;

  // Also maintain the messages (type=5 only) array separately so GRP messages
  // are never evicted by a flood of ADV packets.
  let nextMessages = state.messages.slice();
  const messageIndex = new Map(nextMessages.map((message, index) => [message.packetHash, index]));
  for (const packet of incomingPackets) {
    if (packet.packetType !== 5) continue;
    const msgIdx = messageIndex.get(packet.packetHash) ?? -1;
    if (msgIdx >= 0) {
      const cur = nextMessages[msgIdx]!;
      const updated: AggregatedPacket = {
        ...cur,
        firstSeenTs: cur.firstSeenTs ?? cur.ts,
        summary: packet.summary ?? extractPacketSummary(packet.payload) ?? cur.summary,
        rxNodeId: packet.rxNodeId ?? cur.rxNodeId,
        observerIds: packet.rxNodeId
          ? [packet.rxNodeId, ...cur.observerIds.filter((id) => id !== packet.rxNodeId)]
          : cur.observerIds,
        observerIatas: packet.iata
          ? [packet.iata, ...cur.observerIatas.filter((iata) => iata !== packet.iata)]
          : cur.observerIatas,
        rxCount: cur.rxCount + (packet.direction !== 'tx' ? 1 : 0),
        txCount: cur.txCount + (packet.direction === 'tx' ? 1 : 0),
        ts: packet.ts,
      };
      nextMessages[msgIdx] = updated;
    } else {
      const entry = createAggregatedPacketFromLive(packet);
      messageIndex.set(packet.packetHash, nextMessages.length);
      nextMessages.push(entry);
    }
  }
  nextMessages.sort((a, b) => b.ts - a.ts);
  if (nextMessages.length > FEED_MAX_MESSAGES) nextMessages.length = FEED_MAX_MESSAGES;

  let nextArcs = state.arcs.filter((arc) => Date.now() - arc.ts < PACKET_ARC_TTL_MS);
  if (state.arcCollectionEnabled) {
    for (const packet of incomingPackets) {
      const arc = packetArc(packet, state.nodes);
      if (arc) nextArcs.push(arc);
    }
    if (nextArcs.length > PACKET_ARC_MAX) {
      nextArcs = nextArcs
        .sort((a, b) => b.ts - a.ts)
        .slice(0, PACKET_ARC_MAX);
    }
  } else {
    nextArcs = [];
  }

  setState({
    ...state,
    packets: next,
    messages: nextMessages,
    arcs: nextArcs,
  });
  scheduleArcExpiry();
}

function handleNodeUpdate(data: { nodeId: string; ts: number }, epoch?: number) {
  if (!acceptsEpoch(epoch)) return;
  const nodeId = canonicalNodeId(data.nodeId);
  const existing = state.nodes.get(nodeId);
  const seenAt = new Date(data.ts).toISOString();
  const next = new Map(state.nodes);
  next.set(nodeId, {
    node_id: nodeId,
    ...(existing ?? {}),
    last_seen: existing && seenAtMs(existing) > data.ts ? existing.last_seen : seenAt,
    is_online: true,
  });
  setState({
    ...state,
    nodes: next,
  });
}

function handleNodeUpdateBatch(updates: { nodeId: string; ts: number }[], epoch?: number) {
  if (!acceptsEpoch(epoch)) return;
  if (updates.length === 0) return;
  const next = new Map(state.nodes);
  for (const data of updates) {
    const nodeId = canonicalNodeId(data.nodeId);
    const existing = next.get(nodeId);
    const seenAt = new Date(data.ts).toISOString();
    next.set(nodeId, {
      node_id: nodeId,
      ...(existing ?? {}),
      last_seen: existing && seenAtMs(existing) > data.ts ? existing.last_seen : seenAt,
      is_online: true,
    });
  }
  setState({
    ...state,
    nodes: next,
  });
}

function handleNodeUpsert(node: Partial<MeshNode> & { node_id: string }, epoch?: number) {
  if (!acceptsEpoch(epoch)) return;
  const normalized = normalizeNode(node);
  const existing = state.nodes.get(normalized.node_id) ?? {
    node_id: normalized.node_id,
    last_seen: new Date().toISOString(),
    is_online: true,
  };
  const updates = Object.fromEntries(
    Object.entries(normalized).filter(([, value]) => value !== undefined),
  ) as Partial<MeshNode> & { node_id: string };
  const merged = mergeNode(existing, {
    ...existing,
    ...updates,
  });
  const next = new Map(state.nodes);
  next.set(normalized.node_id, merged);
  setState({
    ...state,
    nodes: next,
  });
}

function handleNodeUpsertBatch(
  nodes: (Partial<MeshNode> & { node_id: string })[],
  epoch?: number,
) {
  if (!acceptsEpoch(epoch)) return;
  if (nodes.length === 0) return;
  const next = new Map(state.nodes);
  const nowIso = new Date().toISOString();
  for (const node of nodes) {
    const normalized = normalizeNode(node);
    const existing = next.get(normalized.node_id) ?? {
      node_id: normalized.node_id,
      last_seen: nowIso,
      is_online: true,
    };
    const updates = Object.fromEntries(
      Object.entries(normalized).filter(([, value]) => value !== undefined),
    ) as Partial<MeshNode> & { node_id: string };
    next.set(normalized.node_id, mergeNode(existing, {
      ...existing,
      ...updates,
    }));
  }
  setState({
    ...state,
    nodes: next,
  });
}

export const nodeStore = {
  subscribe,
  getState,
  reset,
  handleInitialState,
  replaceRecentPackets,
  handlePacket,
  handleNodeUpdate,
  handleNodeUpdateBatch,
  handleNodeUpsert,
  handleNodeUpsertBatch,
  setArcCollectionEnabled,
  pruneExpiredArcs,
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
