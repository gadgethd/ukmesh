import { useState, useCallback } from 'react';

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
  advert_count?:  number;  // persistent DB count of times this node has advertised
  elevation_m?:   number;  // terrain elevation ASL from SRTM (set when viewshed computed)
}

export interface LivePacketData {
  id:           string;
  packetHash:   string;
  rxNodeId?:    string;
  srcNodeId?:   string;
  topic:        string;
  packetType?:  number;
  hopCount?:    number;
  direction?:   string;
  summary?:     string;
  payload?:     Record<string, unknown>;
  path?:        string[];   // relay hop hashes in packet order (1/2/3-byte => 2/4/6 hex chars)
  advertCount?: number;     // for Advert packets: persistent count from DB
  ts:           number;
}

/** Deduplicated packet entry shown in the live feed. */
export interface AggregatedPacket {
  id:           string;     // stable React key (first seen)
  packetHash:   string;
  packetType?:  number;
  rxNodeId?:    string;     // observer — for node-name fallback
  observerIds:  string[];
  srcNodeId?:   string;     // sender node id (from decoded payload)
  summary?:     string;
  hopCount?:    number;
  path?:        string[];   // relay hop hashes from first observation
  rxCount:      number;
  txCount:      number;
  ts:           number;     // most recent activity
  advertCount?: number;     // for Advert packets: how many times this node has advertised this session
}

export interface PacketArc {
  id:         string;
  from:       [number, number];
  to:         [number, number];
  hopCount:   number;
  ts:         number;
  packetHash: string;
}

const FEED_MAX_PACKETS = 120;

function extractPacketSummary(payload?: Record<string, unknown>): string | undefined {
  if (!payload) return undefined;
  const persisted = payload['_summary'];
  if (typeof persisted === 'string' && persisted.trim() !== '') return persisted;
  const appData = payload['appData'] as Record<string, unknown> | undefined;
  if (typeof appData?.['name'] === 'string') return appData['name'];

  const decrypted = payload['decrypted'] as Record<string, unknown> | undefined;
  if (decrypted) {
    const sender = typeof decrypted['sender'] === 'string' ? decrypted['sender'] : undefined;
    const message = typeof decrypted['message'] === 'string' ? decrypted['message'] : undefined;
    if (sender && message) return `${sender}: ${message}`;
    if (message) return message;
  }

  if (typeof payload['checksum'] === 'string') return `ACK ${(payload['checksum'] as string).slice(0, 4)}`;
  if (typeof payload['pathLength'] === 'number') return `${payload['pathLength']} hop path`;
  const pathHashes = payload['pathHashes'];
  if (Array.isArray(pathHashes)) return `trace ${pathHashes.length} hops`;

  return undefined;
}

function packetInfoScore(packet: Pick<AggregatedPacket, 'packetType' | 'srcNodeId' | 'summary' | 'hopCount' | 'path' | 'advertCount'>): number {
  let score = 0;
  if (packet.summary) score += 4;
  if (packet.srcNodeId) score += 3;
  if (packet.packetType === 4) score += 2;
  else if (packet.packetType !== undefined) score += 1;
  if (packet.hopCount !== undefined) score += 1;
  if (packet.path && packet.path.length > 0) score += 1;
  if ((packet.advertCount ?? 0) > 0) score += 1;
  return score;
}

export function useNodes() {
  const [nodes, setNodes]             = useState<Map<string, MeshNode>>(new Map());
  const [packets, setPackets]         = useState<AggregatedPacket[]>([]);
  const [arcs]                        = useState<PacketArc[]>([]);
  const [activeNodes] = useState<Set<string>>(new Set());

  const mapRecentRows = useCallback((rows: Array<{
    time: string;
    packet_hash: string;
    rx_node_id?: string;
    observer_node_ids?: string[] | null;
    src_node_id?: string;
    packet_type?: number;
    hop_count?: number;
    summary?: string | null;
    payload?: Record<string, unknown>;
    advert_count?: number | null;
    path_hashes?: string[] | null;
    rx_count?: number | null;
    tx_count?: number | null;
  }>): AggregatedPacket[] => {
    const mapped = new Map<string, AggregatedPacket>();
    for (const row of rows) {
      const summary = row.summary ?? extractPacketSummary(row.payload);
      const observerIds = Array.from(new Set([
        ...(row.observer_node_ids ?? []),
        ...(row.rx_node_id ? [row.rx_node_id] : []),
      ]));
      const next: AggregatedPacket = {
        id:          row.packet_hash,
        packetHash:  row.packet_hash,
        packetType:  row.packet_type,
        rxNodeId:    row.rx_node_id,
        observerIds,
        srcNodeId:   row.src_node_id,
        summary,
        hopCount:    row.hop_count,
        path:        row.path_hashes ?? undefined,
        rxCount:     Number(row.rx_count ?? 1),
        txCount:     Number(row.tx_count ?? 0),
        ts:          new Date(row.time).getTime(),
        advertCount: row.advert_count ?? undefined,
      };
      const current = mapped.get(row.packet_hash);
      if (!current) {
        mapped.set(row.packet_hash, next);
        continue;
      }

      const mergedCandidate: AggregatedPacket = {
        ...current,
        packetType: next.packetType ?? current.packetType,
        rxNodeId: next.rxNodeId ?? current.rxNodeId,
        observerIds: Array.from(new Set([...current.observerIds, ...next.observerIds])),
        srcNodeId: next.srcNodeId ?? current.srcNodeId,
        summary: next.summary ?? current.summary,
        hopCount: next.hopCount ?? current.hopCount,
        path: next.path ?? current.path,
        rxCount: Math.max(current.rxCount, next.rxCount),
        txCount: Math.max(current.txCount, next.txCount),
        ts: Math.max(current.ts, next.ts),
        advertCount: Math.max(current.advertCount ?? 0, next.advertCount ?? 0) || undefined,
      };
      mapped.set(
        row.packet_hash,
        packetInfoScore(mergedCandidate) >= packetInfoScore(current) ? mergedCandidate : {
          ...current,
          observerIds: Array.from(new Set([...current.observerIds, ...next.observerIds])),
          rxCount: Math.max(current.rxCount, next.rxCount),
          txCount: Math.max(current.txCount, next.txCount),
          ts: Math.max(current.ts, next.ts),
          advertCount: Math.max(current.advertCount ?? 0, next.advertCount ?? 0) || undefined,
        },
      );
    }
    return Array.from(mapped.values())
      .sort((a, b) => b.ts - a.ts)
      .slice(0, FEED_MAX_PACKETS);
  }, []);

  const mergePackets = useCallback((existing: AggregatedPacket[], incoming: AggregatedPacket[]) => {
    const merged = new Map<string, AggregatedPacket>();

    for (const packet of existing) {
      merged.set(packet.packetHash, packet);
    }

    for (const next of incoming) {
      const current = merged.get(next.packetHash);
      if (!current) {
        merged.set(next.packetHash, next);
        continue;
      }

      const candidate: AggregatedPacket = {
        ...current,
        packetType: next.packetType ?? current.packetType,
        rxNodeId: next.rxNodeId ?? current.rxNodeId,
        observerIds: Array.from(new Set([...current.observerIds, ...next.observerIds])),
        srcNodeId: next.srcNodeId ?? current.srcNodeId,
        summary: next.summary ?? current.summary,
        hopCount: next.hopCount ?? current.hopCount,
        path: next.path ?? current.path,
        rxCount: Math.max(current.rxCount, next.rxCount),
        txCount: Math.max(current.txCount, next.txCount),
        ts: Math.max(current.ts, next.ts),
        advertCount: Math.max(current.advertCount ?? 0, next.advertCount ?? 0) || undefined,
      };

      merged.set(
        next.packetHash,
        packetInfoScore(candidate) >= packetInfoScore(current)
          ? candidate
          : {
              ...current,
              observerIds: Array.from(new Set([...current.observerIds, ...next.observerIds])),
              rxCount: Math.max(current.rxCount, next.rxCount),
              txCount: Math.max(current.txCount, next.txCount),
              ts: Math.max(current.ts, next.ts),
              advertCount: Math.max(current.advertCount ?? 0, next.advertCount ?? 0) || undefined,
            },
      );
    }

    return Array.from(merged.values())
      .sort((a, b) => b.ts - a.ts)
      .slice(0, FEED_MAX_PACKETS);
  }, []);

  const handleInitialState = useCallback((data: {
    nodes: MeshNode[];
    packets: Array<{
      time: string;
      packet_hash: string;
      rx_node_id?: string;
      observer_node_ids?: string[] | null;
      src_node_id?: string;
      packet_type?: number;
      hop_count?: number;
      summary?: string | null;
      payload?: Record<string, unknown>;
      advert_count?: number | null;
      path_hashes?: string[] | null;
      rx_count?: number | null;
      tx_count?: number | null;
    }>;
  }) => {
    const nodeMap = new Map<string, MeshNode>();
    for (const n of data.nodes) nodeMap.set(n.node_id, n);
    setNodes(nodeMap);
    setPackets(mapRecentRows(data.packets));
  }, [mapRecentRows]);

  const replaceRecentPackets = useCallback((rows: Array<{
    time: string;
    packet_hash: string;
    rx_node_id?: string;
    observer_node_ids?: string[] | null;
    src_node_id?: string;
    packet_type?: number;
    hop_count?: number;
    summary?: string | null;
    payload?: Record<string, unknown>;
    advert_count?: number | null;
    path_hashes?: string[] | null;
    rx_count?: number | null;
    tx_count?: number | null;
  }>) => {
    const mapped = mapRecentRows(rows);
    setPackets((prev) => mergePackets(prev, mapped));
  }, [mapRecentRows, mergePackets]);

  const handlePacket = useCallback((packet: LivePacketData) => {
    // ── Aggregate by packetHash ─────────────────────────────────────────────
    setPackets((prev) => {
      const idx = prev.findIndex((p) => p.packetHash === packet.packetHash);

      if (idx >= 0) {
        // Known packet — increment count, bubble to top
        const current = prev[idx]!;
        const observerIds = packet.rxNodeId
          ? [packet.rxNodeId, ...current.observerIds.filter((id) => id !== packet.rxNodeId)]
          : current.observerIds;
        const candidate: AggregatedPacket = {
          ...current,
          packetType: packet.packetType ?? current.packetType,
          rxNodeId:   packet.rxNodeId ?? current.rxNodeId,
          observerIds,
          srcNodeId:  packet.srcNodeId ?? current.srcNodeId,
          summary:    packet.summary ?? extractPacketSummary(packet.payload) ?? current.summary,
          hopCount:   packet.hopCount ?? current.hopCount,
          path:       packet.path ?? current.path,
          advertCount: Math.max(current.advertCount ?? 0, packet.advertCount ?? 0) || undefined,
          rxCount: current.rxCount + (packet.direction !== 'tx' ? 1 : 0),
          txCount: current.txCount + (packet.direction === 'tx' ? 1 : 0),
          ts: packet.ts,
        };
        const useCandidate = packetInfoScore(candidate) >= packetInfoScore(current);
        const entry: AggregatedPacket = {
          ...(useCandidate ? candidate : current),
          rxCount: current.rxCount + (packet.direction !== 'tx' ? 1 : 0),
          txCount: current.txCount + (packet.direction === 'tx' ? 1 : 0),
          ts: packet.ts,
        };
        return prev.map((p, i) => i === idx ? entry : p);
      }

      const entry: AggregatedPacket = {
        id:         packet.id,
        packetHash: packet.packetHash,
        packetType: packet.packetType,
        rxNodeId:   packet.rxNodeId,
        observerIds: packet.rxNodeId ? [packet.rxNodeId] : [],
        srcNodeId:  packet.srcNodeId,
        summary:    packet.summary ?? extractPacketSummary(packet.payload),
        hopCount:   packet.hopCount,
        path:       packet.path,
        rxCount:    packet.direction !== 'tx' ? 1 : 0,
        txCount:    packet.direction === 'tx'  ? 1 : 0,
        ts:         packet.ts,
        advertCount: packet.advertCount,
      };
      return [entry, ...prev].slice(0, FEED_MAX_PACKETS);
    });

  }, []);

  const handleNodeUpdate = useCallback((data: { nodeId: string; ts: number }) => {
    setNodes((prev) => {
      const existing = prev.get(data.nodeId);
      const next = new Map(prev);
      next.set(data.nodeId, {
        node_id:   data.nodeId,
        ...(existing ?? {}),
        last_seen: new Date(data.ts).toISOString(),
        is_online: true,
      });
      return next;
    });
  }, []);

  const handleNodeUpsert = useCallback((node: Partial<MeshNode> & { node_id: string }) => {
    setNodes((prev) => {
      const existing = prev.get(node.node_id) ?? { node_id: node.node_id, last_seen: new Date().toISOString(), is_online: true };
      const next = new Map(prev);
      // Filter out undefined values so they don't overwrite existing lat/lon/name etc.
      const updates = Object.fromEntries(
        Object.entries(node).filter(([, v]) => v !== undefined)
      ) as Partial<MeshNode> & { node_id: string };
      next.set(node.node_id, { ...existing, ...updates });
      return next;
    });
  }, []);

  return {
    nodes,
    packets,
    arcs,
    activeNodes,
    handleInitialState,
    replaceRecentPackets,
    handlePacket,
    handleNodeUpdate,
    handleNodeUpsert,
  };
}
