import { useState, useCallback } from 'react';
import {
  createAggregatedPacketFromLive,
  extractPacketSummary,
  mapRecentRows,
  mergeAggregatedPacket,
  mergePackets,
  packetInfoScore,
  type RecentPacketRow,
  FEED_MAX_PACKETS,
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
  advert_count?:  number;  // persistent DB count of times this node has advertised
  elevation_m?:   number;  // terrain elevation ASL from SRTM (set when viewshed computed)
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
  pathHashSizeBytes?: number;
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

export function useNodes() {
  const [nodes, setNodes]             = useState<Map<string, MeshNode>>(new Map());
  const [packets, setPackets]         = useState<AggregatedPacket[]>([]);
  const [arcs]                        = useState<PacketArc[]>([]);
  const [activeNodes] = useState<Set<string>>(new Set());

  const handleInitialState = useCallback((data: {
    nodes: MeshNode[];
    packets: RecentPacketRow[];
  }) => {
    const nodeMap = new Map<string, MeshNode>();
    for (const n of data.nodes) nodeMap.set(n.node_id, n);
    setNodes(nodeMap);
    setPackets(mapRecentRows(data.packets));
  }, []);

  const replaceRecentPackets = useCallback((rows: RecentPacketRow[]) => {
    const mapped = mapRecentRows(rows);
    setPackets((prev) => mergePackets(prev, mapped));
  }, []);

  const handlePacket = useCallback((packetOrArray: LivePacketData | LivePacketData[]) => {
    const packets = Array.isArray(packetOrArray) ? packetOrArray : [packetOrArray];
    if (packets.length === 0) return;

    setPackets((prev) => {
      let next = prev;
      for (const packet of packets) {
        const idx = next.findIndex((p) => p.packetHash === packet.packetHash);

        if (idx >= 0) {
          const current = next[idx]!;
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
            pathHashSizeBytes: packet.pathHashSizeBytes ?? current.pathHashSizeBytes,
            path:       packet.path ?? current.path,
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
      return next;
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

  const handleNodeUpdateBatch = useCallback((updates: { nodeId: string; ts: number }[]) => {
    if (updates.length === 0) return;
    setNodes((prev) => {
      const next = new Map(prev);
      for (const data of updates) {
        const existing = prev.get(data.nodeId);
        next.set(data.nodeId, {
          node_id:   data.nodeId,
          ...(existing ?? {}),
          last_seen: new Date(data.ts).toISOString(),
          is_online: true,
        });
      }
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

  const handleNodeUpsertBatch = useCallback((nodes: (Partial<MeshNode> & { node_id: string })[]) => {
    if (nodes.length === 0) return;
    setNodes((prev) => {
      const next = new Map(prev);
      const now = new Date();
      for (const node of nodes) {
        const existing = prev.get(node.node_id) ?? { node_id: node.node_id, last_seen: now.toISOString(), is_online: true };
        const updates = Object.fromEntries(
          Object.entries(node).filter(([, v]) => v !== undefined)
        ) as Partial<MeshNode> & { node_id: string };
        next.set(node.node_id, { ...existing, ...updates });
      }
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
    handleNodeUpdateBatch,
    handleNodeUpsert,
    handleNodeUpsertBatch,
  };
}
