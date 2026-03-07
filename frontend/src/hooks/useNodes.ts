import { useState, useCallback, useRef } from 'react';

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

const ARC_TTL = 5000;
const FEED_MAX_PACKETS = 120;

interface HashRecord {
  observers: string[];
  ts:        number;
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
  const [arcs, setArcs]               = useState<PacketArc[]>([]);
  const [activeNodes, setActiveNodes] = useState<Set<string>>(new Set());

  const hashRegistry = useRef<Map<string, HashRecord>>(new Map());

  const pruneArcs = useCallback(() => {
    const cutoff = Date.now() - ARC_TTL - 500;
    setArcs((prev) => prev.filter((a) => a.ts > cutoff));
    const regCutoff = Date.now() - 10_000;
    for (const [k, v] of hashRegistry.current) {
      if (v.ts < regCutoff) hashRegistry.current.delete(k);
    }
  }, []);

  const handleInitialState = useCallback((data: {
    nodes: MeshNode[];
    packets: Array<{
      time: string;
      packet_hash: string;
      rx_node_id?: string;
      src_node_id?: string;
      packet_type?: number;
      hop_count?: number;
      payload?: Record<string, unknown>;
      advert_count?: number | null;
      path_hashes?: string[] | null;
    }>;
  }) => {
    const nodeMap = new Map<string, MeshNode>();
    for (const n of data.nodes) nodeMap.set(n.node_id, n);
    setNodes(nodeMap);

    // Pre-populate the live feed with the last N packets from DB.
    // Deduplicate by packet_hash in case the same packet was heard by multiple observers.
    const seen = new Set<string>();
    const initialPackets: AggregatedPacket[] = [];
    for (const row of data.packets) {
      if (seen.has(row.packet_hash)) continue;
      seen.add(row.packet_hash);
      const appData = row.payload?.['appData'] as Record<string, unknown> | undefined;
      const summary = (appData?.['name'] as string | undefined)
        ?? (row.payload?.['origin'] as string | undefined);
      initialPackets.push({
        id:          row.packet_hash,
        packetHash:  row.packet_hash,
        packetType:  row.packet_type,
        rxNodeId:    row.rx_node_id,
        srcNodeId:   row.src_node_id,
        summary,
        hopCount:    row.hop_count,
        path:        row.path_hashes ?? undefined,
        rxCount:     1,
        txCount:     0,
        ts:          new Date(row.time).getTime(),
        advertCount: row.advert_count ?? undefined,
      });
    }
    setPackets(initialPackets.slice(0, FEED_MAX_PACKETS));
  }, []);

  const handlePacket = useCallback((packet: LivePacketData) => {
    // ── Aggregate by packetHash ─────────────────────────────────────────────
    setPackets((prev) => {
      const idx = prev.findIndex((p) => p.packetHash === packet.packetHash);

      if (idx >= 0) {
        // Known packet — increment count, bubble to top
        const current = prev[idx]!;
        const candidate: AggregatedPacket = {
          ...current,
          packetType: packet.packetType ?? current.packetType,
          srcNodeId:  packet.srcNodeId ?? current.srcNodeId,
          summary:    packet.summary ?? (packet.payload?.['origin'] as string | undefined) ?? current.summary,
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
        const next = prev.filter((_, i) => i !== idx);
        return [entry, ...next];
      }

      const entry: AggregatedPacket = {
        id:         packet.id,
        packetHash: packet.packetHash,
        packetType: packet.packetType,
        rxNodeId:   packet.rxNodeId,
        srcNodeId:  packet.srcNodeId,
        summary:    packet.summary ?? (packet.payload?.['origin'] as string | undefined),
        hopCount:   packet.hopCount,
        path:       packet.path,
        rxCount:    packet.direction !== 'tx' ? 1 : 0,
        txCount:    packet.direction === 'tx'  ? 1 : 0,
        ts:         packet.ts,
        advertCount: packet.advertCount,
      };
      return [entry, ...prev].slice(0, FEED_MAX_PACKETS);
    });

    // ── Pulse the observer node ─────────────────────────────────────────────
    if (packet.rxNodeId) {
      setActiveNodes((prev) => {
        const next = new Set(prev);
        next.add(packet.rxNodeId!);
        setTimeout(() => setActiveNodes((s) => {
          const n = new Set(s); n.delete(packet.rxNodeId!); return n;
        }), 1200);
        return next;
      });
    }

    // ── Build hop-trail arcs via packetHash correlation ─────────────────────
    if (packet.rxNodeId) {
      const reg = hashRegistry.current;
      const existing = reg.get(packet.packetHash);

      if (existing) {
        const prevObserverId = existing.observers[existing.observers.length - 1]!;
        existing.observers.push(packet.rxNodeId);
        existing.ts = packet.ts;

        setNodes((currentNodes) => {
          const prev = currentNodes.get(prevObserverId);
          const curr = currentNodes.get(packet.rxNodeId!);
          if (typeof prev?.lat === 'number' && typeof prev?.lon === 'number'
            && typeof curr?.lat === 'number' && typeof curr?.lon === 'number') {
            const arc: PacketArc = {
              id:         packet.id,
              from:       [prev.lon, prev.lat],
              to:         [curr.lon, curr.lat],
              hopCount:   packet.hopCount ?? existing.observers.length,
              ts:         packet.ts,
              packetHash: packet.packetHash,
            };
            setArcs((a) => [...a, arc]);
            setTimeout(pruneArcs, ARC_TTL + 500);
          }
          return currentNodes;
        });
      } else {
        reg.set(packet.packetHash, {
          observers: [packet.rxNodeId],
          ts:        packet.ts,
        });
      }
    }
  }, [pruneArcs]);

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
    handlePacket,
    handleNodeUpdate,
    handleNodeUpsert,
  };
}
