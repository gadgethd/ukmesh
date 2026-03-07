export interface DecodedMeshPacket {
  messageHash: string;
  routeType: number;
  payloadType: number;
  payloadVersion: number;
  pathLength: number;
  path: null | unknown[];
  payload: {
    raw: string;
    decoded: Record<string, unknown> | null;
  };
  totalBytes: number;
  isValid: boolean;
}

export interface StoredPacket {
  time: Date;
  packetHash: string;
  rxNodeId?: string;
  srcNodeId?: string;
  topic: string;
  packetType?: number;
  routeType?: number;
  hopCount?: number;
  rssi?: number;
  snr?: number;
  payload?: Record<string, unknown>;
  rawHex: string;
}

export interface Node {
  nodeId: string;
  name?: string;
  lat?: number;
  lon?: number;
  lastSeen: Date;
  isOnline: boolean;
  hardwareModel?: string;
  firmwareVersion?: string;
}

export type WSMessageType = 'packet' | 'node_update' | 'node_upsert' | 'coverage_update' | 'initial_state' | 'link_update';

export interface WSMessage {
  type: WSMessageType;
  data: unknown;
  ts: number;
}

export interface LivePacket {
  id: string;
  packetHash: string;
  rxNodeId?: string;
  srcNodeId?: string;
  topic: string;
  packetType?: number;
  hopCount?: number;
  direction?: string;   // 'rx' | 'tx' from mctomqtt
  summary?: string;     // human-readable decoded content
  payload?: Record<string, unknown>;
  path?: string[];      // relay hop hashes in packet order (1/2/3-byte => 2/4/6 hex chars)
  advertCount?: number; // for Advert packets: persistent DB count after this event
  ts: number;
}
