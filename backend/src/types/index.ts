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
  network?: string;
  packetType?: number;
  routeType?: number;   // 0=TransportFlood, 1=Flood, 2=Direct, 3=TransportDirect
  hopCount?: number;
  pathHashSizeBytes?: number;
  direction?: string;   // 'rx' | 'tx' from mctomqtt
  summary?: string;     // human-readable decoded content
  payload?: Record<string, unknown>;
  path?: string[];      // relay hop hashes in packet order (1/2/3-byte => 2/4/6 hex chars)
  advertCount?: number; // for Advert packets: persistent DB count after this event
  transportCodes?: string; // raw 4-byte hex for TransportFlood/TransportDirect packets
  regionScope?: string;    // matched region name e.g. '#Europe', or undefined if no match
  isPrivate?: boolean;     // materialized at packet ingest
  visibilityOk?: boolean;  // public privacy/path validation decision
  ts: number;
}
