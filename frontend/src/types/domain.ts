/** Shared frontend domain types.
 *
 * These describe data exchanged with the backend and are consumed by hooks,
 * pages, stores, map layers and utilities. Keeping them out of a hook module
 * means a type change no longer drags every consumer through the live-store
 * implementation.
 */

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

/** Message tags produced by the tagger-worker (TypeSafe/Jev). Flags are 0..1
 *  confidences; kind/topic/speaker are choices with their own confidences. */
export type MessageTags = {
  kind?: string;
  topic?: string;
  speaker?: string;
  safety?: number;
  directed?: number;
  mentions_location?: number;
};

export type TagConfidence = {
  kind?: number;
  topic?: number;
  speaker?: number;
};

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
  tags?:        MessageTags;
  tagConfidence?: TagConfidence;
  taggedAt?:    string;
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
