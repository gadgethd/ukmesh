import { fetchJson } from '../../utils/api.js';
import { ScopedCache } from '../../utils/scopedCache.js';

export type OwnerNode = {
  node_id: string;
  canonicalId: string;
  members: string[];
  name: string | null;
  network: string;
  last_seen: string | null;
  advert_count: number;
  lat: number | null;
  lon: number | null;
  iata: string | null;
  role: number | null;
};

export function nodeRoleLabel(role: number | null): string {
  if (role === 1) return 'Companion';
  if (role === 3) return 'Room Server';
  return 'Repeater';
}

export type OwnerDashboard = {
  nodes: OwnerNode[];
};

export type OwnerSessionResponse = {
  ok: boolean;
  dashboard: OwnerDashboard;
  mqttUsername?: string | null;
};

const OWNER_SESSION_EVENT = 'meshcore-owner-session';
const LAST_HOP_EXCLUDED_COOKIE = 'meshcore-owner-last-hop-hidden-v1';
const LAST_HOP_COOKIE_MAX_AGE = 60 * 60 * 24 * 365;

export function publishOwnerSession(mqttUsername: string | null) {
  window.dispatchEvent(new CustomEvent(OWNER_SESSION_EVENT, { detail: { mqttUsername } }));
}

function readCookieValue(key: string): string | null {
  if (typeof document === 'undefined') return null;
  const prefix = `${key}=`;
  for (const entry of document.cookie.split(';')) {
    const trimmed = entry.trim();
    if (trimmed.startsWith(prefix)) {
      return decodeURIComponent(trimmed.slice(prefix.length));
    }
  }
  return null;
}

export function readExcludedLastHopSeries(nodeId: string): string[] {
  if (!nodeId) return [];
  try {
    const raw = readCookieValue(LAST_HOP_EXCLUDED_COOKIE);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const keys = parsed[nodeId];
    return Array.isArray(keys)
      ? keys.filter((value): value is string => typeof value === 'string' && value.length > 0)
      : [];
  } catch {
    return [];
  }
}

export function writeExcludedLastHopSeries(nodeId: string, seriesKeys: string[]) {
  if (typeof document === 'undefined' || !nodeId) return;
  let parsed: Record<string, unknown> = {};
  try {
    const raw = readCookieValue(LAST_HOP_EXCLUDED_COOKIE);
    if (raw) {
      const decoded = JSON.parse(raw) as Record<string, unknown>;
      if (decoded && typeof decoded === 'object') parsed = decoded;
    }
  } catch {
    parsed = {};
  }
  if (seriesKeys.length > 0) {
    parsed[nodeId] = Array.from(new Set(seriesKeys)).sort();
  } else {
    delete parsed[nodeId];
  }
  document.cookie = [
    `${LAST_HOP_EXCLUDED_COOKIE}=${encodeURIComponent(JSON.stringify(parsed))}`,
    'Path=/',
    'SameSite=Lax',
    `Max-Age=${LAST_HOP_COOKIE_MAX_AGE}`,
  ].join('; ');
}

export type LivePeer = {
  node_id: string;
  name: string | null;
  network: string | null;
  iata: string | null;
  lat: number | null;
  lon: number | null;
  packets_24h: number;
  last_seen: string | null;
};

export type LivePacket = {
  time: string;
  packet_type: number | null;
  route_type: number | null;
  hop_count: number | null;
  packet_hash: string | null;
  src_node_id: string | null;
  src_node_name: string | null;
  sender: string | null;
  body: string | null;
};

export type OwnerStatus = {
  sampled_at: string | null;
  battery_mv: number | null;
  solar_mv: number | null;
  board_temp_c: number | null;
  wifi_rssi: number | null;
  wifi_ssid: string | null;
  wifi_uptime_ms: number | null;
  ntp_synced: boolean | null;
  ntp_sync_age_ms: number | null;
  boot_count: number | null;
  reset_reason: string | null;
  max_loop_ms: number | null;
  max_loop_at_ms: number | null;
  nodes_heard_24h: number | null;
  channel_utilization: number | null;
  air_util_tx: number | null;
  air_util_rx: number | null;
  last_rx_rssi: number | null;
  last_rx_snr: number | null;
  tx_power_dbm: number | null;
  config_version: string | null;
  config_crc32: string | null;
  fs_free_bytes: number | null;
  fs_total_bytes: number | null;
  nvs_free_entries: number | null;
  channel_id: number | null;
  git_commit: string | null;
  boot_epoch: number | null;
  mqtt: {
    broker_uri: string | null;
    broker_username: string | null;
    uptime_ms: number | null;
    reconnect_attempts_1h: number | null;
    session_status_publishes: number | null;
    session_packet_publishes: number | null;
    last_offline_epoch: number | null;
  };
};

export type HeardNeighbor = {
  id: string;
  rssi: number | null;
  snr: number | null;
  last_seen_at: string | null;
  sampled_at: string | null;
};

export type OwnerLiveResponse = {
  nodeId: string;
  ownerNode: OwnerNode;
  incomingPeers: LivePeer[];
  heardBy: Array<LivePeer & { packets_7d: number; best_hops: number | null }>;
  linkHealth: Array<{
    peer_node_id: string;
    peer_name: string | null;
    peer_network: string | null;
    owner_to_peer: number;
    peer_to_owner: number;
    observed_count: number;
    itm_path_loss_db: number | null;
    itm_viable: boolean | null;
    force_viable: boolean;
    last_observed: string | null;
  }>;
  advertTrend24h: Array<{ bucket: string; adverts: number }>;
  telemetry24h: Array<{
    bucket: string;
    batteryPct: number | null;
    batteryMv: number | null;
    uptimeSecs: number | null;
    channelUtilPct: number | null;
    airUtilTxPct: number | null;
  }>;
  status: OwnerStatus | null;
  heardNeighbors: HeardNeighbor[];
  packetsSent24h: number;
  packetsReceived24h: number;
  alerts: Array<{ level: 'info' | 'warn' | 'error'; message: string }>;
  recentPackets: LivePacket[];
};

export type LastHopStrengthPoint = {
  bucket: string;
  lastHopNodeId: string | null;
  lastHopName: string;
  resolution: 'direct' | 'resolved' | 'inferred' | 'unresolved';
  avgSnr: number | null;
  avgRssi: number | null;
  sampleCount: number;
};

export type OwnerLastHopStrengthResponse = {
  points: LastHopStrengthPoint[];
};

// Scope: authenticated owner session + public privacy generation. This cache is
// cleared at logout/session expiry and bounded to 128 series or 12 MiB.
export const lastHopSeriesCache = new ScopedCache<LastHopStrengthPoint[]>({
  name: 'owner-last-hop',
  ttlMs: 5 * 60_000,
  maxEntries: 128,
  maxBytes: 12 * 1024 * 1024,
  maxInflight: 4,
});

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isOwnerSessionResponse(value: unknown): value is OwnerSessionResponse {
  if (!isRecord(value) || value['ok'] !== true || !isRecord(value['dashboard'])) return false;
  const dashboard = value['dashboard'];
  return Array.isArray(dashboard['nodes'])
    && dashboard['nodes'].every((node) => isRecord(node)
      && typeof node['node_id'] === 'string'
      && typeof node['canonicalId'] === 'string'
      && Array.isArray(node['members'])
      && node['members'].every((member) => typeof member === 'string'))
    && (value['mqttUsername'] == null || typeof value['mqttUsername'] === 'string');
}

export function isOwnerLiveResponse(value: unknown): value is OwnerLiveResponse {
  if (!isRecord(value)) return false;
  const status = value['status'];
  const heardNeighbors = value['heardNeighbors'];
  if (status != null && (!isRecord(status) || !isRecord(status['mqtt']))) return false;
  if (heardNeighbors != null && (!Array.isArray(heardNeighbors)
    || heardNeighbors.some((neighbor) => !isRecord(neighbor) || typeof neighbor['id'] !== 'string'))) return false;
  return typeof value['nodeId'] === 'string'
    && isRecord(value['ownerNode'])
    && Array.isArray(value['incomingPeers'])
    && Array.isArray(value['heardBy'])
    && Array.isArray(value['linkHealth'])
    && Array.isArray(value['advertTrend24h'])
    && Array.isArray(value['telemetry24h'])
    && Array.isArray(value['alerts'])
    && Array.isArray(value['recentPackets']);
}

export function isOwnerLastHopStrengthResponse(value: unknown): value is OwnerLastHopStrengthResponse {
  return isRecord(value) && Array.isArray(value['points']);
}

export async function fetchOwnerCsrfToken(signal?: AbortSignal): Promise<string> {
  const body = await fetchJson<{ csrfToken?: unknown }>(
    '/api/owner/csrf',
    { cache: 'no-store', signal },
    { timeoutMs: 10_000, maxBytes: 8 * 1024 },
  );
  if (typeof body.csrfToken !== 'string' || !body.csrfToken) {
    throw new Error('Unable to initialize secure owner session');
  }
  return body.csrfToken;
}

export type MappedPeer = LivePeer & { lat: number; lon: number };

export function fmtTs(timestamp: string | null): string {
  if (!timestamp) return 'No recent activity';
  return new Date(timestamp).toLocaleString();
}

export function formatDurationMs(milliseconds: number | null): string {
  if (milliseconds == null || !Number.isFinite(milliseconds) || milliseconds < 0) return '—';
  const totalMinutes = Math.floor(milliseconds / 60_000);
  const days = Math.floor(totalMinutes / 1_440);
  const hours = Math.floor((totalMinutes % 1_440) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `${days}d ${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

export function formatEpochSeconds(seconds: number | null): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds <= 0) return 'Unsynced';
  const date = new Date(seconds * 1_000);
  return Number.isFinite(date.getTime()) ? date.toLocaleString() : '—';
}

export function formatNeighborAge(timestamp: string | null, now = Date.now()): string {
  if (!timestamp) return '—';
  const time = Date.parse(timestamp);
  if (!Number.isFinite(time)) return '—';
  const totalMinutes = Math.max(0, Math.floor((now - time) / 60_000));
  const days = Math.floor(totalMinutes / 1_440);
  const hours = Math.floor((totalMinutes % 1_440) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `${days}d ${hours}h ${minutes}m ago`;
  if (hours > 0) return `${hours}h ${minutes}m ago`;
  return `${minutes}m ago`;
}

export function isValidMapCoord(lat: number | null, lon: number | null): boolean {
  if (lat == null || lon == null) return false;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (Math.abs(lat) < 5 && Math.abs(lon) < 5) return false;
  return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

export const PACKET_LABELS: Record<number, string> = {
  0: 'Request',
  1: 'Response',
  2: 'DM',
  3: 'Ack',
  4: 'Advert',
  5: 'GroupText',
  6: 'GroupData',
  7: 'AnonReq',
  8: 'Path',
  9: 'Trace',
};

export const ROUTE_LABELS: Record<number, string> = {
  0: 'Flood',
  1: 'Direct',
  2: 'Guided',
  3: 'Opportunistic',
};

export const OWNER_AXIS_COLOR = '#3a5070';
export const OWNER_LABEL_COLOR = '#6b8aaa';
export const OWNER_TOOLTIP_BG = '#0d1520';
export const OWNER_TOOLTIP_BORDER = 'rgba(0,196,255,0.25)';

export function cleanPacketBody(packet: LivePacket): string | null {
  const body = packet.body?.trim();
  if (!body) return null;
  if (/^\d+$/.test(body) && body === String(packet.packet_type ?? '')) return null;
  return body;
}

export function formatCompactTs(timestamp: string | null): string {
  if (!timestamp) return '-';
  return new Date(timestamp).toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function formatPathLoss(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(1)} dB`;
}

export function linkBadge(link: OwnerLiveResponse['linkHealth'][number]): string {
  if (link.force_viable) return 'Forced';
  if (link.itm_viable) return 'Viable';
  if (link.itm_path_loss_db != null && link.itm_path_loss_db <= 137.5) return 'Weak';
  return 'Unproven';
}
