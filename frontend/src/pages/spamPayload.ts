export type OriginLevel = 'high' | 'medium' | 'low' | 'insufficient';

export interface PublicOrigin {
  region: string;
  confidence: number;
  level: OriginLevel;
  zone: { lat: number; lon: number; radiusKm: number } | null;
  observerCount: number;
  reasons: string[];
}

export interface PublicIncident {
  id: string;
  status: 'active' | 'closed';
  network: string;
  firstSeen: string;
  lastSeen: string;
  messageCount: number;
  observerCount: number;
  channels: string[];
  similarUsernames: string[];
  usernameVariants: number;
  sampleMessage: string;
  spamMarker: boolean;
  confidence: number;
  reasons: string[];
  origin: PublicOrigin;
}

export interface TimelineEntry {
  observedAt: string;
  channel: string;
  observerCount: number;
  minHopCount: number | null;
  bestRssi: number | null;
  bestSnr: number | null;
}

export interface IncidentDetail extends PublicIncident {
  timeline: TimelineEntry[];
}

export interface StatusResp {
  ongoing: boolean;
  activeIncidents: number;
  totalIncidents: number;
  messagesLast24h: number;
  observersInvolved: number;
  lastIncidentAt: string | null;
  updatedAt: string;
}

export interface IncidentsResp {
  filters: { status: string; minConfidence: number; limit: number; offset: number };
  returned: number;
  incidents: PublicIncident[];
}

const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
const count = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
const finite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);
const strings = (value: unknown): value is string[] => Array.isArray(value) && value.every(item => typeof item === 'string');
const timestamp = (value: unknown): value is string => typeof value === 'string' && Number.isFinite(Date.parse(value));
const nullableNumber = (value: unknown) => value === null || finite(value);

export function isPublicIncident(value: unknown): value is PublicIncident {
  if (!record(value) || !record(value.origin)) return false;
  const origin = value.origin;
  const zone = origin.zone;
  return typeof value.id === 'string' && value.id.length > 0
    && ['active', 'closed'].includes(String(value.status)) && typeof value.network === 'string'
    && timestamp(value.firstSeen) && timestamp(value.lastSeen)
    && count(value.messageCount) && count(value.observerCount) && count(value.usernameVariants)
    && strings(value.channels) && strings(value.similarUsernames) && strings(value.reasons)
    && typeof value.sampleMessage === 'string' && typeof value.spamMarker === 'boolean' && finite(value.confidence)
    && typeof origin.region === 'string' && finite(origin.confidence) && count(origin.observerCount) && strings(origin.reasons)
    && ['high', 'medium', 'low', 'insufficient'].includes(String(origin.level))
    && (zone === null || (record(zone) && finite(zone.lat) && Math.abs(zone.lat) <= 90
      && finite(zone.lon) && Math.abs(zone.lon) <= 180 && finite(zone.radiusKm) && zone.radiusKm > 0));
}

export function isStatusResp(value: unknown): value is StatusResp {
  return record(value) && typeof value.ongoing === 'boolean' && count(value.activeIncidents)
    && count(value.totalIncidents) && count(value.messagesLast24h) && count(value.observersInvolved)
    && (value.lastIncidentAt === null || timestamp(value.lastIncidentAt)) && timestamp(value.updatedAt);
}

export function isIncidentsResp(value: unknown): value is IncidentsResp {
  return record(value) && record(value.filters) && typeof value.filters.status === 'string'
    && finite(value.filters.minConfidence) && count(value.filters.limit) && count(value.filters.offset)
    && count(value.returned) && Array.isArray(value.incidents) && value.incidents.length <= 200
    && value.incidents.every(isPublicIncident);
}

export function isIncidentDetail(value: unknown): value is IncidentDetail {
  return isPublicIncident(value) && 'timeline' in value && Array.isArray(value.timeline)
    && value.timeline.length <= 5000 && value.timeline.every(row => record(row)
      && timestamp(row.observedAt) && typeof row.channel === 'string' && count(row.observerCount)
      && nullableNumber(row.minHopCount) && nullableNumber(row.bestRssi) && nullableNumber(row.bestSnr));
}
