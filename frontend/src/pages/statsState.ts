export type StatsPayloadMinimum = {
  snapshot?: { generatedAt?: string };
  summary: {
    totalPackets24h: number;
    totalPackets7d: number;
    uniqueRadios24h: number;
  };
  packetsPerHour?: unknown[];
  packetsPerDay?: unknown[];
  observerRegions?: unknown[];
};

export function statsGeneratedAt(value: StatsPayloadMinimum): number | null {
  const parsed = Date.parse(value.snapshot?.generatedAt ?? '');
  return Number.isFinite(parsed) ? parsed : null;
}

export type LiveObserverSnapshot = {
  generatedAt: string;
  windowMinutes: number;
  regions: Array<{ iata: string; activeObservers: number; lastPacketAt: string }>;
};

export function isLiveObserverSnapshot(value: unknown): value is LiveObserverSnapshot {
  if (!value || typeof value !== 'object') return false;
  const data = value as LiveObserverSnapshot;
  return Number.isFinite(Date.parse(data.generatedAt)) && data.windowMinutes === 15
    && Array.isArray(data.regions) && data.regions.every(row => typeof row.iata === 'string'
      && Number.isSafeInteger(row.activeObservers) && row.activeObservers >= 0
      && Number.isFinite(Date.parse(row.lastPacketAt)));
}

export function isStatsPayload(value: unknown): value is StatsPayloadMinimum {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const summary = (value as Record<string, unknown>)['summary'];
  if (typeof summary !== 'object' || summary === null || Array.isArray(summary)) return false;
  const record = summary as Record<string, unknown>;
  return ['totalPackets24h', 'totalPackets7d', 'uniqueRadios24h']
    .every((key) => typeof record[key] === 'number' && Number.isFinite(record[key]));
}

export function isStatsPayloadEmpty(value: StatsPayloadMinimum): boolean {
  return value.summary.totalPackets24h === 0
    && value.summary.totalPackets7d === 0
    && value.summary.uniqueRadios24h === 0
    && (value.packetsPerHour?.length ?? 0) === 0
    && (value.packetsPerDay?.length ?? 0) === 0
    && (value.observerRegions?.length ?? 0) === 0;
}
