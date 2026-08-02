export type StatsPayloadMinimum = {
  summary: {
    totalPackets24h: number;
    totalPackets7d: number;
    uniqueRadios24h: number;
  };
  packetsPerHour?: unknown[];
  packetsPerDay?: unknown[];
  observerRegions?: unknown[];
};

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
