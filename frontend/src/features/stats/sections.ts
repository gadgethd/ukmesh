import { STATS_SECTION_FIELDS } from '../../contracts/publicData.generated.js';
import type { ChartData, StatsTabId } from './model.js';

export { STATS_SECTION_FIELDS } from '../../contracts/publicData.generated.js';

type Check = (value: unknown) => boolean;
const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
const text: Check = value => typeof value === 'string' && value.length <= 4096;
const number: Check = value => typeof value === 'number' && Number.isFinite(value);
const count: Check = value => number(value) && Number(value) >= 0;
const nullable = (check: Check): Check => value => value === null || check(value);
const shape = (fields: Record<string, Check>): Check => value => record(value) && Object.entries(fields).every(([key, check]) => check(value[key]));
const rows = (check: Check, max = 10_000): Check => value => Array.isArray(value) && value.length <= max && value.every(check);
const countSeries = (label: string) => rows(shape({ [label]: text, count }));
const diversity = shape({ averageObserversPerPacket: count, maxObserversPerPacket: count,
  totalPackets24h: count, singleObserverPackets24h: count, singleObserverPct24h: count });
const pathNodes = rows(shape({ ord: count, node_id: text, name: nullable(text), lat: nullable(number), lon: nullable(number) }), 256);
const pathFields: Record<string, Check> = {
  last24hHops: shape({ one_byte: count, two_byte: count, three_byte: count }),
  multibytePackets24h: count, fullyDecodedMultibyte24h: count,
};
for (const prefix of ['latestMultibyte', 'latestFullyDecoded', 'longestFullyDecoded']) {
  pathFields[`${prefix}At`] = nullable(text);
  pathFields[`${prefix}Hash`] = nullable(text);
}
for (const prefix of ['latestFullyDecoded', 'longestFullyDecoded']) {
  pathFields[`${prefix}Hops`] = nullable(count);
  pathFields[`${prefix}Path`] = nullable(text);
  pathFields[`${prefix}Nodes`] = pathNodes;
}
const fieldChecks: Record<keyof ChartData, Check> = {
  summary: shape({ totalPackets24h: count, totalPackets7d: count, uniqueRadios24h: count, peakHour: nullable(text), peakHourCount: count }),
  packetsPerHour: countSeries('hour'), packetsPerDay: countSeries('day'),
  radiosPerHour: countSeries('hour'), radiosPerDay: countSeries('day'),
  packetTypes: countSeries('label'),
  channelTraffic: rows(shape({ channel: text, count, pct: count, allPct: count })),
  hopDistribution: rows(shape({ hops: count, count })),
  prefixCollisions: rows(shape({ prefix: text, repeats: count })),
  observerRegions: rows(shape({ iata: text, activeObservers: count, observers: count, packets24h: count, packets7d: count,
    lastPacketAt: nullable(text), series: countSeries('day'), health: shape({ score: count,
      status: value => ['healthy', 'watch', 'poor'].includes(String(value)),
      factors: value => record(value) && Object.values(value).every(number) }) }), 256),
  observerDiversity: diversity,
  signalSummary: shape({ avgRssi: nullable(number), medianRssi: nullable(number), avgSnr: nullable(number), medianSnr: nullable(number),
    rssiSamples24h: count, snrSamples24h: count }),
  routeTypes: rows(shape({ label: text, description: text, routeType: text, count })),
  transportCodes: rows(shape({ raw: text, label: text, description: text, regionScope: nullable(text), scopeCode: nullable(number),
    scopeCodeHex: nullable(text), returnCode: nullable(number), returnCodeHex: nullable(text), count })),
  pathDecodeTrend: rows(shape({ day: text, multibyte: count, fullyDecoded: count, decodedPct: count })),
  pathHashes: shape(pathFields),
};

export type StatsSectionScope = { network: string; observer: string | null; generation: number };
export type SelectedStats = { data: ChartData; generatedAt: number };

export function parseStatsSection(value: unknown, section: StatsTabId, scope: StatsSectionScope): SelectedStats | null {
  if (!record(value) || !record(value.snapshot) || value.snapshot.visibilityGeneration !== scope.generation
    || !Number.isSafeInteger(scope.generation) || scope.generation < 1
    || value.snapshot.scope !== scope.network || !text(value.snapshot.generatedAt)) return null;
  const generatedAt = Date.parse(String(value.snapshot.generatedAt));
  if (!Number.isFinite(generatedAt)) return null;
  if (value.representation !== undefined) {
    const r = value.representation;
    if (!record(r) || !Array.isArray(r.sections) || r.sections.length !== 1 || r.sections[0] !== section
      || r.visibilityGeneration !== scope.generation || !record(r.scope)
      || r.scope.network !== scope.network || r.scope.observer !== scope.observer) return null;
  } else {
    // Old servers return the complete representation. Validate it before use.
    if (!Object.entries(fieldChecks).every(([field, check]) => check(value[field]))) return null;
  }
  const required = ['summary', ...STATS_SECTION_FIELDS[section]] as (keyof ChartData)[];
  if (!required.every(field => fieldChecks[field](value[field]))) return null;
  // Defaults belong only to unrequested sections, never malformed requested data.
  // They keep shared presentation helpers safe while the active tab is loaded.
  const data: ChartData = {
    packetsPerHour: [], packetsPerDay: [], radiosPerHour: [], radiosPerDay: [], packetTypes: [], channelTraffic: [],
    hopDistribution: [], prefixCollisions: [], observerRegions: [], routeTypes: [], transportCodes: [], pathDecodeTrend: [],
    observerDiversity: { averageObserversPerPacket: 0, maxObserversPerPacket: 0, totalPackets24h: 0, singleObserverPackets24h: 0, singleObserverPct24h: 0 },
    signalSummary: { avgRssi: null, medianRssi: null, avgSnr: null, medianSnr: null, rssiSamples24h: 0, snrSamples24h: 0 },
    pathHashes: { last24hHops: { one_byte: 0, two_byte: 0, three_byte: 0 }, multibytePackets24h: 0, fullyDecodedMultibyte24h: 0,
      latestMultibyteAt: null, latestMultibyteHash: null, latestFullyDecodedAt: null, latestFullyDecodedHash: null,
      latestFullyDecodedHops: null, latestFullyDecodedPath: null, latestFullyDecodedNodes: [],
      longestFullyDecodedAt: null, longestFullyDecodedHash: null, longestFullyDecodedHops: null,
      longestFullyDecodedPath: null, longestFullyDecodedNodes: [] },
    summary: value.summary as ChartData['summary'],
  };
  for (const field of required) Object.assign(data, { [field]: value[field] });
  return { data, generatedAt };
}
