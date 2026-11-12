import type { STATS_SECTION_FIELDS } from '../../contracts/publicData.generated.js';

export interface ChartData {
  packetsPerHour: { hour: string; count: number }[];
  packetsPerDay: { day: string; count: number }[];
  radiosPerHour: { hour: string; count: number }[];
  radiosPerDay: { day: string; count: number }[];
  packetTypes: { label: string; count: number }[];
  channelTraffic: { channel: string; count: number; pct: number; allPct: number }[];
  hopDistribution: { hops: number; count: number }[];
  prefixCollisions: { prefix: string; repeats: number }[];
  observerRegions: Array<{
    iata: string;
    activeObservers: number;
    observers: number;
    packets24h: number;
    packets7d: number;
    lastPacketAt: string | null;
    health: {
      score: number;
      status: 'healthy' | 'watch' | 'poor';
      factors: Record<string, number>;
    };
    series: { day: string; count: number }[];
  }>;
  pathHashes: {
    last24hHops: {
      one_byte: number;
      two_byte: number;
      three_byte: number;
    };
    multibytePackets24h: number;
    fullyDecodedMultibyte24h: number;
    latestMultibyteAt: string | null;
    latestMultibyteHash: string | null;
    latestFullyDecodedAt: string | null;
    latestFullyDecodedHash: string | null;
    latestFullyDecodedHops: number | null;
    latestFullyDecodedPath: string | null;
    latestFullyDecodedNodes: DecodedPathNode[];
    longestFullyDecodedAt: string | null;
    longestFullyDecodedHash: string | null;
    longestFullyDecodedHops: number | null;
    longestFullyDecodedPath: string | null;
    longestFullyDecodedNodes: DecodedPathNode[];
  };
  observerDiversity: {
    averageObserversPerPacket: number;
    maxObserversPerPacket: number;
    totalPackets24h: number;
    singleObserverPackets24h: number;
    singleObserverPct24h: number;
  };
  signalSummary: {
    avgRssi: number | null;
    medianRssi: number | null;
    avgSnr: number | null;
    medianSnr: number | null;
    rssiSamples24h: number;
    snrSamples24h: number;
  };
  routeTypes: { label: string; description: string; routeType: string; count: number }[];
  transportCodes: Array<{
    raw: string;
    label: string;
    description: string;
    regionScope: string | null;
    scopeCode: number | null;
    scopeCodeHex: string | null;
    returnCode: number | null;
    returnCodeHex: string | null;
    count: number;
  }>;
  pathDecodeTrend: { day: string; multibyte: number; fullyDecoded: number; decodedPct: number }[];
  summary: {
    totalPackets24h: number;
    totalPackets7d: number;
    uniqueRadios24h: number;
    peakHour: string | null;
    peakHourCount: number;
  };
}

export type DecodedPathNode = {
  ord: number;
  node_id: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
};

export type StatsTabId = keyof typeof STATS_SECTION_FIELDS;
