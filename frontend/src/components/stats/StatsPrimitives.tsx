import React, { lazy, memo, Suspense, useEffect } from 'react';
import type maplibregl from 'maplibre-gl';
import { AnimatedPathOverlay, type AerialPath } from '../Map/AnimatedPathOverlay.js';

export const C_CYAN = '#00c4ff';
export const C_GREEN = '#00e676';
export const C_AMBER = '#ffb300';
export const C_PURPLE = '#ce93d8';
export const C_RED = '#ff1744';
export const C_ORANGE = '#ff9800';
export const PIE_COLORS = [
  C_CYAN,
  C_GREEN,
  C_AMBER,
  C_PURPLE,
  C_RED,
  C_ORANGE,
  '#69f0ae',
  '#40c4ff',
  '#ea80fc',
  '#ffd740',
];

const AXIS_COLOR = '#3a5070';
export const LABEL_COLOR = '#6b8aaa';
export const GRID_COLOR = 'rgba(32,80,140,0.2)';
const TIP_BG = '#0d1520';
const TIP_BORDER = 'rgba(0,196,255,0.25)';
export const axisProps = {
  tick: { fill: LABEL_COLOR, fontSize: 11 },
  axisLine: { stroke: AXIS_COLOR },
  tickLine: false,
} as const;
export const gridProps = { stroke: GRID_COLOR, strokeDasharray: '3 3' } as const;

export const CustomTooltip: React.FC<{
  active?: boolean;
  payload?: any[];
  label?: string;
  labelSuffix?: string;
}> = ({ active, payload, label, labelSuffix = '' }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: TIP_BG, border: `1px solid ${TIP_BORDER}`, borderRadius: 6, padding: '8px 12px', fontSize: 12 }}>
      {label && <p style={{ color: LABEL_COLOR, margin: '0 0 4px' }}>{label}{labelSuffix}</p>}
      {payload.map((entry, index) => (
        <p key={index} style={{ color: entry.color ?? C_CYAN, margin: '2px 0' }}>
          {entry.name}: <strong>{entry.value}</strong>
          {entry.payload?.percent !== undefined && (
            <span style={{ color: LABEL_COLOR }}> ({(entry.payload.percent * 100).toFixed(1)}%)</span>
          )}
        </p>
      ))}
    </div>
  );
};

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

export const STATS_TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'traffic', label: 'Traffic' },
  { id: 'observers', label: 'Observers' },
  { id: 'paths', label: 'Paths' },
  { id: 'signal', label: 'Signal' },
] as const;
export type StatsTabId = typeof STATS_TABS[number]['id'];

export function isStatsTabId(value: string | null): value is StatsTabId {
  return STATS_TABS.some((tab) => tab.id === value);
}

export function channelLabel(channel: string): string {
  const clean = channel.trim() || 'Unknown';
  if (clean.toLowerCase() === 'encrypted') return 'Encrypted';
  if (clean.toLowerCase() === 'unknown') return 'Unknown';
  return clean.startsWith('#') ? clean : `#${clean}`;
}

export function fmtTrafficPct(value: number | undefined): string {
  return `${Number(value ?? 0).toFixed(1)}%`;
}

export function formatCount(value: number): string {
  return value.toLocaleString();
}

export function formatRatio(numerator: number, denominator: number): string {
  return denominator > 0 ? `${Math.round((numerator / denominator) * 100)}%` : '0%';
}

export function formatTimeAgo(timestamp: string | null): string {
  if (!timestamp) return 'never';
  const seconds = Math.floor(Math.max(0, Date.now() - Date.parse(timestamp)) / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

export function presentDecodedPath(nodes: DecodedPathNode[], fallback: string | null): {
  nodes: DecodedPathNode[];
  summary: string;
} {
  const locatedNodes = nodes.filter(
    (node) => Number.isFinite(node.lat) && Number.isFinite(node.lon),
  );
  const start = locatedNodes[0]?.name ?? locatedNodes[0]?.node_id ?? null;
  const end = locatedNodes[locatedNodes.length - 1]?.name
    ?? locatedNodes[locatedNodes.length - 1]?.node_id
    ?? null;
  const summary = start && end
    ? start === end ? start : `${start} -> ${end}`
    : fallback ?? 'not decoded yet';
  return { nodes: locatedNodes, summary };
}

export function describeSeries<T>(
  rows: T[],
  label: (row: T) => string,
  value: (row: T) => number,
  unit: string,
): string {
  if (rows.length === 0) return `No ${unit} values are available.`;
  const peak = rows.reduce((best, row) => value(row) > value(best) ? row : best, rows[0]!);
  const first = rows[0]!;
  const last = rows[rows.length - 1]!;
  const total = rows.reduce((sum, row) => sum + value(row), 0);
  return `${rows.length} values. First ${label(first)}: ${value(first)} ${unit}; latest ${label(last)}: ${value(last)} ${unit}; peak ${label(peak)}: ${value(peak)} ${unit}; total ${total} ${unit}.`;
}

export const StatCard: React.FC<{
  label: string;
  value: string;
  sub?: string;
  color?: string;
}> = memo(({ label, value, sub, color = C_CYAN }) => (
  <div className="stats-page__stat">
    <span className="stats-page__stat-label">{label}</span>
    <span className="stats-page__stat-value" style={{ color }}>{value}</span>
    {sub && <span className="stats-page__stat-sub">{sub}</span>}
  </div>
));

const LazyChartMount = lazy(() => import('../LazyChartMount.js'));
export const ChartCard: React.FC<{
  title: string;
  sub?: string;
  summary: string;
  children: React.ReactNode;
  tall?: boolean;
}> = memo(({ title, sub, summary, children, tall }) => (
  <div className={`stats-page__chart${tall ? ' stats-page__chart--tall' : ''}`} aria-label={title}>
    <div className="stats-page__chart-header">
      <span className="stats-page__chart-title">{title}</span>
      {sub && <span className="stats-page__chart-sub">{sub}</span>}
    </div>
    <Suspense fallback={<div className="stats-page__chart-skeleton skeleton-shimmer" aria-hidden="true" />}>
      <LazyChartMount minHeight={220}>{children}</LazyChartMount>
    </Suspense>
    <p className="ui-visually-hidden">{summary}</p>
  </div>
));

export const EmptyPacketState: React.FC<{ label?: string }> = ({
  label = 'No packet data in this window.',
}) => <div className="stats-page__empty">{label}</div>;

const CARTO_DARK_TILES = [
  'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
];

export const DecodedPathMapView: React.FC<{ nodes: DecodedPathNode[] }> = ({ nodes }) => {
  const containerRef = React.useRef<HTMLDivElement>(null);
  const [mapInstance, setMapInstance] = React.useState<maplibregl.Map | null>(null);
  const paths = React.useMemo<AerialPath[]>(() => [{
    id: 'decoded-path',
    confidence: 1,
    nodes: nodes.map((node) => ({
      position: [Number(node.lon), Number(node.lat)],
      nodeId: node.node_id,
      name: node.name ?? undefined,
    })),
  }], [nodes]);
  useEffect(() => {
    if (!containerRef.current || nodes.length < 2) return undefined;
    let cancelled = false;
    let map: maplibregl.Map | null = null;
    void Promise.all([
      import('maplibre-gl'),
      import('maplibre-gl/dist/maplibre-gl.css'),
    ]).then(([maplibreModule]) => {
      if (cancelled || !containerRef.current) return;
      const maplibre = maplibreModule.default;
      map = new maplibre.Map({
        container: containerRef.current,
        style: {
          version: 8,
          sources: {
            tiles: {
              type: 'raster',
              tiles: CARTO_DARK_TILES,
              tileSize: 256,
              maxzoom: 19,
              attribution: '© OpenStreetMap © CARTO',
            },
          },
          layers: [{ id: 'bg', type: 'raster', source: 'tiles' }],
        },
        center: [Number(nodes[0]!.lon), Number(nodes[0]!.lat)],
        zoom: 8,
        attributionControl: false,
      });
      map.on('load', () => {
        if (cancelled || !map) return;
        const bounds = new maplibre.LngLatBounds();
        for (const node of nodes) bounds.extend([Number(node.lon), Number(node.lat)]);
        map.fitBounds(bounds, { padding: 24, animate: false });
        map.setPitch(50);
        map.setBearing(-8);
        setMapInstance(map);
      });
    });
    return () => {
      cancelled = true;
      setMapInstance(null);
      map?.remove();
    };
  }, [nodes]);
  return (
    <div style={{ position: 'relative', height: '100%', width: '100%' }}>
      <div ref={containerRef} style={{ height: '100%', width: '100%' }} />
      <AnimatedPathOverlay map={mapInstance} paths={paths} active />
    </div>
  );
};
