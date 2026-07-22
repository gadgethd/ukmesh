import React, { useState, useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  AreaChart, Area, BarChart, Bar,
  PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer,
} from 'recharts';
import type maplibregl from 'maplibre-gl';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { getCurrentSite } from '../config/site.js';
import { chartStatsEndpoint, uncachedEndpoint } from '../utils/api.js';
import { useWatchlist } from '../hooks/useWatchlist.js';
import './stats-page.css';

// ── Colours ───────────────────────────────────────────────────────────────────
const C_CYAN   = '#00c4ff';
const C_GREEN  = '#00e676';
const C_AMBER  = '#ffb300';
const C_PURPLE = '#ce93d8';
const C_RED    = '#ff1744';
const C_ORANGE = '#ff9800';

const PIE_COLORS = [C_CYAN, C_GREEN, C_AMBER, C_PURPLE, C_RED, C_ORANGE,
                    '#69f0ae', '#40c4ff', '#ea80fc', '#ffd740'];

const AXIS_COLOR  = '#3a5070';
const LABEL_COLOR = '#6b8aaa';
const GRID_COLOR  = 'rgba(32,80,140,0.2)';
const TIP_BG      = '#0d1520';
const TIP_BORDER  = 'rgba(0,196,255,0.25)';

// ── Shared chart defaults ─────────────────────────────────────────────────────
const axisProps = { tick: { fill: LABEL_COLOR, fontSize: 11 }, axisLine: { stroke: AXIS_COLOR }, tickLine: false } as const;
const gridProps = { stroke: GRID_COLOR, strokeDasharray: '3 3' } as const;

const CustomTooltip: React.FC<{ active?: boolean; payload?: any[]; label?: string; labelSuffix?: string }> = ({
  active, payload, label, labelSuffix = '',
}) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: TIP_BG, border: `1px solid ${TIP_BORDER}`, borderRadius: 6, padding: '8px 12px', fontSize: 12 }}>
      {label && <p style={{ color: LABEL_COLOR, margin: '0 0 4px' }}>{label}{labelSuffix}</p>}
      {payload.map((p, i) => (
        <p key={i} style={{ color: p.color ?? C_CYAN, margin: '2px 0' }}>
          {p.name}: <strong>{p.value}</strong>
          {p.payload?.percent !== undefined && (
            <span style={{ color: LABEL_COLOR }}> ({(p.payload.percent * 100).toFixed(1)}%)</span>
          )}
        </p>
      ))}
    </div>
  );
};

// ── Types ─────────────────────────────────────────────────────────────────────
interface ChartData {
  packetsPerHour:  { hour: string;  count: number }[];
  packetsPerDay:   { day: string;   count: number }[];
  radiosPerHour:   { hour: string;  count: number }[];
  radiosPerDay:    { day: string;   count: number }[];
  packetTypes:     { label: string; count: number }[];
  channelTraffic:  { channel: string; count: number; pct: number; allPct: number }[];
  hopDistribution: { hops: number;  count: number }[];
  prefixCollisions:{ prefix: string; repeats: number }[];
  observerRegions: {
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
  }[];
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
    latestFullyDecodedNodes: Array<{
      ord: number;
      node_id: string;
      name: string | null;
      lat: number | null;
      lon: number | null;
    }>;
    longestFullyDecodedAt: string | null;
    longestFullyDecodedHash: string | null;
    longestFullyDecodedHops: number | null;
    longestFullyDecodedPath: string | null;
    longestFullyDecodedNodes: Array<{
      ord: number;
      node_id: string;
      name: string | null;
      lat: number | null;
      lon: number | null;
    }>;
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
  transportCodes: {
    raw: string;
    label: string;
    description: string;
    regionScope: string | null;
    scopeCode: number | null;
    scopeCodeHex: string | null;
    returnCode: number | null;
    returnCodeHex: string | null;
    count: number;
  }[];
  pathDecodeTrend: { day: string; multibyte: number; fullyDecoded: number; decodedPct: number }[];
  summary: {
    totalPackets24h:  number;
    totalPackets7d:   number;
    uniqueRadios24h:  number;
    peakHour:         string | null;
    peakHourCount:    number;
  };
}

const STATS_TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'traffic', label: 'Traffic' },
  { id: 'observers', label: 'Observers' },
  { id: 'paths', label: 'Paths' },
  { id: 'signal', label: 'Signal' },
] as const;

type StatsTabId = typeof STATS_TABS[number]['id'];

function isStatsTabId(value: string | null): value is StatsTabId {
  return STATS_TABS.some((tab) => tab.id === value);
}

function channelLabel(channel: string): string {
  const clean = channel.trim() || 'Unknown';
  if (clean.toLowerCase() === 'encrypted') return 'Encrypted';
  if (clean.toLowerCase() === 'unknown') return 'Unknown';
  return clean.startsWith('#') ? clean : `#${clean}`;
}

function fmtTrafficPct(value: number | undefined): string {
  return `${Number(value ?? 0).toFixed(1)}%`;
}

// ── Stat card ─────────────────────────────────────────────────────────────────
const StatCard: React.FC<{ label: string; value: string; sub?: string; color?: string }> = ({
  label, value, sub, color = C_CYAN,
}) => (
  <div className="stats-page__stat">
    <span className="stats-page__stat-label">{label}</span>
    <span className="stats-page__stat-value" style={{ color }}>{value}</span>
    {sub && <span className="stats-page__stat-sub">{sub}</span>}
  </div>
);

// ── Chart card ────────────────────────────────────────────────────────────────
const ChartCard: React.FC<{ title: string; sub?: string; children: React.ReactNode; tall?: boolean }> = ({
  title, sub, children, tall,
}) => (
  <div className={`stats-page__chart${tall ? ' stats-page__chart--tall' : ''}`}>
    <div className="stats-page__chart-header">
      <span className="stats-page__chart-title">{title}</span>
      {sub && <span className="stats-page__chart-sub">{sub}</span>}
    </div>
    {children}
  </div>
);

const EmptyPacketState: React.FC<{ label?: string }> = ({ label = 'No packet data in this window.' }) => (
  <div className="stats-page__empty">{label}</div>
);

const CARTO_DARK_TILES = [
  'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
];

const DecodedPathMapView: React.FC<{
  nodes: Array<{
    ord: number;
    node_id: string;
    name: string | null;
    lat: number | null;
    lon: number | null;
  }>;
}> = ({ nodes }) => {
  const containerRef = React.useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current || nodes.length < 2) return;

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
        const lineData: GeoJSON.FeatureCollection<GeoJSON.LineString> = {
          type: 'FeatureCollection',
          features: [{
            type: 'Feature',
            geometry: {
              type: 'LineString',
              coordinates: nodes.map((node) => [Number(node.lon), Number(node.lat)] as [number, number]),
            },
            properties: {},
          }],
        };

        const pointData: GeoJSON.FeatureCollection<GeoJSON.Point, { ord: string }> = {
          type: 'FeatureCollection',
          features: nodes.map((node) => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [Number(node.lon), Number(node.lat)] },
            properties: { ord: String(node.ord) },
          })),
        };

        map.addSource('decoded-path-line', { type: 'geojson', data: lineData });
        map.addSource('decoded-path-nodes', { type: 'geojson', data: pointData });

        map.addLayer({
          id: 'decoded-path-line-layer',
          type: 'line',
          source: 'decoded-path-line',
          paint: {
            'line-color': C_PURPLE,
            'line-width': 4,
            'line-opacity': 0.9,
          },
        });

        map.addLayer({
          id: 'decoded-path-node-circles',
          type: 'circle',
          source: 'decoded-path-nodes',
          paint: {
            'circle-radius': 10,
            'circle-color': '#0b1725',
            'circle-stroke-color': C_CYAN,
            'circle-stroke-width': 2,
          },
        });

        map.addLayer({
          id: 'decoded-path-node-labels',
          type: 'symbol',
          source: 'decoded-path-nodes',
          layout: {
            'text-field': ['get', 'ord'],
            'text-size': 12,
            'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
          },
          paint: {
            'text-color': '#ffffff',
          },
        });

        const bounds = new maplibre.LngLatBounds();
        for (const node of nodes) bounds.extend([Number(node.lon), Number(node.lat)]);
        map.fitBounds(bounds, { padding: 24, animate: false });
      });
    });

    return () => {
      cancelled = true;
      map?.remove();
    };
  }, [nodes]);

  return <div ref={containerRef} style={{ height: '100%', width: '100%' }} />;
};

// ── Main page ─────────────────────────────────────────────────────────────────
export const StatsPage: React.FC = () => {
  const watchlist = useWatchlist();
  const [data, setData]       = useState<ChartData | null>(null);
  const [loading, setLoading] = useState(true);
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedDecodedPath, setSelectedDecodedPath] = useState<{
    title: string;
    hash: string | null;
    hops: number | null;
    nodes: Array<{
      ord: number;
      node_id: string;
      name: string | null;
      lat: number | null;
      lon: number | null;
    }>;
  } | null>(null);
  const site = getCurrentSite();
  const statsScope = { network: site.networkFilter, observer: site.observerId };
  const refreshSeconds = 30 * 60;
  const requestedTab = searchParams.get('tab');
  const activeTab: StatsTabId = isStatsTabId(requestedTab) ? requestedTab : 'overview';

  const load = () => {
    fetch(uncachedEndpoint(chartStatsEndpoint(statsScope)), { cache: 'no-store' })
      .then(r => r.json())
      .then((d: ChartData) => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  };

  useEffect(() => {
    load();
    const t = setInterval(load, refreshSeconds * 1000);
    return () => clearInterval(t);
  }, [site.networkFilter, site.observerId]);

  const fmt = (n: number) => n.toLocaleString();
  const pct = (num: number, den: number) => den > 0 ? `${Math.round((num / den) * 100)}%` : '0%';
  const setActiveTab = (tab: StatsTabId) => {
    const next = new URLSearchParams(searchParams);
    if (tab === 'overview') {
      next.delete('tab');
    } else {
      next.set('tab', tab);
    }
    setSearchParams(next, { replace: true });
  };
  const timeAgo = (ts: string | null) => {
    if (!ts) return 'never';
    const diff = Math.max(0, Date.now() - Date.parse(ts));
    const sec = Math.floor(diff / 1000);
    if (sec < 60) return `${sec}s ago`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
    if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
    return `${Math.floor(sec / 86400)}d ago`;
  };
  const decodedPathNodes = (data?.pathHashes.latestFullyDecodedNodes ?? []).filter(
    (node) => Number.isFinite(node.lat) && Number.isFinite(node.lon),
  );
  const decodedPathStart = decodedPathNodes[0]?.name ?? decodedPathNodes[0]?.node_id ?? null;
  const decodedPathEnd = decodedPathNodes[decodedPathNodes.length - 1]?.name ?? decodedPathNodes[decodedPathNodes.length - 1]?.node_id ?? null;
  const decodedPathSummary = decodedPathStart && decodedPathEnd
    ? decodedPathStart === decodedPathEnd
      ? decodedPathStart
      : `${decodedPathStart} -> ${decodedPathEnd}`
    : data?.pathHashes.latestFullyDecodedPath ?? 'not decoded yet';
  const longestDecodedPathNodes = (data?.pathHashes.longestFullyDecodedNodes ?? []).filter(
    (node) => Number.isFinite(node.lat) && Number.isFinite(node.lon),
  );
  const longestDecodedPathStart = longestDecodedPathNodes[0]?.name ?? longestDecodedPathNodes[0]?.node_id ?? null;
  const longestDecodedPathEnd = longestDecodedPathNodes[longestDecodedPathNodes.length - 1]?.name ?? longestDecodedPathNodes[longestDecodedPathNodes.length - 1]?.node_id ?? null;
  const longestDecodedPathSummary = longestDecodedPathStart && longestDecodedPathEnd
    ? longestDecodedPathStart === longestDecodedPathEnd
      ? longestDecodedPathStart
      : `${longestDecodedPathStart} -> ${longestDecodedPathEnd}`
    : data?.pathHashes.longestFullyDecodedPath ?? 'not decoded yet';
  const selectedDecodedPathNodes = useMemo(
    () => (selectedDecodedPath?.nodes ?? []).filter(
      (node) => Number.isFinite(node.lat) && Number.isFinite(node.lon),
    ),
    [selectedDecodedPath],
  );
  const isRedactedDecodedNode = (node: { name: string | null }) => node.name === 'Redacted repeater';
  const channelTraffic = data?.channelTraffic ?? [];
  const maxChannelTraffic = Math.max(1, ...channelTraffic.map((channel) => channel.count));
  const routeTypes = data?.routeTypes ?? [];
  const maxRouteTypeCount = Math.max(1, ...routeTypes.map((route) => route.count));
  const transportCodes = data?.transportCodes ?? [];
  const maxTransportCodeCount = Math.max(1, ...transportCodes.map((code) => code.count));

  return (
    <div className="site-layout__inner">
      {/* ── Page hero ─────────────────────────────────────────────────────── */}

      <div className="site-content">

        {loading && (
          <LoadingIndicator label="Loading stats..." variant="block" />
        )}

        {data && (
          <>
            <div className="stats-page__tabs" role="tablist" aria-label="Stats sections">
              {STATS_TABS.map((tab) => (
                <button
                  key={tab.id}
                  type="button"
                  role="tab"
                  aria-selected={activeTab === tab.id}
                  className={`stats-page__tab${activeTab === tab.id ? ' stats-page__tab--active' : ''}`}
                  onClick={() => setActiveTab(tab.id)}
                >
                  {tab.label}
                </button>
              ))}
            </div>

            {activeTab === 'overview' && (
              <>
                <div className="stats-page__summary">
                  <StatCard label="Observed packets (24h)" value={fmt(data.summary.totalPackets24h)} />
                  <StatCard label="Observed packets (7D)" value={fmt(data.summary.totalPackets7d)} />
                  <StatCard label="Radios heard (24h)" value={fmt(data.summary.uniqueRadios24h)} color={C_GREEN} />
                  <StatCard
                    label="Peak hour"
                    value={data.summary.peakHour ?? '—'}
                    sub={data.summary.peakHour ? `${fmt(data.summary.peakHourCount)} packets` : undefined}
                    color={C_AMBER}
                  />
                  <StatCard
                    label="Avg observers"
                    value={data.observerDiversity.averageObserversPerPacket.toFixed(2)}
                    sub="per packet · 24h"
                    color={C_PURPLE}
                  />
                  <StatCard
                    label="Single-observer packets"
                    value={fmtTrafficPct(data.observerDiversity.singleObserverPct24h)}
                    sub={`${fmt(data.observerDiversity.singleObserverPackets24h)} packets`}
                    color={C_ORANGE}
                  />
                </div>

                <div className="stats-page__row">
                  <ChartCard title="Observed packets per hour" sub="rolling 1h window · last 24 hours">
                    <ResponsiveContainer width="100%" height={220}>
                      <AreaChart data={data.packetsPerHour} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <defs>
                          <linearGradient id="gCyan" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={C_CYAN} stopOpacity={0.3} />
                            <stop offset="95%" stopColor={C_CYAN} stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="hour" {...axisProps} interval="preserveStartEnd" />
                        <YAxis {...axisProps} />
                        <Tooltip content={<CustomTooltip />} />
                        <Area type="monotone" dataKey="count" name="Packets" stroke={C_CYAN} fill="url(#gCyan)" strokeWidth={2} dot={false} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </ChartCard>

                  <ChartCard title="Observed packets per day" sub="last 7 days">
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.packetsPerDay} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="day" {...axisProps} />
                        <YAxis {...axisProps} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="count" name="Packets" fill={C_CYAN} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>

                <div className="stats-page__row">
                  <ChartCard title="Unique radios heard per hour" sub="distinct transmitting nodes · last 24 hours">
                    <ResponsiveContainer width="100%" height={220}>
                      <AreaChart data={data.radiosPerHour} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <defs>
                          <linearGradient id="gGreen" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={C_GREEN} stopOpacity={0.3} />
                            <stop offset="95%" stopColor={C_GREEN} stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="hour" {...axisProps} interval="preserveStartEnd" />
                        <YAxis {...axisProps} allowDecimals={false} />
                        <Tooltip content={<CustomTooltip />} />
                        <Area type="monotone" dataKey="count" name="Radios" stroke={C_GREEN} fill="url(#gGreen)" strokeWidth={2} dot={false} />
                      </AreaChart>
                    </ResponsiveContainer>
                  </ChartCard>

                  <ChartCard title="Unique radios heard per day" sub="distinct transmitting nodes · last 7 days">
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.radiosPerDay} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="day" {...axisProps} />
                        <YAxis {...axisProps} allowDecimals={false} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="count" name="Radios" fill={C_GREEN} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>
              </>
            )}

            {activeTab === 'traffic' && (
              <>
                <div className="stats-page__row">
                  <ChartCard title="Packet types" sub="last 24 hours · all observer hits">
                    {data.packetTypes.length > 0 ? (
                      <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
                        <ResponsiveContainer width="50%" height={220}>
                          <PieChart>
                            <Pie
                              data={data.packetTypes}
                              dataKey="count"
                              nameKey="label"
                              cx="50%" cy="50%"
                              innerRadius={55} outerRadius={85}
                              paddingAngle={2}
                            >
                              {data.packetTypes.map((_, i) => (
                                <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                              ))}
                            </Pie>
                            <Tooltip content={<CustomTooltip />} />
                          </PieChart>
                        </ResponsiveContainer>
                        <div className="stats-page__pie-legend">
                          {data.packetTypes.map((t, i) => (
                            <div key={i} className="stats-page__pie-item">
                              <span className="stats-page__pie-dot" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                              <span className="stats-page__pie-label">{t.label}</span>
                              <span className="stats-page__pie-count">{fmt(t.count)}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ) : (
                      <EmptyPacketState />
                    )}
                  </ChartCard>

                  <ChartCard title="Route types" sub="last 24 hours">
                    {routeTypes.length > 0 ? (
                      <div className="stats-page__channel-traffic stats-page__channel-traffic--stacked">
                        {routeTypes.map((route, i) => (
                          <div key={route.routeType} className="stats-page__channel-row">
                            <div className="stats-page__channel-head">
                              <span className="stats-page__channel-name">
                                <span className="stats-page__pie-dot" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                                {route.label}
                              </span>
                              <span className="stats-page__channel-pct">{fmt(route.count)}</span>
                            </div>
                            <div className="stats-page__channel-track" aria-hidden="true">
                              <span
                                className="stats-page__channel-fill"
                                style={{
                                  width: `${Math.max(3, (route.count / maxRouteTypeCount) * 100)}%`,
                                  background: PIE_COLORS[i % PIE_COLORS.length],
                                }}
                              />
                            </div>
                            <div className="stats-page__channel-meta">
                              <span>{route.description}</span>
                              <span>raw {route.routeType}</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <EmptyPacketState />
                    )}
                  </ChartCard>
                </div>

                <div className="stats-page__observer-section">
                  <div className="stats-page__chart-header">
                    <span className="stats-page__chart-title">Channel traffic</span>
                    <span className="stats-page__chart-sub">GroupText packets in the last 24 hours</span>
                  </div>
                  {channelTraffic.length > 0 ? (
                    <div className="stats-page__channel-traffic">
                      {channelTraffic.map((channel, i) => (
                        <div key={channel.channel} className="stats-page__channel-row">
                          <div className="stats-page__channel-head">
                            <span className="stats-page__channel-name">
                              <span className="stats-page__pie-dot" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                              {channelLabel(channel.channel)}
                            </span>
                            <span className="stats-page__channel-pct">{fmtTrafficPct(channel.pct)}</span>
                          </div>
                          <div className="stats-page__channel-track" aria-hidden="true">
                            <span
                              className="stats-page__channel-fill"
                              style={{
                                width: `${Math.max(3, (channel.count / maxChannelTraffic) * 100)}%`,
                                background: PIE_COLORS[i % PIE_COLORS.length],
                              }}
                            />
                          </div>
                          <div className="stats-page__channel-meta">
                            <span>{fmt(channel.count)} observed packets</span>
                            <span>{fmtTrafficPct(channel.allPct)} of all packet traffic</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyPacketState />
                  )}
                </div>

                <div className="stats-page__observer-section">
                  <div className="stats-page__chart-header">
                    <span className="stats-page__chart-title">Transport codes</span>
                    <span className="stats-page__chart-sub">decoded from packet route metadata · last 24 hours</span>
                  </div>
                  {transportCodes.length > 0 ? (
                    <div className="stats-page__channel-traffic">
                      {transportCodes.map((code, i) => (
                        <div key={code.raw} className="stats-page__channel-row">
                          <div className="stats-page__channel-head">
                            <span className="stats-page__channel-name">
                              <span className="stats-page__pie-dot" style={{ background: PIE_COLORS[i % PIE_COLORS.length] }} />
                              {code.label}
                            </span>
                            <span className="stats-page__channel-pct">{fmt(code.count)}</span>
                          </div>
                          <div className="stats-page__channel-track" aria-hidden="true">
                            <span
                              className="stats-page__channel-fill"
                              style={{
                                width: `${Math.max(3, (code.count / maxTransportCodeCount) * 100)}%`,
                                background: PIE_COLORS[i % PIE_COLORS.length],
                              }}
                            />
                          </div>
                          <div className="stats-page__channel-meta">
                            <span>{code.description}</span>
                            <span>raw {code.raw}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyPacketState label="No transport-code packet data in this window." />
                  )}
                </div>
              </>
            )}

            {activeTab === 'observers' && (
              <>
                <div className="stats-page__summary">
                  <StatCard label="Avg observers per packet" value={data.observerDiversity.averageObserversPerPacket.toFixed(2)} color={C_PURPLE} />
                  <StatCard label="Max observers on one packet" value={fmt(data.observerDiversity.maxObserversPerPacket)} color={C_GREEN} />
                  <StatCard label="Unique packets measured" value={fmt(data.observerDiversity.totalPackets24h)} />
                  <StatCard label="Single-observer packets" value={fmt(data.observerDiversity.singleObserverPackets24h)} color={C_ORANGE} />
                  <StatCard label="Single-observer share" value={fmtTrafficPct(data.observerDiversity.singleObserverPct24h)} color={C_AMBER} />
                </div>

                <div className="stats-page__observer-section">
                  <div className="stats-page__chart-header">
                    <span className="stats-page__chart-title">Observer regions</span>
                    <span className="stats-page__chart-sub">sorted by observed packets over the last 7 days</span>
                  </div>
                  {data.observerRegions.length > 0 ? (
                    <div className="stats-page__observer-grid">
                      {data.observerRegions.map((region) => (
                        <div key={region.iata} className="stats-page__observer-card">
                          <div className="stats-page__observer-card-head">
                            <span className="stats-page__observer-iata">{region.iata}</span>
                            <span className={`stats-page__health stats-page__health--${region.health?.status ?? 'poor'}`} title="Weighted from ingest freshness, active observers, packet volume, and observer diversity">
                              {region.health?.score ?? 0}% {region.health?.status ?? 'poor'}
                            </span>
                            <span className="stats-page__observer-last">last packet {timeAgo(region.lastPacketAt)}</span>
                          </div>
                          <div className="stats-page__observer-watch">
                            <button type="button" onClick={() => watchlist.toggle('region', region.iata, `${region.iata} region`)}>{watchlist.isWatched('region', region.iata) ? '★ Region' : '☆ Region'}</button>
                            <button type="button" onClick={() => watchlist.toggle('observer', region.iata, `${region.iata} observers`)}>{watchlist.isWatched('observer', region.iata) ? '★ Observers' : '☆ Observers'}</button>
                          </div>
                          <div className="stats-page__observer-metrics">
                            <div className="stats-page__observer-metric">
                              <span>Packets (7D)</span>
                              <strong>{fmt(region.packets7d)}</strong>
                            </div>
                            <div className="stats-page__observer-metric">
                              <span>Packets (24h)</span>
                              <strong>{fmt(region.packets24h)}</strong>
                            </div>
                            <div className="stats-page__observer-metric">
                              <span>Observers</span>
                              <strong>{fmt(region.activeObservers)}|{fmt(region.observers)}</strong>
                            </div>
                          </div>
                          <div className="stats-page__observer-chart">
                            <ResponsiveContainer width="100%" height={90}>
                              <AreaChart data={region.series} margin={{ top: 4, right: 0, left: 0, bottom: 0 }}>
                                <defs>
                                  <linearGradient id={`gObserver-${region.iata}`} x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor={C_CYAN} stopOpacity={0.28} />
                                    <stop offset="95%" stopColor={C_CYAN} stopOpacity={0} />
                                  </linearGradient>
                                </defs>
                                <CartesianGrid vertical={false} {...gridProps} />
                                <XAxis dataKey="day" hide />
                                <YAxis hide />
                                <Tooltip content={<CustomTooltip />} />
                                <Area
                                  type="monotone"
                                  dataKey="count"
                                  name="Packets"
                                  stroke={C_CYAN}
                                  fill={`url(#gObserver-${region.iata})`}
                                  strokeWidth={2}
                                  dot={false}
                                />
                              </AreaChart>
                            </ResponsiveContainer>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyPacketState />
                  )}
                </div>
              </>
            )}

            {activeTab === 'paths' && (
              <>
                <div className="stats-page__observer-section">
                  <div className="stats-page__chart-header">
                    <span className="stats-page__chart-title">Path hashes</span>
                    <span className="stats-page__chart-sub">observed hop widths across the last 24 hours</span>
                  </div>
                  <div className="site-stats-grid site-stats-grid--4 health-system-grid">
                    <div className="site-stat">
                      <span className="site-stat__value">{fmt(data.pathHashes.last24hHops.one_byte)}</span>
                      <span className="site-stat__label">1-byte Hops (24h)</span>
                    </div>
                    <div className="site-stat">
                      <span className="site-stat__value">{fmt(data.pathHashes.last24hHops.two_byte)}</span>
                      <span className="site-stat__label">2-byte Hops (24h)</span>
                    </div>
                    <div className="site-stat">
                      <span className="site-stat__value">{fmt(data.pathHashes.last24hHops.three_byte)}</span>
                      <span className="site-stat__label">3-byte Hops (24h)</span>
                    </div>
                    <div className="site-stat">
                      <span className="site-stat__value">{fmt(data.pathHashes.multibytePackets24h)}</span>
                      <span className="site-stat__label">Multibyte Packets (24h)</span>
                    </div>
                    <div className="site-stat">
                      <span className="site-stat__value">{fmt(data.pathHashes.fullyDecodedMultibyte24h)}</span>
                      <span className="site-stat__label">Fully Decoded (24h)</span>
                      <span className="site-stat__sub">{pct(data.pathHashes.fullyDecodedMultibyte24h, data.pathHashes.multibytePackets24h)} of multibyte packets</span>
                    </div>
                  </div>
                  <div className="health-meta">
                    <div className="health-kv">
                      <span>Latest Multibyte Packet</span>
                      <strong>
                        {data.pathHashes.latestMultibyteHash
                          ? `${data.pathHashes.latestMultibyteHash} · ${timeAgo(data.pathHashes.latestMultibyteAt)}`
                          : 'not seen yet'}
                      </strong>
                    </div>
                    <div className="health-kv">
                      <span>Last Fully Decoded Packet</span>
                      <strong>
                        {data.pathHashes.latestFullyDecodedHash
                          ? `${data.pathHashes.latestFullyDecodedHash} · ${data.pathHashes.latestFullyDecodedHops ?? 0} hops · ${timeAgo(data.pathHashes.latestFullyDecodedAt)}`
                          : 'not decoded yet'}
                      </strong>
                    </div>
                    <div className="health-kv">
                      <span>Decoded Path</span>
                      {decodedPathNodes.length > 1 ? (
                        <button
                          type="button"
                          className="stats-page__path-link"
                          onClick={() => setSelectedDecodedPath({
                            title: 'Last Fully Decoded Path',
                            hash: data.pathHashes.latestFullyDecodedHash,
                            hops: data.pathHashes.latestFullyDecodedHops,
                            nodes: data.pathHashes.latestFullyDecodedNodes,
                          })}
                        >
                          {decodedPathSummary}
                        </button>
                      ) : (
                        <strong>{decodedPathSummary}</strong>
                      )}
                    </div>
                    <div className="health-kv">
                      <span>Longest Decoded Path</span>
                      {longestDecodedPathNodes.length > 1 ? (
                        <button
                          type="button"
                          className="stats-page__path-link"
                          onClick={() => setSelectedDecodedPath({
                            title: 'Longest Fully Decoded Path',
                            hash: data.pathHashes.longestFullyDecodedHash,
                            hops: data.pathHashes.longestFullyDecodedHops,
                            nodes: data.pathHashes.longestFullyDecodedNodes,
                          })}
                        >
                          {`${longestDecodedPathSummary} · ${data.pathHashes.longestFullyDecodedHops ?? 0} hops`}
                        </button>
                      ) : (
                        <strong>
                          {data.pathHashes.longestFullyDecodedHash
                            ? `${longestDecodedPathSummary} · ${data.pathHashes.longestFullyDecodedHops ?? 0} hops`
                            : 'not decoded yet'}
                        </strong>
                      )}
                    </div>
                  </div>
                </div>

                <div className="stats-page__row">
                  <ChartCard title="Hop count distribution" sub="last 7 days · all observer hits">
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.hopDistribution} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="hops" {...axisProps} label={{ value: 'hops', position: 'insideBottom', offset: -2, fill: LABEL_COLOR, fontSize: 10 }} />
                        <YAxis {...axisProps} />
                        <Tooltip content={<CustomTooltip labelSuffix=" hops" />} />
                        <Bar dataKey="count" name="Packets" fill={C_AMBER} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>

                  <ChartCard title="Multibyte path-hash trend" sub="packet-inferred path hashes · last 7 days">
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.pathDecodeTrend} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="day" {...axisProps} />
                        <YAxis {...axisProps} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="multibyte" name="Multibyte" fill={C_CYAN} fillOpacity={0.75} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>

                <div className="stats-page__row">
                  <ChartCard title="Repeated observed path hashes" sub="Top 10 path-hash values over the last 7 days" tall>
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={data.prefixCollisions} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid {...gridProps} />
                        <XAxis dataKey="prefix" {...axisProps} />
                        <YAxis {...axisProps} allowDecimals={false} />
                        <Tooltip content={<CustomTooltip />} formatter={(value: number) => [value, 'Repeats']} />
                        <Bar dataKey="repeats" name="Repeats" fill={C_PURPLE} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </ChartCard>
                </div>
              </>
            )}

            {activeTab === 'signal' && (
              <>
                <div className="stats-page__summary">
                  <StatCard
                    label="Mean RSSI"
                    value={data.signalSummary.avgRssi == null ? '—' : `${data.signalSummary.avgRssi.toFixed(1)} dBm`}
                    color={C_CYAN}
                  />
                  <StatCard
                    label="Mean SNR"
                    value={data.signalSummary.avgSnr == null ? '—' : `${data.signalSummary.avgSnr.toFixed(1)} dB`}
                    color={C_GREEN}
                  />
                  <StatCard label="RSSI samples" value={fmt(data.signalSummary.rssiSamples24h)} sub="last 24 hours" />
                  <StatCard label="SNR samples" value={fmt(data.signalSummary.snrSamples24h)} sub="last 24 hours" />
                </div>
                {data.signalSummary.rssiSamples24h < 1 && data.signalSummary.snrSamples24h < 1 && (
                  <EmptyPacketState label="No RSSI or SNR packet data in this window." />
                )}
              </>
            )}
          </>
        )}

        {selectedDecodedPath && selectedDecodedPathNodes.length > 1 && (
          <div
            className="disclaimer-overlay"
            role="dialog"
            aria-modal="true"
            aria-label="Decoded path map"
            onClick={() => setSelectedDecodedPath(null)}
          >
            <div className="stats-page__path-modal" onClick={(e) => e.stopPropagation()}>
              <div className="stats-page__path-modal-header">
                <div>
                  <h2 className="stats-page__path-modal-title">{selectedDecodedPath.title}</h2>
                  <p className="stats-page__path-modal-sub">
                    {selectedDecodedPath.hash} · {selectedDecodedPath.hops ?? 0} hops
                  </p>
                </div>
                <button
                  type="button"
                  className="disclaimer-modal__close stats-page__path-modal-close"
                  onClick={() => setSelectedDecodedPath(null)}
                >
                  Close
                </button>
              </div>
              <div className="stats-page__path-modal-map">
                <DecodedPathMapView nodes={selectedDecodedPathNodes} />
              </div>
              <div className="stats-page__path-modal-list">
                {selectedDecodedPathNodes.map((node) => (
                  <div key={`${node.node_id}-label-${node.ord}`} className="stats-page__path-modal-node">
                    <span>{node.ord}</span>
                    <strong>
                      {node.name ?? node.node_id}
                      {isRedactedDecodedNode(node) ? ' · approximate within 1 mile' : ''}
                    </strong>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
