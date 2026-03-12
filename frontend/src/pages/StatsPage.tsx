import React, { useState, useEffect } from 'react';
import {
  AreaChart, Area, BarChart, Bar,
  PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer,
} from 'recharts';
import { getCurrentSite } from '../config/site.js';
import { chartStatsEndpoint, uncachedEndpoint } from '../utils/api.js';

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
  repeatersPerDay: { hour: string;  count: number }[];
  hopDistribution: { hops: number;  count: number }[];
  prefixCollisions:{ prefix: string; repeats: number }[];
  observerRegions: {
    iata: string;
    observers: number;
    packets24h: number;
    packets7d: number;
    lastPacketAt: string | null;
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
  };
  summary: {
    totalPackets24h:  number;
    totalPackets7d:   number;
    uniqueRadios24h:  number;
    activeRepeaters:  number;
    staleRepeaters:   number;
    peakHour:         string | null;
    peakHourCount:    number;
  };
}

interface HealthPayload {
  system: {
    generated_at: string;
    cpu: { load_1m: number; count: number; load_pct: number; usage_pct: number };
    memory: { total_mb: number; used_mb: number; used_pct: number };
    disk: { total_gb: number; used_gb: number; used_pct: number };
    runtime: { uptime_s: number; node_version: string; platform: string; arch: string };
  };
  workers: Array<{
    worker_name: string;
    status: string;
    queue_depth: number;
    processed_1h: number;
    last_activity_at: string | null;
  }>;
  frontend_errors_1h: number;
  ingest: {
    stale_nodes: number;
    active_nodes: number;
    max_stale_minutes: number;
    stale_threshold_minutes: number;
    global_last_packet_at: string | null;
  };
}

function workerLabel(name: string): string {
  if (name === 'viewshed-worker') return 'Viewshed Worker';
  if (name === 'link-worker') return 'Link Worker';
  if (name === 'path-learning') return 'Path Learning';
  if (name === 'health-worker') return 'Health Worker';
  if (name === 'link-backfill-worker') return 'Link Backfill Worker';
  if (name === 'path-history-worker') return 'Path History Worker';
  return name;
}

function fmtInt(value: number | undefined): string {
  return Number(value ?? 0).toLocaleString();
}

function fmtPct(value: number | undefined): string {
  return `${Number(value ?? 0).toFixed(1)}%`;
}

function fmtGb(value: number | undefined): string {
  return `${Number(value ?? 0).toFixed(1)} GB`;
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

// ── Main page ─────────────────────────────────────────────────────────────────
export const StatsPage: React.FC = () => {
  const [data, setData]       = useState<ChartData | null>(null);
  const [health, setHealth]   = useState<HealthPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const site = getCurrentSite();
  const statsScope = { network: site.networkFilter, observer: site.observerId };
  const refreshSeconds = 30 * 60;
  const healthRefreshSeconds = 5 * 60;

  const load = () => {
    fetch(uncachedEndpoint(chartStatsEndpoint(statsScope)), { cache: 'no-store' })
      .then(r => r.json())
      .then((d: ChartData) => { setData(d); setLoading(false); setLastUpdate(new Date()); })
      .catch(() => setLoading(false));
  };

  const loadHealth = () => {
    fetch('/api/health', { cache: 'no-store' })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: HealthPayload) => setHealth(d))
      .catch(() => undefined);
  };

  useEffect(() => {
    load();
    const t = setInterval(load, refreshSeconds * 1000);
    return () => clearInterval(t);
  }, [site.networkFilter, site.observerId]);

  useEffect(() => {
    loadHealth();
    const t = setInterval(loadHealth, healthRefreshSeconds * 1000);
    return () => clearInterval(t);
  }, []);

  const fmt = (n: number) => n.toLocaleString();
  const pct = (num: number, den: number) => den > 0 ? `${Math.round((num / den) * 100)}%` : '0%';
  const timeAgo = (ts: string | null) => {
    if (!ts) return 'never';
    const diff = Math.max(0, Date.now() - Date.parse(ts));
    const sec = Math.floor(diff / 1000);
    if (sec < 60) return `${sec}s ago`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
    if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
    return `${Math.floor(sec / 86400)}d ago`;
  };

  return (
    <div className="site-layout__inner">
      {/* ── Page hero ─────────────────────────────────────────────────────── */}
      <section className="site-page-hero">
        <div className="site-content">
          <h1 className="site-page-hero__title">Network Stats</h1>
          <p className="site-page-hero__sub">
            {site.id === 'dev'
              ? 'Stats for the isolated test MQTT feed. Updates every 30 minutes.'
              : site.id === 'ukmesh'
              ? 'Live analytics across all connected networks. Updates every 30 minutes.'
              : `Live analytics from the ${site.displayName} network. Updates every 30 minutes.`}
          </p>
          {lastUpdate && (
            <p style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>
              Last updated {lastUpdate.toLocaleTimeString()}
            </p>
          )}
        </div>
      </section>

      <div className="site-content">

        {loading && (
          <div style={{ padding: '80px 0', textAlign: 'center', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
            Loading stats…
          </div>
        )}

        {data && (
          <>
            {/* ── Summary row ─────────────────────────────────────────────── */}
            <div className="stats-page__summary">
              <StatCard label="Observed packets (24h)"     value={fmt(data.summary.totalPackets24h)} />
              <StatCard label="Observed packets (7D)"      value={fmt(data.summary.totalPackets7d)} />
              <StatCard label="Radios heard (24h)" value={fmt(data.summary.uniqueRadios24h)} color={C_GREEN} />
              <StatCard
                label="Peak hour"
                value={data.summary.peakHour ?? '—'}
                sub={data.summary.peakHour ? `${fmt(data.summary.peakHourCount)} packets` : undefined}
                color={C_AMBER}
              />
              <StatCard
                label="Active repeaters"
                value={fmt(data.summary.activeRepeaters)}
                sub="seen in last 7 days"
                color={C_GREEN}
              />
              <StatCard
                label="Stale repeaters"
                value={fmt(data.summary.staleRepeaters)}
                sub="not seen in 7 days"
                color={C_AMBER}
              />
            </div>

            {data.observerRegions.length > 0 && (
              <div className="stats-page__observer-section">
                <div className="stats-page__chart-header">
                  <span className="stats-page__chart-title">Observer regions</span>
                  <span className="stats-page__chart-sub">sorted by observed packets over the last 7 days</span>
                </div>
                <div className="stats-page__observer-grid">
                  {data.observerRegions.map((region) => (
                    <div key={region.iata} className="stats-page__observer-card">
                      <div className="stats-page__observer-card-head">
                        <span className="stats-page__observer-iata">{region.iata}</span>
                        <span className="stats-page__observer-last">last packet {timeAgo(region.lastPacketAt)}</span>
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
                          <strong>{fmt(region.observers)}</strong>
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
              </div>
            )}

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
                  <strong>{data.pathHashes.latestMultibyteAt ? timeAgo(data.pathHashes.latestMultibyteAt) : 'not seen yet'}</strong>
                </div>
              </div>
            </div>

            {/* ── Packets over time ────────────────────────────────────────── */}
            <div className="stats-page__row">
              <ChartCard title="Observed packets per hour" sub="last 24 hours">
                <ResponsiveContainer width="100%" height={220}>
                  <AreaChart data={data.packetsPerHour} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                    <defs>
                      <linearGradient id="gCyan" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%"  stopColor={C_CYAN} stopOpacity={0.3} />
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

            {/* ── Unique radios heard over time ────────────────────────────── */}
            <div className="stats-page__row">
              <ChartCard title="Unique radios heard per hour" sub="distinct transmitting nodes · last 24 hours">
                <ResponsiveContainer width="100%" height={220}>
                  <AreaChart data={data.radiosPerHour} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                    <defs>
                      <linearGradient id="gGreen" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%"  stopColor={C_GREEN} stopOpacity={0.3} />
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

            {/* ── Repeaters per day + packet types ─────────────────────────── */}
            <div className="stats-page__row">
              <ChartCard title="Active repeaters over the last 7 days" sub="total known repeater nodes in network">
                <ResponsiveContainer width="100%" height={220}>
                  <AreaChart data={data.repeatersPerDay} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                    <defs>
                      <linearGradient id="gAmber" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%"  stopColor={C_AMBER} stopOpacity={0.3} />
                        <stop offset="95%" stopColor={C_AMBER} stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid {...gridProps} />
                    <XAxis dataKey="hour" {...axisProps} interval={23} />
                    <YAxis {...axisProps} allowDecimals={false} />
                    <Tooltip content={<CustomTooltip />} />
                    <Area type="monotone" dataKey="count" name="Repeaters" stroke={C_AMBER} fill="url(#gAmber)" strokeWidth={2} dot={false} />
                  </AreaChart>
                </ResponsiveContainer>
              </ChartCard>

              <ChartCard title="Packet types" sub="last 24 hours · all observer hits">
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
              </ChartCard>
            </div>

            {/* ── Hop distribution + prefix collisions ─────────────────────── */}
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

              <ChartCard
                title="Repeated first-2-hex prefixes"
                sub="Top 10 by repeat count"
                tall
              >
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={data.prefixCollisions} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                    <CartesianGrid {...gridProps} />
                    <XAxis
                      dataKey="prefix"
                      {...axisProps}
                    />
                    <YAxis
                      {...axisProps}
                      allowDecimals={false}
                    />
                    <Tooltip
                      content={<CustomTooltip />}
                      formatter={(value: number) => [value, 'Repeats']}
                    />
                    <Bar dataKey="repeats" name="Repeats" fill={C_PURPLE} fillOpacity={0.8} radius={[3, 3, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>
            </div>

            {health && (
              <div className="stats-page__observer-section" style={{ marginBottom: 64 }}>
                <div className="stats-page__chart-header">
                  <span className="stats-page__chart-title">System Health</span>
                  <span className="stats-page__chart-sub">workers and host stats, refreshed every 5 minutes</span>
                </div>

                <p className="prose-note">
                  {health.ingest.stale_nodes < 1
                    ? 'All ingest nodes are active.'
                    : health.ingest.stale_nodes === 1
                      ? `1 ingest node has not injected for ${fmtInt(health.ingest.max_stale_minutes)} minutes.`
                      : `${fmtInt(health.ingest.stale_nodes)} ingest nodes have not injected for up to ${fmtInt(health.ingest.max_stale_minutes)} minutes.`}
                </p>

                <div className="health-workers-grid" style={{ marginBottom: 24 }}>
                  {health.workers.map((worker) => {
                    const statusClass = worker.status === 'running' ? 'health-pill health-pill--ok' : 'health-pill';
                    return (
                      <div key={worker.worker_name} className="site-card health-card">
                        <div className="health-card__head">
                          <h3 className="site-card__title">{workerLabel(worker.worker_name)}</h3>
                          <span className={statusClass}>{worker.status.toUpperCase()}</span>
                        </div>
                        <div className="health-card__stats">
                          <div className="health-kv"><span>Queue</span><strong>{worker.queue_depth}</strong></div>
                          <div className="health-kv"><span>Processed 1h</span><strong>{worker.processed_1h}</strong></div>
                          <div className="health-kv"><span>Last Activity</span><strong>{timeAgo(worker.last_activity_at)}</strong></div>
                        </div>
                      </div>
                    );
                  })}
                </div>

                <div className="site-stats-grid site-stats-grid--6 health-system-grid">
                  <div className="site-stat"><span className="site-stat__value">{fmtPct(health.system.cpu.usage_pct)}</span><span className="site-stat__label">CPU Usage</span></div>
                  <div className="site-stat"><span className="site-stat__value">{fmtPct(health.system.memory.used_pct)}</span><span className="site-stat__label">Memory Used</span></div>
                  <div className="site-stat"><span className="site-stat__value">{fmtPct(health.system.disk.used_pct)}</span><span className="site-stat__label">Disk Used</span></div>
                  <div className="site-stat"><span className="site-stat__value">{fmtInt(health.frontend_errors_1h)}</span><span className="site-stat__label">Frontend Errors (1h)</span></div>
                  <div className="site-stat"><span className="site-stat__value">{fmtInt(Math.floor(health.system.runtime.uptime_s / 3600))}h</span><span className="site-stat__label">Uptime</span></div>
                  <div className="site-stat"><span className="site-stat__value">{fmtInt(health.workers.reduce((sum, worker) => sum + worker.queue_depth, 0))}</span><span className="site-stat__label">Queued Jobs</span></div>
                </div>

                <div className="health-meta">
                  <div className="health-kv"><span>Updated</span><strong>{health.system.generated_at ? timeAgo(health.system.generated_at) : 'just now'}</strong></div>
                  <div className="health-kv"><span>Node Runtime</span><strong>{health.system.runtime.node_version}</strong></div>
                  <div className="health-kv"><span>Platform</span><strong>{health.system.runtime.platform} / {health.system.runtime.arch}</strong></div>
                  <div className="health-kv"><span>CPU 1m Load</span><strong>{Number(health.system.cpu.load_1m ?? 0).toFixed(2)} ({health.system.cpu.count} cores)</strong></div>
                  <div className="health-kv"><span>Memory</span><strong>{fmtInt(health.system.memory.used_mb)} / {fmtInt(health.system.memory.total_mb)} MB</strong></div>
                  <div className="health-kv"><span>Disk</span><strong>{fmtGb(health.system.disk.used_gb)} / {fmtGb(health.system.disk.total_gb)}</strong></div>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
};
