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
  topChatters:     { name: string;  count: number }[];
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
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const site = getCurrentSite();
  const statsNetwork = site.id === 'ukmesh' ? undefined : site.network;

  const load = () => {
    fetch(uncachedEndpoint(chartStatsEndpoint(statsNetwork)), { cache: 'no-store' })
      .then(r => r.json())
      .then((d: ChartData) => { setData(d); setLoading(false); setLastUpdate(new Date()); })
      .catch(() => setLoading(false));
  };

  useEffect(() => {
    load();
    const t = setInterval(load, 60_000);
    return () => clearInterval(t);
  }, []);

  const fmt = (n: number) => n.toLocaleString();

  return (
    <div className="site-layout__inner">
      {/* ── Page hero ─────────────────────────────────────────────────────── */}
      <section className="site-page-hero">
        <div className="site-content">
          <h1 className="site-page-hero__title">Network Stats</h1>
          <p className="site-page-hero__sub">
            {site.id === 'ukmesh'
              ? 'Live analytics across all connected networks. Updates every 60 seconds.'
              : `Live analytics from the ${site.displayName} network. Updates every 60 seconds.`}
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
              <StatCard label="Packets (24h)"     value={fmt(data.summary.totalPackets24h)} />
              <StatCard label="Packets (7 days)"  value={fmt(data.summary.totalPackets7d)} />
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

            {/* ── Packets over time ────────────────────────────────────────── */}
            <div className="stats-page__row">
              <ChartCard title="Packets per hour" sub="last 24 hours">
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

              <ChartCard title="Packets per day" sub="last 7 days">
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

              <ChartCard title="Packet types" sub="last 24 hours">
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

            {/* ── Hop distribution + top nodes ─────────────────────────────── */}
            <div className="stats-page__row" style={{ marginBottom: 64 }}>
              <ChartCard title="Hop count distribution" sub="last 7 days">
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

              <ChartCard title="Most active chatters" sub="public channel · by message count · last 7 days" tall>
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart
                    data={[...data.topChatters].reverse()}
                    layout="vertical"
                    margin={{ top: 4, right: 16, left: 4, bottom: 0 }}
                  >
                    <CartesianGrid {...gridProps} horizontal={false} />
                    <XAxis type="number" {...axisProps} />
                    <YAxis type="category" dataKey="name" {...axisProps} width={110} tick={{ fill: LABEL_COLOR, fontSize: 10 }} />
                    <Tooltip content={<CustomTooltip />} />
                    <Bar dataKey="count" name="Messages" fill={C_PURPLE} fillOpacity={0.75} radius={[0, 3, 3, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>
            </div>
          </>
        )}
      </div>
    </div>
  );
};
