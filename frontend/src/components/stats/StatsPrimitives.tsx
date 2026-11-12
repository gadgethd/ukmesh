import { STATS_SECTION_FIELDS } from '../../contracts/publicData.generated.js';
import type { DecodedPathNode, StatsTabId } from '../../features/stats/model.js';
import React, { lazy, memo, Suspense } from 'react';
import { timeAgo as relativeTime } from '../../utils/time.js';

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

const STATS_TAB_LABELS: Record<StatsTabId, string> = {
  overview: 'Overview', traffic: 'Traffic', observers: 'Observers', paths: 'Paths', signal: 'Signal',
};
export const STATS_TABS = (Object.keys(STATS_SECTION_FIELDS) as StatsTabId[])
  .map(id => ({ id, label: STATS_TAB_LABELS[id] }));

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
  return relativeTime(timestamp, { empty: 'never' });
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
