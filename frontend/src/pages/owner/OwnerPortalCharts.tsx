import React, { useEffect, useMemo, useState } from 'react';
import { Area, AreaChart, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import {
  OWNER_AXIS_COLOR as AXIS_COLOR,
  OWNER_LABEL_COLOR as LABEL_COLOR,
  OWNER_TOOLTIP_BG as TIP_BG,
  OWNER_TOOLTIP_BORDER as TIP_BORDER,
  formatCompactTs,
  readExcludedLastHopSeries,
  writeExcludedLastHopSeries,
  type LastHopStrengthPoint,
  type OwnerLiveResponse,
} from './ownerPortalModel.js';
export const TrendBars: React.FC<{ points: Array<{ bucket: string; adverts: number }> }> = ({ points }) => {
  const max = Math.max(1, ...points.map((point) => point.adverts));
  return (
    <div className="owner-trend">
      <div className="owner-trend__bars" aria-label="Advert trend for the last 24 hours">
        {points.map((point) => {
          const height = Math.max(10, Math.round((point.adverts / max) * 100));
          return (
            <div
              key={point.bucket}
              className="owner-trend__bar"
              title={`${formatCompactTs(point.bucket)} · ${point.adverts} advert${point.adverts === 1 ? '' : 's'}`}
              style={{ height: `${height}%` }}
            />
          );
        })}
      </div>
      <p className="ui-visually-hidden">
        {points.length === 0
          ? 'No advert samples in the last 24 hours.'
          : `${points.length} advert buckets, ${points.reduce((sum, point) => sum + point.adverts, 0)} adverts total, latest bucket ${points[points.length - 1]!.adverts}.`}
      </p>
      <div className="owner-trend__meta">
        <span>24h advert trend</span>
        <strong>{points.reduce((sum, point) => sum + point.adverts, 0)}</strong>
      </div>
    </div>
  );
};

type TelemetryPoint = OwnerLiveResponse['telemetry24h'][number];

export const TELEMETRY_SERIES = [
  {
    key: 'batteryPct' as const,
    title: 'Battery',
    suffix: '%',
    stroke: '#6ddc7a',
    meta: (point: TelemetryPoint | null) => point?.batteryMv == null ? 'No data' : `${point.batteryMv} mV`,
  },
  {
    key: 'channelUtilPct' as const,
    title: 'Channel Utilization',
    suffix: '%',
    stroke: '#00c4ff',
    meta: () => 'Rolling from status samples',
  },
  {
    key: 'airUtilTxPct' as const,
    title: 'Air Util TX',
    suffix: '%',
    stroke: '#ff9f43',
    meta: () => 'Rolling TX air time',
  },
] as const;

export function formatUptime(seconds: number | null): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '—';
  const total = Math.floor(seconds);
  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

const OwnerTelemetryTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ value: number }>;
  label?: string;
  suffix: string;
}> = ({ active, payload, label, suffix }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: TIP_BG, border: `1px solid ${TIP_BORDER}`, borderRadius: 6, padding: '8px 12px', fontSize: 12 }}>
      {label ? <p style={{ color: LABEL_COLOR, margin: '0 0 4px' }}>{formatCompactTs(label)}</p> : null}
      <p style={{ color: '#e8f0fb', margin: 0 }}>
        <strong>{payload[0]?.value?.toFixed(1)}{suffix}</strong>
      </p>
    </div>
  );
};

const LAST_HOP_COLORS = ['#00c4ff', '#6ddc7a', '#ff9f43', '#f472b6', '#a78bfa', '#facc15'];
const MIN_LAST_HOP_SAMPLES = 10;

const LastHopStrengthTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ dataKey: string; value: number; color: string; payload: Record<string, unknown> }>;
  label?: string;
  seriesByKey: Map<string, { label: string; totalSamples: number; bucketCount: number }>;
}> = ({ active, payload, label, seriesByKey }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: TIP_BG, border: `1px solid ${TIP_BORDER}`, borderRadius: 6, padding: '8px 12px', fontSize: 12 }}>
      {label ? <p style={{ color: LABEL_COLOR, margin: '0 0 6px' }}>{formatCompactTs(label)}</p> : null}
      {payload.map((entry) => {
        const series = seriesByKey.get(entry.dataKey);
        const sampleCount = Number((entry.payload?.[`${entry.dataKey}__samples`] as number | undefined) ?? 0);
        return (
          <p key={entry.dataKey} style={{ color: '#e8f0fb', margin: '2px 0' }}>
            <strong style={{ color: entry.color }}>{series?.label ?? entry.dataKey}</strong>
            {`: ${Number(entry.value).toFixed(1)} dB`}
            {sampleCount > 0 ? ` (${sampleCount} sample${sampleCount === 1 ? '' : 's'})` : ''}
          </p>
        );
      })}
    </div>
  );
};

export const LastHopStrengthChart: React.FC<{ nodeId: string; points: LastHopStrengthPoint[]; isPassiveRepeater?: boolean }> = ({ nodeId, points, isPassiveRepeater }) => {
  const [selectedSeriesKey, setSelectedSeriesKey] = useState<string | null>(null);
  const [excludedSeriesKeys, setExcludedSeriesKeys] = useState<string[]>(() => readExcludedLastHopSeries(nodeId));
  const visiblePoints = useMemo(
    () => points.filter((point) => point.resolution !== 'unresolved'),
    [points],
  );
  const excludedSet = useMemo(() => new Set(excludedSeriesKeys), [excludedSeriesKeys]);

  useEffect(() => {
    setExcludedSeriesKeys(readExcludedLastHopSeries(nodeId));
    setSelectedSeriesKey(null);
  }, [nodeId]);

  const chartState = useMemo(() => {
    const totals = new Map<string, {
      key: string;
      label: string;
      totalSamples: number;
      bucketCount: number;
      buckets: Set<string>;
      resolution: LastHopStrengthPoint['resolution'];
    }>();
    for (const point of visiblePoints) {
      const key = point.lastHopNodeId ?? `unresolved:${point.lastHopName}`;
      const existing = totals.get(key);
      if (existing) {
        existing.totalSamples += point.sampleCount;
        existing.buckets.add(point.bucket);
        existing.bucketCount = existing.buckets.size;
      } else {
        totals.set(key, {
          key,
          label: point.lastHopName,
          totalSamples: point.sampleCount,
          bucketCount: 1,
          buckets: new Set([point.bucket]),
          resolution: point.resolution,
        });
      }
    }

    const allSeries = Array.from(totals.values())
      .filter((series) => series.totalSamples >= MIN_LAST_HOP_SAMPLES)
      .sort((a, b) => b.totalSamples - a.totalSamples || a.label.localeCompare(b.label));
    const includedSeries = allSeries.filter((series) => !excludedSet.has(series.key));
    const selected = selectedSeriesKey
      ? includedSeries.filter((series) => series.key === selectedSeriesKey)
      : includedSeries.slice(0, 6);
    const selectedKeys = new Set(selected.map((series) => series.key));
    const seriesByKey = new Map(allSeries.map((series) => [series.key, series] as const));

    const bucketed = new Map<string, Record<string, string | number | null>>();
    for (const point of visiblePoints) {
      const key = point.lastHopNodeId ?? `unresolved:${point.lastHopName}`;
      if (!selectedKeys.has(key)) continue;
      const entry = bucketed.get(point.bucket) ?? { bucket: point.bucket };
      entry[key] = point.avgSnr;
      entry[`${key}__samples`] = point.sampleCount;
      bucketed.set(point.bucket, entry);
    }

    return {
      allSeries,
      includedSeries,
      excludedCount: allSeries.length - includedSeries.length,
      series: selected,
      seriesByKey,
      data: Array.from(bucketed.values()).sort((a, b) => String(a.bucket).localeCompare(String(b.bucket))),
    };
  }, [excludedSet, selectedSeriesKey, visiblePoints]);

  useEffect(() => {
    if (selectedSeriesKey && !chartState.allSeries.some((series) => series.key === selectedSeriesKey)) {
      setSelectedSeriesKey(null);
    }
  }, [chartState.allSeries, selectedSeriesKey]);

  useEffect(() => {
    if (selectedSeriesKey && excludedSet.has(selectedSeriesKey)) {
      setSelectedSeriesKey(null);
    }
  }, [excludedSet, selectedSeriesKey]);

  useEffect(() => {
    writeExcludedLastHopSeries(nodeId, excludedSeriesKeys);
  }, [excludedSeriesKeys, nodeId]);

  const toggleSeriesExclusion = (seriesKey: string) => {
    setExcludedSeriesKeys((current) => (
      current.includes(seriesKey)
        ? current.filter((key) => key !== seriesKey)
        : [...current, seriesKey]
    ));
  };

  const hideSelectedSeries = () => {
    if (!selectedSeriesKey) return;
    toggleSeriesExclusion(selectedSeriesKey);
  };

  const clearExcludedSeries = () => {
    setExcludedSeriesKeys([]);
  };

  if (chartState.allSeries.length < 1) {
    return (
      <article className="owner-telemetry-metric">
        <div className="owner-panel__head owner-panel__head--compact">
          <div>
            <h3>RX Strength by Last Hop <span className="owner-telemetry-metric__hint">(Node names are predicted based on the first two hex characters in the path)</span></h3>
            <p>Average SNR over the last 7 days</p>
          </div>
        </div>
        <div
          className="owner-telemetry-metric__chart"
          role="img"
          aria-label="No last-hop signal series with at least ten samples are available"
        >
          <div className="owner-telemetry-metric__empty">
            {isPassiveRepeater
              ? 'This node does not report received packets via MQTT — rx signal data is captured by its companion node'
              : 'No last-hop repeaters with 10+ samples yet'}
          </div>
        </div>
        <div className="owner-telemetry-metric__footer">
          <span>Last 7d</span>
          <span>Minimum 10 samples</span>
        </div>
      </article>
    );
  }

  const latestBucket = chartState.data[chartState.data.length - 1];
  const latestActive = chartState.series
    .map((series) => {
      const value = latestBucket?.[series.key];
      return typeof value === 'number'
        ? `${series.label}: ${value.toFixed(1)} dB`
        : null;
    })
    .filter(Boolean)
    .slice(0, 2)
    .join(' · ');

  return (
      <article className="owner-telemetry-metric owner-telemetry-metric--wide owner-telemetry-metric--tall">
        <div className="owner-panel__head owner-panel__head--compact">
          <div>
            <h3>RX Strength by Last Hop <span className="owner-telemetry-metric__hint">(Node names are predicted based on the first two hex characters in the path)</span></h3>
            <p>{latestActive || (chartState.includedSeries.length > 0 ? 'Average SNR over the last 7 days' : 'All eligible repeaters are currently hidden')}</p>
          </div>
          <strong className="owner-telemetry-metric__value">{chartState.series.length}</strong>
        </div>
        <div
          className="owner-telemetry-metric__chart"
          role="img"
          aria-label={latestActive || `${chartState.series.length} last-hop signal series over seven days`}
        >
          {chartState.series.length > 0 && chartState.data.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartState.data} margin={{ top: 6, right: 10, left: 0, bottom: 0 }}>
                <XAxis dataKey="bucket" hide />
                <YAxis
                  width={28}
                  axisLine={{ stroke: AXIS_COLOR }}
                  tickLine={false}
                  tick={{ fill: LABEL_COLOR, fontSize: 11 }}
                  domain={['auto', 'auto']}
                />
                <Tooltip content={<LastHopStrengthTooltip seriesByKey={chartState.seriesByKey} />} />
                {chartState.series.map((series, index) => (
                  <Line
                    key={series.key}
                    type="monotone"
                    dataKey={series.key}
                    stroke={LAST_HOP_COLORS[index % LAST_HOP_COLORS.length]}
                    strokeWidth={2}
                    dot={false}
                    connectNulls
                    isAnimationActive={false}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="owner-telemetry-metric__empty">
              {chartState.includedSeries.length > 0 ? 'No chartable last-hop samples yet' : 'All repeaters are hidden. Show one below or reset exclusions.'}
            </div>
          )}
        </div>
        <div className="owner-telemetry-metric__footer">
          <span>Last 7d</span>
          <span>
            {selectedSeriesKey
              ? 'Focused on one repeater'
              : `${chartState.includedSeries.length} shown of ${chartState.allSeries.length}`}
            {chartState.excludedCount > 0 ? ` · ${chartState.excludedCount} hidden` : ''}
          </span>
        </div>
        <div className="owner-telemetry-metric__series-list">
          <button
            type="button"
            className={`owner-telemetry-metric__series-btn${selectedSeriesKey == null ? ' owner-telemetry-metric__series-btn--active' : ''}`}
            onClick={() => setSelectedSeriesKey(null)}
          >
            All
          </button>
          {chartState.excludedCount > 0 ? (
            <button
              type="button"
              className="owner-telemetry-metric__series-btn owner-telemetry-metric__series-btn--secondary"
              onClick={clearExcludedSeries}
            >
              Show Hidden
            </button>
          ) : null}
          {selectedSeriesKey ? (
            <button
              type="button"
              className="owner-telemetry-metric__series-btn owner-telemetry-metric__series-btn--secondary"
              onClick={hideSelectedSeries}
            >
              Hide Selected
            </button>
          ) : null}
          {chartState.includedSeries.map((series) => (
            <button
              key={series.key}
              type="button"
              className={`owner-telemetry-metric__series-btn${selectedSeriesKey === series.key ? ' owner-telemetry-metric__series-btn--active' : ''}`}
              onClick={() => setSelectedSeriesKey(series.key)}
              title={`${series.label} · ${series.bucketCount} hourly buckets · ${series.totalSamples} packets in last 7d`}
            >
              {series.label} ({series.totalSamples})
            </button>
          ))}
        </div>
      </article>
  );
};

export const TelemetryMiniChart: React.FC<{
  title: string;
  stroke: string;
  suffix: string;
  points: TelemetryPoint[];
  metric: keyof Pick<TelemetryPoint, 'batteryPct' | 'channelUtilPct' | 'airUtilTxPct'>;
  meta: (point: TelemetryPoint | null) => string;
}> = ({ title, stroke, suffix, points, metric, meta }) => {
  const chartData = points
    .map((point) => ({ bucket: point.bucket, value: point[metric] }))
    .filter((entry): entry is { bucket: string; value: number } => entry.value != null);
  const latest = points.length > 0 ? points[points.length - 1]! : null;
  const latestValue = latest?.[metric] ?? null;

  return (
    <article className="owner-telemetry-metric">
      <div className="owner-panel__head owner-panel__head--compact">
        <div>
          <h3>{title}</h3>
          <p>{meta(latest)}</p>
        </div>
        <strong className="owner-telemetry-metric__value">
          {latestValue == null ? '—' : `${latestValue.toFixed(1)}${suffix}`}
        </strong>
      </div>
      <div
        className="owner-telemetry-metric__chart"
        role="img"
        aria-label={latestValue == null
          ? `${title}: no telemetry`
          : `${title}: latest ${latestValue.toFixed(1)}${suffix}, ${chartData.length} samples over 24 hours`}
      >
        {chartData.length > 0 ? (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
              <XAxis dataKey="bucket" hide />
              <YAxis
                hide
                domain={[0, 100]}
                axisLine={{ stroke: AXIS_COLOR }}
                tickLine={false}
                tick={{ fill: LABEL_COLOR, fontSize: 11 }}
              />
              <Tooltip content={<OwnerTelemetryTooltip suffix={suffix} />} />
              <Area
                type="monotone"
                dataKey="value"
                stroke={stroke}
                fill={stroke}
                fillOpacity={0.18}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <div className="owner-telemetry-metric__empty">No telemetry yet</div>
        )}
      </div>
      <div className="owner-telemetry-metric__footer">
        <span>Last 24h</span>
        <span>{chartData.length} samples</span>
      </div>
    </article>
  );
};

export const TelemetryStatCard: React.FC<{
  title: string;
  value: string;
  meta: string;
}> = ({ title, value, meta }) => (
  <article className="owner-telemetry-metric owner-telemetry-metric--stat">
    <div className="owner-panel__head owner-panel__head--compact">
      <div>
        <h3>{title}</h3>
        <p>{meta}</p>
      </div>
    </div>
    <div className="owner-telemetry-metric__stat">
      <strong>{value}</strong>
    </div>
    <div className="owner-telemetry-metric__footer">
      <span>Last 24h</span>
      <span>Latest sample</span>
    </div>
  </article>
);
