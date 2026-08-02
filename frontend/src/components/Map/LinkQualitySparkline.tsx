import { memo, useEffect, useState } from 'react';
import { Line, LineChart, ResponsiveContainer, Tooltip, YAxis } from 'recharts';
import { useVisibilityPoll } from '../../hooks/useVisibilityPoll.js';
import { fetchJson, withScopeParams } from '../../utils/api.js';
import { canonicalNodeId } from '../../utils/nodeIds.js';
import { RequestLimiter } from '../../utils/requestLimiter.js';
import { ScopedCache } from '../../utils/scopedCache.js';

type Point = { time: string; snr: number | null; path_loss: number | null };
type LinkHistoryResponse = { points: Point[] };

// At most two sparkline requests run at once across the page; sixteen may wait.
// Results are scoped by network/observer/privacy generation, live for five
// minutes, and are bounded to 256 links or 8 MiB.
const sparklineRequestLimiter = new RequestLimiter(2, 16);
const sparklineCache = new ScopedCache<Point[]>({
  name: 'link-quality-sparklines',
  ttlMs: 5 * 60_000,
  maxEntries: 256,
  maxBytes: 8 * 1024 * 1024,
  maxInflight: 18,
});

function isLinkHistoryResponse(value: unknown): value is LinkHistoryResponse {
  if (typeof value !== 'object' || value === null || !Array.isArray((value as { points?: unknown }).points)) {
    return false;
  }
  return (value as { points: unknown[] }).points.every((point) => (
    typeof point === 'object'
    && point !== null
    && typeof (point as Record<string, unknown>)['time'] === 'string'
  ));
}

export const LinkQualitySparkline = memo(function LinkQualitySparkline({
  source,
  target,
  network,
  observer,
  privacyGeneration,
}: {
  source: string;
  target: string;
  network?: string;
  observer?: string;
  privacyGeneration: number;
}) {
  const [points, setPoints] = useState<Point[]>([]);

  const pair = [canonicalNodeId(source), canonicalNodeId(target)].sort().join(':');
  const cacheScope = `${network ?? 'all'}|${observer ?? 'all'}|privacy-${privacyGeneration}`;

  useEffect(() => {
    setPoints(sparklineCache.get(cacheScope, pair) ?? []);
  }, [cacheScope, pair]);

  useVisibilityPoll(async (signal) => {
    const next = await sparklineCache.getOrLoad(cacheScope, pair, () => (
      sparklineRequestLimiter.run(signal, async () => {
        const value = await fetchJson<LinkHistoryResponse>(
          withScopeParams(`/api/links/${encodeURIComponent(pair)}/history?hours=72`, {
            network,
            observer,
          }),
          { signal, cache: 'no-store' },
          {
            timeoutMs: 15_000,
            maxBytes: 512 * 1024,
            validate: isLinkHistoryResponse,
          },
        );
        return value.points.slice(-288);
      })
    ));
    if (!signal.aborted) setPoints(next);
  }, {
    scopeKey: `link-sparkline:${cacheScope}:${pair}`,
    intervalMs: 5 * 60_000,
    timeoutMs: 15_000,
  });

  if (points.length === 0) return <span className="node-popup__muted">No 72h radio samples</span>;
  const latest = points[points.length - 1];
  const summary = [
    `${points.length} radio samples over 72 hours`,
    latest?.snr == null ? null : `latest SNR ${latest.snr.toFixed(1)} decibels`,
    latest?.path_loss == null ? null : `estimated path loss ${latest.path_loss.toFixed(1)} decibels`,
  ].filter(Boolean).join(', ');
  return (
    <div className="node-popup__link-spark" role="img" aria-label={summary}>
      <ResponsiveContainer width="100%" height={70}>
        <LineChart data={points}>
          <YAxis hide domain={['dataMin - 2', 'dataMax + 2']} />
          <Tooltip labelFormatter={(value) => new Date(String(value)).toLocaleString()} />
          <Line type="monotone" dataKey="snr" stroke="var(--color-green)" strokeWidth={2} dot={false} connectNulls />
          <Line type="monotone" dataKey="path_loss" stroke="var(--color-primary)" strokeWidth={1.5} dot={false} connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
});
