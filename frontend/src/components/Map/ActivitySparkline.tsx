import React, { useEffect, useMemo, useState } from 'react';
import { withScopeParams } from '../../utils/api.js';
import { getCurrentSite } from '../../config/site.js';

type HistoryRow = { time: string };

type Props = {
  nodeId: string;
  hours?: number;
};

/** Tiny 24h activity sparkline from /api/nodes/:id/history. */
export const ActivitySparkline: React.FC<Props> = ({ nodeId, hours = 24 }) => {
  const [buckets, setBuckets] = useState<number[] | null>(null);
  const [error, setError] = useState(false);
  const site = getCurrentSite();

  useEffect(() => {
    const controller = new AbortController();
    setBuckets(null);
    setError(false);
    const endpoint = withScopeParams(
      `/api/nodes/${encodeURIComponent(nodeId)}/history?hours=${hours}`,
      { network: site.networkFilter, observer: site.observerId },
    );
    void fetch(endpoint, { signal: controller.signal })
      .then((res) => (res.ok ? (res.json() as Promise<HistoryRow[]>) : Promise.reject(new Error('bad'))))
      .then((rows) => {
        const counts = new Array(hours).fill(0) as number[];
        const now = Date.now();
        const windowMs = hours * 3_600_000;
        const bucketMs = windowMs / hours;
        for (const row of Array.isArray(rows) ? rows : []) {
          const t = Date.parse(row.time);
          if (!Number.isFinite(t)) continue;
          const age = now - t;
          if (age < 0 || age > windowMs) continue;
          // oldest bucket = 0, newest = hours-1
          const idx = Math.min(hours - 1, Math.max(0, hours - 1 - Math.floor(age / bucketMs)));
          counts[idx] += 1;
        }
        setBuckets(counts);
      })
      .catch((err: unknown) => {
        if ((err as DOMException).name !== 'AbortError') {
          setError(true);
          setBuckets([]);
        }
      });
    return () => controller.abort();
  }, [hours, nodeId, site.networkFilter, site.observerId]);

  const { points, max, total } = useMemo(() => {
    if (!buckets || buckets.length === 0) {
      return { points: '', max: 0, total: 0 };
    }
    const maxVal = Math.max(...buckets, 1);
    const w = 120;
    const h = 28;
    const step = w / Math.max(buckets.length - 1, 1);
    const pts = buckets.map((v, i) => {
      const x = i * step;
      const y = h - (v / maxVal) * (h - 2) - 1;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    });
    return {
      points: pts.join(' '),
      max: Math.max(...buckets),
      total: buckets.reduce((a, b) => a + b, 0),
    };
  }, [buckets]);

  if (buckets === null) {
    return (
      <div className="node-dock__spark" aria-busy="true">
        <span className="node-dock__spark-label">24h activity</span>
        <span className="node-dock__spark-empty">Loading…</span>
      </div>
    );
  }

  if (error || total === 0) {
    return (
      <div className="node-dock__spark">
        <span className="node-dock__spark-label">24h activity</span>
        <span className="node-dock__spark-empty">{error ? 'Unavailable' : 'No samples'}</span>
      </div>
    );
  }

  return (
    <div className="node-dock__spark" title={`${total} packets in last ${hours}h (peak ${max}/h)`}>
      <div className="node-dock__spark-head">
        <span className="node-dock__spark-label">24h activity</span>
        <span className="node-dock__spark-total">{total.toLocaleString()}</span>
      </div>
      <svg
        className="node-dock__spark-svg"
        viewBox="0 0 120 28"
        width="100%"
        height="28"
        role="img"
        aria-label={`${total} packets over the last ${hours} hours`}
      >
        <polyline
          fill="none"
          stroke="currentColor"
          strokeWidth="1.75"
          strokeLinejoin="round"
          strokeLinecap="round"
          points={points}
        />
      </svg>
    </div>
  );
};
