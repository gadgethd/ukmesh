import { memo, useEffect, useState } from 'react';
import { Line, LineChart, ResponsiveContainer, Tooltip, YAxis } from 'recharts';

type Point = { time: string; snr: number | null; path_loss: number | null };

export const LinkQualitySparkline = memo(function LinkQualitySparkline({
  source,
  target,
}: {
  source: string;
  target: string;
}) {
  const [points, setPoints] = useState<Point[]>([]);
  useEffect(() => {
    const controller = new AbortController();
    fetch(`/api/links/${encodeURIComponent(`${source}:${target}`)}/history?hours=72`, { signal: controller.signal })
      .then((response) => response.ok ? response.json() as Promise<{ points?: Point[] }> : { points: [] })
      .then((value) => setPoints(value.points ?? []))
      .catch(() => {});
    return () => controller.abort();
  }, [source, target]);
  if (points.length === 0) return <span className="node-popup__muted">No 72h radio samples</span>;
  return (
    <div className="node-popup__link-spark" aria-label="72 hour link quality">
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
