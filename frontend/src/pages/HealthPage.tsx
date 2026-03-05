import React, { useEffect, useMemo, useState } from 'react';

type HealthPayload = {
  system: {
    generated_at: string;
    cpu: { load_1m: number; count: number; load_pct: number };
    memory: { total_mb: number; used_mb: number; used_pct: number };
    disk: { total_gb: number; used_gb: number; used_pct: number };
    runtime: { uptime_s: number; node_version: string; platform: string; arch: string };
  };
  workers: Array<{
    worker_name: string;
    status: string;
    queue_depth: number;
    processed_5m: number;
    last_activity_at: string | null;
  }>;
  history: Array<{
    ts: string;
    worker_name: string;
    status: string;
    queue_depth: number;
    processed_5m: number;
    cpu_load_1m: number | null;
    mem_used_pct: number | null;
    disk_used_pct: number | null;
  }>;
  frontend_errors_1h: number;
};

function timeAgo(ts: string | null): string {
  if (!ts) return 'never';
  const diff = Math.max(0, Date.now() - Date.parse(ts));
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

function workerLabel(name: string): string {
  if (name === 'viewshed-worker') return 'Viewshed Worker';
  if (name === 'link-worker') return 'Link Worker';
  if (name === 'path-learning') return 'Path Learning';
  return name;
}

export const HealthPage: React.FC = () => {
  const [data, setData] = useState<HealthPayload | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const load = () => {
      fetch('/api/health', { cache: 'no-store' })
        .then((r) => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return r.json();
        })
        .then((json: HealthPayload) => {
          if (cancelled) return;
          setData(json);
          setError(null);
        })
        .catch((err: Error) => {
          if (cancelled) return;
          setError(err.message);
        });
    };

    load();
    const timer = setInterval(load, 15_000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  const historyByWorker = useMemo(() => {
    const grouped = new Map<string, HealthPayload['history']>();
    for (const row of data?.history ?? []) {
      const list = grouped.get(row.worker_name) ?? [];
      list.push(row);
      grouped.set(row.worker_name, list);
    }
    return grouped;
  }, [data]);

  return (
    <>
      <section className="site-page-hero">
        <div className="site-content">
          <h1 className="site-page-hero__title">Network Health</h1>
          <p className="site-page-hero__sub">
            Live worker status, queue depth, and host resource usage.
          </p>
        </div>
      </section>

      <div className="site-content site-prose">
        {error && <p className="prose-note">Health API error: {error}</p>}

        <section className="prose-section">
          <h2>Workers</h2>
          <div className="health-workers-grid">
            {(data?.workers ?? []).map((worker) => {
              const statusClass = worker.status === 'running' ? 'health-pill health-pill--ok' : 'health-pill';
              const hist = historyByWorker.get(worker.worker_name) ?? [];
              const peakQueue = Math.max(1, ...hist.map((h) => h.queue_depth));
              const queueBars = hist.slice(0, 48).reverse();

              return (
                <div key={worker.worker_name} className="site-card health-card">
                  <div className="health-card__head">
                    <h3 className="site-card__title">{workerLabel(worker.worker_name)}</h3>
                    <span className={statusClass}>{worker.status.toUpperCase()}</span>
                  </div>
                  <div className="health-card__stats">
                    <div className="health-kv"><span>Queue</span><strong>{worker.queue_depth}</strong></div>
                    <div className="health-kv"><span>Processed 5m</span><strong>{worker.processed_5m}</strong></div>
                    <div className="health-kv"><span>Last Activity</span><strong>{timeAgo(worker.last_activity_at)}</strong></div>
                  </div>
                  <div className="health-spark" aria-label="Recent queue depth">
                    {queueBars.map((row, idx) => (
                      <span
                        key={`${worker.worker_name}-${idx}`}
                        className="health-spark__bar"
                        style={{ height: `${Math.max(8, (row.queue_depth / peakQueue) * 100)}%` }}
                        title={`${new Date(row.ts).toLocaleTimeString()} queue=${row.queue_depth}`}
                      />
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </section>

        <section className="prose-section">
          <h2>Server Stats</h2>
          <div className="site-stats-grid site-stats-grid--6">
            <div className="site-stat"><span className="site-stat__value">{data?.system.cpu.load_pct?.toFixed(1) ?? '0'}%</span><span className="site-stat__label">CPU Load</span></div>
            <div className="site-stat"><span className="site-stat__value">{data?.system.memory.used_pct?.toFixed(1) ?? '0'}%</span><span className="site-stat__label">Memory Used</span></div>
            <div className="site-stat"><span className="site-stat__value">{data?.system.disk.used_pct?.toFixed(1) ?? '0'}%</span><span className="site-stat__label">Disk Used</span></div>
            <div className="site-stat"><span className="site-stat__value">{data?.frontend_errors_1h ?? 0}</span><span className="site-stat__label">Frontend Errors (1h)</span></div>
            <div className="site-stat"><span className="site-stat__value">{data?.system.runtime.node_version ?? '-'}</span><span className="site-stat__label">Node Runtime</span></div>
            <div className="site-stat"><span className="site-stat__value">{data?.system.runtime.platform ?? '-'}/{data?.system.runtime.arch ?? '-'}</span><span className="site-stat__label">Platform</span></div>
          </div>
          <p className="prose-note">
            Updated {data?.system.generated_at ? timeAgo(data.system.generated_at) : 'just now'}.
            Uptime: {data ? Math.floor(data.system.runtime.uptime_s / 3600) : 0}h.
            Memory: {data?.system.memory.used_mb ?? 0} / {data?.system.memory.total_mb ?? 0} MB.
            Disk: {data?.system.disk.used_gb ?? 0} / {data?.system.disk.total_gb ?? 0} GB.
          </p>
        </section>
      </div>
    </>
  );
};
