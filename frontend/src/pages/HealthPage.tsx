import React, { useEffect, useState } from 'react';

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
    processed_1h: number;
    last_activity_at: string | null;
  }>;
  history: Array<{
    ts: string;
    worker_name: string;
    status: string;
    queue_depth: number;
    processed_1h: number;
    cpu_load_1m: number | null;
    mem_used_pct: number | null;
    disk_used_pct: number | null;
  }>;
  frontend_errors_1h: number;
  ingest: {
    stale_nodes: number;
    active_nodes: number;
    max_stale_minutes: number;
    stale_threshold_minutes: number;
    global_last_packet_at: string | null;
  };
  path_hashes: {
    last_24h_hops: {
      one_byte: number;
      two_byte: number;
      three_byte: number;
    };
    multibyte_packets_24h: number;
    latest_multibyte_at: string | null;
  };
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
  if (name === 'health-worker') return 'Health Worker';
  if (name === 'link-backfill-worker') return 'Link Backfill Worker';
  if (name === 'path-sim-worker') return 'Path Simulation Worker';
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

export const HealthPage: React.FC = () => {
  const [data, setData] = useState<HealthPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expandedWorker, setExpandedWorker] = useState<string | null>(null);

  const workerDescriptions: Record<string, string> = {
    'viewshed-worker': 'Computes terrain visibility polygons for repeater nodes using elevation data so map coverage can be pre-rendered.',
    'link-worker': 'Processes observed packet relay paths, resolves node-to-node links, and updates link viability/path-loss metrics.',
    'path-learning': 'Rebuilds the beta path-learning priors from historical packet behavior so route predictions stay current.',
    'health-worker': 'Captures periodic worker/system health snapshots used by this Health page for live and historical status.',
    'link-backfill-worker': 'One-shot startup worker that backfills historical link observations when link tables are empty.',
    'path-sim-worker': 'Replays historical packets through beta/red continuation logic and stores aggregate resolution/permutation diagnostics.',
  };

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
        {!error && data ? (
          <p className="prose-note">
            {data.ingest.stale_nodes < 1
              ? 'All ingest nodes are active.'
              : data.ingest.stale_nodes === 1
                ? `1 ingest node has not injected for ${fmtInt(data.ingest.max_stale_minutes)} minutes.`
                : `${fmtInt(data.ingest.stale_nodes)} ingest nodes have not injected for up to ${fmtInt(data.ingest.max_stale_minutes)} minutes.`}
          </p>
        ) : null}

        <section className="prose-section">
          <h2>Workers</h2>
          <div className="health-workers-grid">
            {(data?.workers ?? []).map((worker) => {
              const statusClass = worker.status === 'running' ? 'health-pill health-pill--ok' : 'health-pill';
              const isExpanded = expandedWorker === worker.worker_name;
              const description = workerDescriptions[worker.worker_name] ?? 'No description available for this worker.';

              return (
                <div
                  key={worker.worker_name}
                  className={`site-card health-card health-card--interactive${isExpanded ? ' health-card--expanded' : ''}`}
                  role="button"
                  tabIndex={0}
                  aria-expanded={isExpanded}
                  onClick={() => setExpandedWorker((curr) => curr === worker.worker_name ? null : worker.worker_name)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      setExpandedWorker((curr) => curr === worker.worker_name ? null : worker.worker_name);
                    }
                  }}
                >
                  <div className="health-card__head">
                    <h3 className="site-card__title">{workerLabel(worker.worker_name)}</h3>
                    <span className={statusClass}>{worker.status.toUpperCase()}</span>
                  </div>
                  <div className="health-card__stats">
                    <div className="health-kv"><span>Queue</span><strong>{worker.queue_depth}</strong></div>
                    <div className="health-kv"><span>Processed 1h</span><strong>{worker.processed_1h}</strong></div>
                    <div className="health-kv"><span>Last Activity</span><strong>{timeAgo(worker.last_activity_at)}</strong></div>
                  </div>
                  {isExpanded && (
                    <p className="health-card__desc">{description}</p>
                  )}
                </div>
              );
            })}
          </div>
        </section>

        <section className="prose-section">
          <h2>Server Stats</h2>
          <div className="site-stats-grid site-stats-grid--6 health-system-grid">
            <div className="site-stat"><span className="site-stat__value">{fmtPct(data?.system.cpu.load_pct)}</span><span className="site-stat__label">CPU Load</span></div>
            <div className="site-stat"><span className="site-stat__value">{fmtPct(data?.system.memory.used_pct)}</span><span className="site-stat__label">Memory Used</span></div>
            <div className="site-stat"><span className="site-stat__value">{fmtPct(data?.system.disk.used_pct)}</span><span className="site-stat__label">Disk Used</span></div>
            <div className="site-stat"><span className="site-stat__value">{fmtInt(data?.frontend_errors_1h)}</span><span className="site-stat__label">Frontend Errors (1h)</span></div>
            <div className="site-stat"><span className="site-stat__value">{fmtInt(data ? Math.floor(data.system.runtime.uptime_s / 3600) : 0)}h</span><span className="site-stat__label">Uptime</span></div>
            <div className="site-stat"><span className="site-stat__value">{fmtInt((data?.workers ?? []).reduce((sum, w) => sum + w.queue_depth, 0))}</span><span className="site-stat__label">Queued Jobs</span></div>
          </div>
          <div className="health-meta">
            <div className="health-kv"><span>Updated</span><strong>{data?.system.generated_at ? timeAgo(data.system.generated_at) : 'just now'}</strong></div>
            <div className="health-kv"><span>Node Runtime</span><strong>{data?.system.runtime.node_version ?? '-'}</strong></div>
            <div className="health-kv"><span>Platform</span><strong>{data?.system.runtime.platform ?? '-'} / {data?.system.runtime.arch ?? '-'}</strong></div>
            <div className="health-kv"><span>CPU 1m Load</span><strong>{Number(data?.system.cpu.load_1m ?? 0).toFixed(2)} ({data?.system.cpu.count ?? 0} cores)</strong></div>
            <div className="health-kv"><span>Memory</span><strong>{fmtInt(data?.system.memory.used_mb)} / {fmtInt(data?.system.memory.total_mb)} MB</strong></div>
            <div className="health-kv"><span>Disk</span><strong>{fmtGb(data?.system.disk.used_gb)} / {fmtGb(data?.system.disk.total_gb)}</strong></div>
          </div>
        </section>

        <section className="prose-section">
          <h2>Path Hashes</h2>
          <div className="site-stats-grid site-stats-grid--4 health-system-grid">
            <div className="site-stat">
              <span className="site-stat__value">{fmtInt(data?.path_hashes.last_24h_hops.one_byte)}</span>
              <span className="site-stat__label">1-byte Hops (24h)</span>
            </div>
            <div className="site-stat">
              <span className="site-stat__value">{fmtInt(data?.path_hashes.last_24h_hops.two_byte)}</span>
              <span className="site-stat__label">2-byte Hops (24h)</span>
            </div>
            <div className="site-stat">
              <span className="site-stat__value">{fmtInt(data?.path_hashes.last_24h_hops.three_byte)}</span>
              <span className="site-stat__label">3-byte Hops (24h)</span>
            </div>
            <div className="site-stat">
              <span className="site-stat__value">{fmtInt(data?.path_hashes.multibyte_packets_24h)}</span>
              <span className="site-stat__label">Multibyte Packets (24h)</span>
            </div>
          </div>
          <div className="health-meta">
            <div className="health-kv">
              <span>Latest Multibyte Packet</span>
              <strong>{data?.path_hashes.latest_multibyte_at ? timeAgo(data.path_hashes.latest_multibyte_at) : 'not seen yet'}</strong>
            </div>
          </div>
        </section>
      </div>
    </>
  );
};
