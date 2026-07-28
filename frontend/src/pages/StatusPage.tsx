import React, { useEffect, useState } from 'react';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import './network-intelligence.css';

type HealthPayload = {
  status: 'healthy' | 'degraded' | 'critical';
  problems: Array<{ code: string; severity: 'warning' | 'critical'; message: string }>;
  maintenance: { active: boolean; message: string | null };
  ingest: { active_nodes: number; stale_nodes: number; global_last_packet_at: string | null; packet_age_minutes: number | null };
  operational_checks: Array<{ check_name: string; status: string; latency_ms: number; detail: string | null; ts: string }>;
  database: { size_bytes: number; dead_rows: number; oldest_vacuum_at: string | null; tables_needing_vacuum: number; connection_count: number; max_connections: number; cache_hit_ratio: number };
  workers: Array<{ worker_name: string; status: string; queue_depth: number; processed_1h?: number; last_activity_at: string | null }>;
};
type FirmwarePayload = { total: number; versions: Array<{ hardware_model: string; firmware_version: string; count: number }> };

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return 'Unknown';
  return `${(value / 1_073_741_824).toFixed(1)} GB`;
}

export const StatusPage: React.FC = () => {
  const [data, setData] = useState<HealthPayload | null>(null);
  const [error, setError] = useState(false);
  const [firmware, setFirmware] = useState<FirmwarePayload | null>(null);
  useEffect(() => {
    let active = true;
    const load = () => fetch('/api/health', { cache: 'no-store' })
      .then((response) => response.ok ? response.json() as Promise<HealthPayload> : Promise.reject(new Error('health unavailable')))
      .then((next) => { if (active) { setData(next); setError(false); } })
      .catch(() => { if (active) setError(true); });
    void load();
    const timer = window.setInterval(() => void load(), 60_000);
    return () => { active = false; window.clearInterval(timer); };
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    fetch('/api/repeaters/firmware', { signal: controller.signal })
      .then((response) => response.ok ? response.json() as Promise<FirmwarePayload> : null)
      .then(setFirmware)
      .catch(() => {});
    return () => controller.abort();
  }, []);

  return (
    <div className="status-page site-content">
      <header><p className="topology-page__eyebrow">Public operations</p><h1>Platform status</h1><p>Service health, ingest freshness, and worker state without exposing private infrastructure details.</p></header>
      {!data && !error && <LoadingIndicator label="Checking platform status…" variant="block" />}
      {error && !data && <div className="status-page__banner status-page__banner--critical" role="alert">Status data is currently unavailable.</div>}
      {data && (
        <>
          <div className={`status-page__banner status-page__banner--${data.status}`} role="status">
            <strong>{data.status === 'healthy' ? 'All monitored systems operational' : `Platform ${data.status}`}</strong>
            <span>Updated {new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>
          </div>
          {data.maintenance.active && <div className="status-page__maintenance"><strong>Planned maintenance</strong><span>{data.maintenance.message ?? 'Maintenance is currently in progress.'}</span></div>}
          {data.problems.length > 0 && <section className="status-page__problems"><h2>Current notices</h2>{data.problems.map((problem) => <article key={`${problem.code}:${problem.message}`} className={`status-page__problem status-page__problem--${problem.severity}`}><strong>{problem.code.replace(/_/g, ' ')}</strong><span>{problem.message}</span></article>)}</section>}
          <div className="status-page__grid">
            <section><h2>Public ingest</h2><dl><div><dt>Active observer nodes</dt><dd>{data.ingest.active_nodes}</dd></div><div><dt>Stale observers</dt><dd>{data.ingest.stale_nodes}</dd></div><div><dt>Latest packet age</dt><dd>{data.ingest.packet_age_minutes == null ? 'Unknown' : `${data.ingest.packet_age_minutes} min`}</dd></div></dl></section>
            <section><h2>Synthetic journeys</h2><div className="status-page__checks">{data.operational_checks.length === 0 ? <p>Monitoring is starting.</p> : data.operational_checks.map((check) => <div key={check.check_name}><span className={`status-page__dot status-page__dot--${check.status}`} /><strong>{check.check_name.replace(/_/g, ' ')}</strong><small>{check.latency_ms} ms</small></div>)}</div></section>
            <section><h2>Background workers</h2><div className="status-page__checks">{data.workers.map((worker) => <div key={worker.worker_name}><span className={`status-page__dot status-page__dot--${worker.status === 'running' || worker.status === 'completed' || worker.status === 'idle' ? 'ok' : worker.status}`} /><strong>{worker.worker_name}</strong><small>{worker.queue_depth} queued · {worker.processed_1h ?? 0}/h · {worker.last_activity_at ? `seen ${Math.max(0, Math.floor((Date.now() - Date.parse(worker.last_activity_at)) / 60_000))}m ago` : 'no activity'}</small></div>)}</div></section>
            <section><h2>Database</h2><dl><div><dt>Disk footprint</dt><dd>{formatBytes(data.database.size_bytes)}</dd></div><div><dt>Connections</dt><dd>{data.database.connection_count}/{data.database.max_connections}</dd></div><div><dt>Cache hit ratio</dt><dd>{data.database.cache_hit_ratio.toFixed(2)}%</dd></div><div><dt>Vacuum attention</dt><dd>{data.database.tables_needing_vacuum} tables</dd></div></dl></section>
          </div>
          <section className="status-page__firmware">
            <h2>Repeater firmware distribution</h2>
            {firmware && firmware.versions.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={firmware.versions.slice(0, 16)} margin={{ left: 8, right: 8, bottom: 70 }}>
                  <CartesianGrid stroke="rgba(255,255,255,.1)" />
                  <XAxis dataKey="firmware_version" angle={-35} textAnchor="end" interval={0} height={80} />
                  <YAxis allowDecimals={false} />
                  <Tooltip formatter={(value: number, _name, item) => [value, item.payload.hardware_model]} />
                  <Bar dataKey="count" fill="var(--color-primary)" />
                </BarChart>
              </ResponsiveContainer>
            ) : <p>Firmware telemetry is not yet available.</p>}
          </section>
          <p className="status-page__privacy">Status values are deliberately aggregated. Hostnames, addresses, credentials, and private node identities are never included.</p>
        </>
      )}
    </div>
  );
};
