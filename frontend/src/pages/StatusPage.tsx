import React, { useEffect, useState } from 'react';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { getCurrentSite } from '../config/site.js';
import { useVisibilityPoll } from '../hooks/useVisibilityPoll.js';
import { fetchJson, withScopeParams } from '../utils/api.js';
import './network-intelligence.css';

type PublicHealthPayload = {
  status: 'healthy' | 'degraded' | 'critical';
  generatedAt: string;
  maintenance: { active: boolean; message: string | null };
  incidents: Array<{ code: string; severity: 'warning' | 'critical' }>;
  components: {
    ingest: { status: string };
    workers: { status: string };
    storage: { status: string };
  };
};
type FirmwarePayload = { total: number; versions: Array<{ hardware_model: string; firmware_version: string; count: number }> };
type FirmwareDistributionRow = {
  firmware_version: string;
  count: number;
  hardware_models: string[];
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function hasStringStatus(value: unknown): value is { status: string } {
  return isRecord(value) && typeof value['status'] === 'string';
}

function isPublicHealthPayload(value: unknown): value is PublicHealthPayload {
  if (!isRecord(value) || !isRecord(value['maintenance']) || !isRecord(value['components'])) return false;
  const components = value['components'];
  return isRecord(value)
    && (value['status'] === 'healthy' || value['status'] === 'degraded' || value['status'] === 'critical')
    && typeof value['generatedAt'] === 'string'
    && typeof value['maintenance']['active'] === 'boolean'
    && (typeof value['maintenance']['message'] === 'string' || value['maintenance']['message'] === null)
    && Array.isArray(value['incidents'])
    && value['incidents'].every((incident) => (
      isRecord(incident)
      && typeof incident['code'] === 'string'
      && (incident['severity'] === 'warning' || incident['severity'] === 'critical')
    ))
    && hasStringStatus(components['ingest'])
    && hasStringStatus(components['workers'])
    && hasStringStatus(components['storage']);
}

function isFirmwarePayload(value: unknown): value is FirmwarePayload {
  return isRecord(value)
    && typeof value['total'] === 'number'
    && Array.isArray(value['versions']);
}

function aggregateFirmwareVersions(versions: FirmwarePayload['versions']): FirmwareDistributionRow[] {
  const byVersion = new Map<string, FirmwareDistributionRow>();
  for (const row of versions) {
    const firmwareVersion = row.firmware_version || 'Unknown';
    const existing = byVersion.get(firmwareVersion);
    if (existing) {
      existing.count += row.count;
      if (!existing.hardware_models.includes(row.hardware_model)) {
        existing.hardware_models.push(row.hardware_model);
      }
      continue;
    }
    byVersion.set(firmwareVersion, {
      firmware_version: firmwareVersion,
      count: row.count,
      hardware_models: [row.hardware_model],
    });
  }
  return Array.from(byVersion.values());
}

function isUnknownFirmwareVersion(version: string): boolean {
  return version.toLowerCase() === 'unknown';
}

function statusPresentation(status: string): { dot: string; label: string } {
  switch (status) {
    case 'ok': return { dot: 'ok', label: 'Operational' };
    case 'running': return { dot: 'ok', label: 'Running' };
    case 'idle': return { dot: 'ok', label: 'Ready' };
    case 'warning': return { dot: 'warning', label: 'Needs attention' };
    case 'stale': return { dot: 'warning', label: 'Delayed' };
    case 'critical': return { dot: 'critical', label: 'Critical' };
    case 'failed': return { dot: 'critical', label: 'Failed' };
    default: return { dot: 'unknown', label: 'Checking' };
  }
}

function reportTime(generatedAt: string): string {
  const value = new Date(generatedAt);
  return Number.isNaN(value.getTime())
    ? 'just now'
    : value.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

export const StatusPage: React.FC = () => {
  const site = getCurrentSite();
  const network = site.networkFilter ?? site.network;
  const observer = site.observerId;
  const { privacyGeneration } = useRuntimeFeatures();
  const [data, setData] = useState<PublicHealthPayload | null>(null);
  const [error, setError] = useState(false);
  const [firmware, setFirmware] = useState<FirmwarePayload | null>(null);
  const [includeUnknownFirmware, setIncludeUnknownFirmware] = useState(false);

  useEffect(() => {
    setData(null);
    setFirmware(null);
    setError(false);
    setIncludeUnknownFirmware(false);
  }, [network, observer, privacyGeneration]);

  useVisibilityPoll(async (signal) => {
    const next = await fetchJson<PublicHealthPayload>(
      withScopeParams('/api/health', { network, observer }),
      { cache: 'no-store', signal },
      { timeoutMs: 15_000, maxBytes: 2 * 1024 * 1024, validate: isPublicHealthPayload },
    );
    if (signal.aborted) return;
    setData(next);
    setError(false);
  }, {
    scopeKey: `platform-health:${network}:${observer ?? 'all'}:${privacyGeneration}`,
    intervalMs: 60_000,
    timeoutMs: 15_000,
    onError: () => setError(true),
  });

  useVisibilityPoll(async (signal) => {
    const next = await fetchJson<FirmwarePayload>(
      withScopeParams('/api/repeaters/firmware', { network, observer }),
      { cache: 'no-store', signal },
      { timeoutMs: 15_000, maxBytes: 2 * 1024 * 1024, validate: isFirmwarePayload },
    );
    if (!signal.aborted) setFirmware(next);
  }, {
    scopeKey: `firmware-health:${network}:${observer ?? 'all'}:${privacyGeneration}`,
    intervalMs: 10 * 60_000,
    timeoutMs: 15_000,
  });

  const firmwareDistribution = firmware ? aggregateFirmwareVersions(firmware.versions) : [];
  const unknownFirmware = firmwareDistribution.find((row) => isUnknownFirmwareVersion(row.firmware_version));
  const visibleFirmwareDistribution = includeUnknownFirmware
    ? firmwareDistribution
    : firmwareDistribution.filter((row) => !isUnknownFirmwareVersion(row.firmware_version));
  const xAxisInterval = Math.max(0, Math.ceil(visibleFirmwareDistribution.length / 8) - 1);

  return (
    <div className="status-page site-content">
      <header><p className="topology-page__eyebrow">Public operations</p><h1>Platform status</h1><p>Service health, ingest freshness, and worker state without exposing private infrastructure details.</p></header>
      {!data && !error && <LoadingIndicator label="Checking platform status…" variant="block" />}
      {error && !data && <div className="status-page__banner status-page__banner--critical" role="alert">Status data is currently unavailable.</div>}
      {data && (
        <>
          <div className={`status-page__banner status-page__banner--${data.status}`} role="status">
            <strong>{data.status === 'healthy' ? 'All monitored systems operational' : `Platform ${data.status}`}</strong>
            <span>Updated {reportTime(data.generatedAt)}</span>
          </div>
          {data.maintenance.active && <div className="status-page__maintenance"><strong>Planned maintenance</strong><span>{data.maintenance.message ?? 'Maintenance is currently in progress.'}</span></div>}
          {data.incidents.length > 0 && <section className="status-page__problems"><h2>Current notices</h2>{data.incidents.map((incident) => <article key={`${incident.code}:${incident.severity}`} className={`status-page__problem status-page__problem--${incident.severity}`}><strong>{incident.code.replace(/_/g, ' ')}</strong><span>{incident.severity === 'critical' ? 'A monitored service is disrupted.' : 'A monitored service needs attention.'}</span></article>)}</section>}
          <div className="status-page__grid">
            {([
              ['Public ingest', 'Packet intake and freshness', data.components.ingest.status],
              ['Background workers', 'Scheduled processing services', data.components.workers.status],
              ['Storage', 'Durable platform storage', data.components.storage.status],
            ] as const).map(([title, description, status]) => {
              const presentation = statusPresentation(status);
              return (
                <section key={title}>
                  <h2>{title}</h2>
                  <div className="status-page__checks">
                    <div>
                      <span className={`status-page__dot status-page__dot--${presentation.dot}`} />
                      <strong>{presentation.label}</strong>
                      <small>{description}</small>
                    </div>
                  </div>
                </section>
              );
            })}
          </div>
          <section className="status-page__firmware">
            <h2>Repeater firmware distribution</h2>
            {firmware && firmware.versions.length > 0 ? (
              <>
                {unknownFirmware && (
                  <p style={{ margin: '0 0 12px', color: 'var(--text-secondary)', fontSize: '12px' }}>
                    <label style={{ display: 'inline-flex', alignItems: 'center', gap: '8px', cursor: 'pointer' }}>
                      <input
                        type="checkbox"
                        checked={includeUnknownFirmware}
                        onChange={(event) => setIncludeUnknownFirmware(event.target.checked)}
                        style={{ accentColor: 'var(--accent)' }}
                      />
                      <span>Include Unknown ({unknownFirmware.count.toLocaleString()})</span>
                    </label>
                  </p>
                )}
                {visibleFirmwareDistribution.length > 0 ? (
                  <>
                    <ResponsiveContainer width="100%" height={320}>
                      <BarChart data={visibleFirmwareDistribution} margin={{ left: 8, right: 8, bottom: 88 }}>
                        <CartesianGrid stroke="rgba(255,255,255,.1)" />
                        <XAxis
                          dataKey="firmware_version"
                          angle={-45}
                          textAnchor="end"
                          interval={xAxisInterval}
                          height={96}
                          minTickGap={10}
                          tick={{ fill: 'var(--text-secondary)', fontSize: 11 }}
                        />
                        <YAxis allowDecimals={false} />
                        <Tooltip formatter={(value: number) => [value.toLocaleString(), 'Repeaters']} />
                        <Bar dataKey="count" name="Repeaters" fill="var(--color-primary)" />
                      </BarChart>
                    </ResponsiveContainer>
                    <div className="ui-visually-hidden">
                      <table>
                        <caption>Repeater firmware distribution{includeUnknownFirmware ? '' : ' (Unknown excluded)'}</caption>
                        <thead><tr><th>Firmware</th><th>Hardware models</th><th>Repeaters</th></tr></thead>
                        <tbody>
                          {visibleFirmwareDistribution.map((row) => (
                            <tr key={row.firmware_version}>
                              <td>{row.firmware_version}</td>
                              <td>{row.hardware_models.join(', ')}</td>
                              <td>{row.count}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : (
                  <p role="status">Only repeaters with unknown firmware were reported. Select "Include Unknown" to show that bucket.</p>
                )}
              </>
            ) : <p>Firmware telemetry is not yet available.</p>}
          </section>
          <p className="status-page__privacy">Status values are deliberately aggregated. Hostnames, addresses, credentials, and private node identities are never included.</p>
        </>
      )}
    </div>
  );
};
