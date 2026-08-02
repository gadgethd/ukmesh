import React, { useEffect, useState } from 'react';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { useVisibilityPoll } from '../hooks/useVisibilityPoll.js';
import { fetchJson, statsEndpoint, uncachedEndpoint } from '../utils/api.js';
import { useFlash } from '../hooks/useFlash.js'; // used by StatCard
import { LoadingIndicator } from './LoadingIndicator.js';

type SiteStats = {
  packetsDay: number;
  totalNodes: number;
  internationalNodes: number;
  internationalLastSeen: string | null;
  internationalLastCountry: string | null;
};

type LiveStatsSectionProps = {
  network?: string;
  observer?: string;
};

const EMPTY_STATS: SiteStats = {
  packetsDay: 0,
  totalNodes: 0,
  internationalNodes: 0,
  internationalLastSeen: null,
  internationalLastCountry: null,
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseSiteStats(value: unknown): SiteStats {
  if (!isRecord(value)) throw new Error('Stats response was not an object');
  return {
    packetsDay: Number.isFinite(Number(value['packetsDay'])) ? Number(value['packetsDay']) : 0,
    totalNodes: Number.isFinite(Number(value['totalNodes'])) ? Number(value['totalNodes']) : 0,
    internationalNodes: Number.isFinite(Number(value['internationalNodes'])) ? Number(value['internationalNodes']) : 0,
    internationalLastSeen: typeof value['internationalLastSeen'] === 'string'
      ? value['internationalLastSeen']
      : null,
    internationalLastCountry: typeof value['internationalLastCountry'] === 'string'
      ? value['internationalLastCountry']
      : null,
  };
}

const timeAgo = (ts: string | null): string => {
  if (!ts) return '';
  const sec = Math.round((Date.now() - new Date(ts).getTime()) / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
};

const StatCard: React.FC<{ value: number; label: string; suffix?: string }> = ({
  value,
  label,
  suffix = '',
}) => {
  const flash = useFlash(value);
  return (
    <div className="site-stat">
      <span className={`site-stat__value${flash ? ' tick-flash' : ''}`}>
        {value.toLocaleString()}
        {suffix && <span className="site-stat__suffix">{suffix}</span>}
      </span>
      <span className="site-stat__label">{label}</span>
    </div>
  );
};

export const LiveStatsSection: React.FC<LiveStatsSectionProps> = ({ network, observer }) => {
  const { privacyGeneration } = useRuntimeFeatures();
  const [stats, setStats] = useState<SiteStats>(EMPTY_STATS);
  const [hasLoaded, setHasLoaded] = useState(false);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    setStats(EMPTY_STATS);
    setHasLoaded(false);
    setLoading(true);
  }, [network, observer, privacyGeneration]);

  useVisibilityPoll(async (signal) => {
    setLoading(true);
    const payload = await fetchJson<unknown>(
      uncachedEndpoint(statsEndpoint({ network, observer })),
      { cache: 'no-store', signal },
      { timeoutMs: 15_000, maxBytes: 2 * 1024 * 1024 },
    );
    if (signal.aborted) return;
    setStats(parseSiteStats(payload));
    setHasLoaded(true);
    setLoading(false);
  }, {
    scopeKey: `live-stats:${network ?? 'all'}:${observer ?? 'all'}:${privacyGeneration}`,
    intervalMs: 5 * 60_000,
    timeoutMs: 15_000,
    onError: () => setLoading(false),
  });

  const initialLoading = loading && !hasLoaded;

  return (
    <section className="site-stats-section">
      <div className="site-content">
        <div className="site-section__head">
          <h2>Live network stats</h2>
          <p>
            {observer
              ? `Updates every 5 minutes from the selected observer feed.`
              : `Updates every 5 minutes from the shared packet feed.`}
          </p>
        </div>
        <div className="site-stats-grid">
          {initialLoading ? (
            <>
              <div className="site-stat site-stat--loading">
                <LoadingIndicator label="Loading packet stats..." variant="block" />
              </div>
              <div className="site-stat site-stat--loading">
                <LoadingIndicator label="Loading node stats..." variant="block" />
              </div>
              <div className="site-stat site-stat--loading">
                <LoadingIndicator label="Checking contacts..." variant="block" />
              </div>
            </>
          ) : (
            <>
              <StatCard value={stats.packetsDay} label="Observed packets in the last 24 hours" />
              <StatCard value={stats.totalNodes} label="Nodes ever heard on the network" />
              <div className="site-stat">
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <span
                className={`conn-dot${stats.internationalNodes > 0 ? ' conn-dot--connected' : ''}`}
                style={{
                  width: '12px', height: '12px', flexShrink: 0,
                  background: stats.internationalNodes > 0 ? undefined : 'var(--danger)',
                }}
              />
              <span
                className="site-stat__value"
                style={{ color: stats.internationalNodes > 0 ? 'var(--online)' : 'var(--danger)' }}
              >
                {stats.internationalNodes > 0 ? 'Active' : 'None'}
              </span>
            </div>
            <span className="site-stat__label">International contacts</span>
            {stats.internationalLastSeen && (
              <span className="site-stat__hash">
                last contact {timeAgo(stats.internationalLastSeen)}
                {stats.internationalLastCountry && ` (${stats.internationalLastCountry})`}
              </span>
            )}
              </div>
            </>
          )}
        </div>
      </div>
    </section>
  );
};
