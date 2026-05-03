import React, { useEffect, useState } from 'react';
import { statsEndpoint, uncachedEndpoint } from '../utils/api.js';
import { useFlash } from '../hooks/useFlash.js'; // used by StatCard

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
  const [stats, setStats] = useState<SiteStats>(EMPTY_STATS);
  const refreshSeconds = 5 * 60;

  useEffect(() => {
    const loadStats = () => {
      fetch(uncachedEndpoint(statsEndpoint({ network, observer })), { cache: 'no-store' })
        .then((response) => response.json())
        .then((data) => setStats({
          packetsDay: data.packetsDay,
          totalNodes: data.totalNodes,
          internationalNodes: data.internationalNodes ?? 0,
          internationalLastSeen: data.internationalLastSeen ?? null,
          internationalLastCountry: data.internationalLastCountry ?? null,
        }))
        .catch(() => {});
    };

    loadStats();
    const interval = setInterval(loadStats, refreshSeconds * 1000);
    return () => clearInterval(interval);
  }, [network, observer]);

  return (
    <section className="site-stats-section">
      <div className="site-content">
        <div className="site-section__head">
          <h2>Live network stats</h2>
          <p>
            {observer
              ? `Updates every 5 minutes from the selected observer feed.`
              : network === 'test'
              ? `Updates every 5 minutes from the isolated test feed.`
              : `Updates every 5 minutes from the shared packet feed.`}
          </p>
        </div>
        <div className="site-stats-grid">
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
        </div>
      </div>
    </section>
  );
};
