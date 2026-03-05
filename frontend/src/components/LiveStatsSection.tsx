import React, { useEffect, useState } from 'react';
import { statsEndpoint, uncachedEndpoint } from '../utils/api.js';
import { useFlash } from '../hooks/useFlash.js';

type SiteStats = {
  packetsDay: number;
  totalNodes: number;
  longestHop: number;
  longestHopHash: string | null;
};

type LiveStatsSectionProps = {
  network?: string;
};

const EMPTY_STATS: SiteStats = {
  packetsDay: 0,
  totalNodes: 0,
  longestHop: 0,
  longestHopHash: null,
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

export const LiveStatsSection: React.FC<LiveStatsSectionProps> = ({ network }) => {
  const [stats, setStats] = useState<SiteStats>(EMPTY_STATS);
  const hopFlash = useFlash(stats.longestHop);

  useEffect(() => {
    const loadStats = () => {
      fetch(uncachedEndpoint(statsEndpoint(network)), { cache: 'no-store' })
        .then((response) => response.json())
        .then((data) => setStats({
          packetsDay: data.packetsDay,
          totalNodes: data.totalNodes,
          longestHop: data.longestHop,
          longestHopHash: data.longestHopHash ?? null,
        }))
        .catch(() => {});
    };

    loadStats();
    const interval = setInterval(loadStats, 30_000);
    return () => clearInterval(interval);
  }, [network]);

  return (
    <section className="site-stats-section">
      <div className="site-content">
        <p className="site-stats-section__eyebrow">Live network stats · updates every 30s</p>
        <div className="site-stats-grid">
          <StatCard value={stats.packetsDay} label="Packets in the last 24 hours" />
          <StatCard value={stats.totalNodes} label="Nodes ever heard on the network" />
          <div className="site-stat">
            <span className={`site-stat__value${hopFlash ? ' tick-flash' : ''}`}>
              {stats.longestHop.toLocaleString()}<span className="site-stat__suffix"> hops</span>
            </span>
            <span className="site-stat__label">Longest relay chain ever recorded</span>
            {stats.longestHopHash && (
              <span className="site-stat__hash" title={stats.longestHopHash}>
                {stats.longestHopHash}
              </span>
            )}
          </div>
        </div>
      </div>
    </section>
  );
};
