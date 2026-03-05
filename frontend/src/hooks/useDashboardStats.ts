import { useEffect, useState } from 'react';
import { statsEndpoint, uncachedEndpoint } from '../utils/api.js';

export type DashboardStats = {
  mqttNodes: number;
  staleNodes: number;
  packetsDay: number;
  mapNodes: number;
  totalNodes: number;
};

const EMPTY_STATS: DashboardStats = {
  mqttNodes: 0,
  staleNodes: 0,
  packetsDay: 0,
  mapNodes: 0,
  totalNodes: 0,
};

export function useDashboardStats(network?: string): DashboardStats {
  const [stats, setStats] = useState<DashboardStats>(EMPTY_STATS);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const response = await fetch(
          uncachedEndpoint(statsEndpoint(network)),
          { cache: 'no-store' }
        );
        if (response.ok) {
          setStats(await response.json() as DashboardStats);
        }
      } catch {
        // non-fatal
      }
    };

    fetchStats();
    const interval = setInterval(fetchStats, 30_000);
    return () => clearInterval(interval);
  }, [network]);

  return stats;
}
