import { useEffect, useState } from 'react';
import { statsEndpoint, uncachedEndpoint, type ApiScope } from '../utils/api.js';

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

export function useDashboardStats(scope: ApiScope = {}): DashboardStats {
  const [stats, setStats] = useState<DashboardStats>(EMPTY_STATS);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const response = await fetch(
          uncachedEndpoint(statsEndpoint(scope)),
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
    const interval = setInterval(fetchStats, 10_000);
    return () => clearInterval(interval);
  }, [scope.network, scope.observer]);

  useEffect(() => {
    const handlePacketObserved = () => {
      setStats((current) => ({
        ...current,
        packetsDay: current.packetsDay + 1,
      }));
    };

    window.addEventListener('meshcore:packet-observed', handlePacketObserved as EventListener);
    return () => {
      window.removeEventListener('meshcore:packet-observed', handlePacketObserved as EventListener);
    };
  }, []);

  return stats;
}
