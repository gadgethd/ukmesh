import { useEffect, useRef, useState } from 'react';

export type DashboardStats = {
  mqttNodes: number;
  staleNodes: number;
  packetsDay: number;
  mapNodes: number;
  totalNodes: number;
};

export const EMPTY_STATS: DashboardStats = {
  mqttNodes: 0,
  staleNodes: 0,
  packetsDay: 0,
  mapNodes: 0,
  totalNodes: 0,
};

/**
 * Wraps externally-fetched stats (from App.tsx consolidated poll) with a
 * real-time `packetsDay` increment driven by the meshcore:packet-observed event.
 * The interval counter resets each time fresh stats arrive from the server.
 */
export function useDashboardStats(externalStats: DashboardStats | null): DashboardStats {
  const [localPacketsDay, setLocalPacketsDay] = useState(0);
  const prevStatsRef = useRef<DashboardStats | null>(null);

  // Reset local counter when the server sends a fresh packetsDay value
  useEffect(() => {
    if (externalStats && externalStats !== prevStatsRef.current) {
      prevStatsRef.current = externalStats;
      setLocalPacketsDay(0);
    }
  }, [externalStats]);

  useEffect(() => {
    const handlePacketObserved = () => {
      setLocalPacketsDay((n) => n + 1);
    };
    window.addEventListener('meshcore:packet-observed', handlePacketObserved as EventListener);
    return () => {
      window.removeEventListener('meshcore:packet-observed', handlePacketObserved as EventListener);
    };
  }, []);

  const base = externalStats ?? EMPTY_STATS;
  return localPacketsDay > 0
    ? { ...base, packetsDay: base.packetsDay + localPacketsDay }
    : base;
}
