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
export function packetObservedCount(event: Event): number {
  const detail = 'detail' in event
    ? (event as Event & { detail?: { count?: unknown } | null }).detail
    : null;
  const count = Number(detail?.count ?? 1);
  return Number.isSafeInteger(count) && count > 0 ? count : 1;
}

export function useDashboardStats(
  externalStats: DashboardStats | null,
  scopeKey = 'default',
): DashboardStats {
  const [localPacketsDay, setLocalPacketsDay] = useState(0);
  const prevStatsRef = useRef<DashboardStats | null>(null);

  useEffect(() => {
    prevStatsRef.current = null;
    setLocalPacketsDay(0);
  }, [scopeKey]);

  // Reset local counter when the server sends a fresh packetsDay value
  useEffect(() => {
    if (externalStats && externalStats !== prevStatsRef.current) {
      prevStatsRef.current = externalStats;
      setLocalPacketsDay(0);
    }
  }, [externalStats]);

  useEffect(() => {
    const handlePacketObserved = (event: Event) => {
      if (document.hidden) return;
      setLocalPacketsDay((n) => n + packetObservedCount(event));
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
