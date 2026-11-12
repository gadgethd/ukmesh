import { useEffect, useMemo, useState } from 'react';
import { POLLING } from '../../config/polling.js';
import { useVisibilityPoll } from '../../hooks/useVisibilityPoll.js';
import { ApiResponseError, fetchJson } from '../../utils/api.js';
import { isValidMapCoord } from '../../utils/pathing.js';
import { isOwnerLastHopStrengthResponse, isOwnerLiveResponse, lastHopSeriesCache,
  type LastHopStrengthPoint, type LastHopWindow, type MappedPeer, type OwnerDashboard,
  type OwnerLastHopStrengthResponse, type OwnerLiveResponse } from './model.js';

export function useOwnerLive({ privacyGeneration, ownerSessionGeneration, dashboard, selectedNodeId, setSelectedNodeId,
  ownerSessionKey, clearOwnerSession }: {
  privacyGeneration: number; ownerSessionGeneration: number; dashboard: OwnerDashboard | null; selectedNodeId: string;
  setSelectedNodeId: (nodeId: string) => void; ownerSessionKey: string | null; clearOwnerSession: () => void;
}) {
  const hasDashboard = dashboard !== null;
  const [liveResult, setLiveResult] = useState<{ scope: string; data: OwnerLiveResponse } | null>(null);
  const [, setLiveLoading] = useState(false);
  const [liveError, setLiveError] = useState<string | null>(null);
  const [lastHopResult, setLastHopResult] = useState<{ scope: string; points: LastHopStrengthPoint[] } | null>(null);
  const [lastHopError, setLastHopError] = useState<string | null>(null);
  const [, setLastHopLoading] = useState(false);
  const [lastHopWindow, setLastHopWindow] = useState<LastHopWindow>('7d');
  const [compareNodeIds, setCompareNodeIds] = useState<string[]>([]);
  const comparisonScope = JSON.stringify([ownerSessionKey, ownerSessionGeneration, privacyGeneration, lastHopWindow, compareNodeIds]);
  const [comparisonResult, setComparisonResult] = useState<{ scope: string; series: Array<{ id: string; points: LastHopStrengthPoint[] }> } | null>(null);
  const compareSeries = comparisonResult?.scope === comparisonScope ? comparisonResult.series : [];
  useEffect(() => {
    setCompareNodeIds([]); setComparisonResult(null); setLastHopResult(null); setLastHopError(null);
  }, [ownerSessionKey, ownerSessionGeneration]);
  const liveScope = `owner-live:${ownerSessionGeneration}:${ownerSessionKey ?? 'none'}:${privacyGeneration}:${selectedNodeId}`;
  const live = liveResult?.scope === liveScope ? liveResult.data : null;
  const lastHopViewScope = `${liveScope}|window:${lastHopWindow}`;
  const lastHopStrength = lastHopResult?.scope === lastHopViewScope ? lastHopResult.points : [];
  const compareKey = compareNodeIds.join('|');
  const compareActive = compareNodeIds.length >= 2;
  const compareChartSeries = useMemo(() => {
    if (!compareActive || compareSeries.length < 2) return undefined;
    return compareSeries.map((entry) => ({
      id: entry.id,
      name: dashboard?.nodes.find((node) => node.node_id === entry.id)?.name ?? entry.id.slice(0, 8),
      points: entry.points,
    }));
  }, [compareActive, compareSeries, dashboard]);

  // Toggles a repeater in/out of the RX-strength comparison set. The first toggle
  // seeds the set with the current primary node so a newly started comparison
  // always contains what the owner was just looking at.
  const toggleCompareNode = (nodeId: string) => {
    setCompareNodeIds((current) => {
      if (current.includes(nodeId)) {
        const next = current.filter((id) => id !== nodeId);
        if (next.length === 1) setSelectedNodeId(next[0]!);
        return next;
      }
      const base = current.length > 0 ? current : (selectedNodeId ? [selectedNodeId] : []);
      return base.includes(nodeId) ? base : [...base, nodeId];
    });
  };

  useEffect(() => {
    setLiveResult(null);
    setLiveError(null);
    setLiveLoading(false);
  }, [liveScope]);

  const retryLive = useVisibilityPoll(async (signal) => {
    setLiveLoading(true);
    setLiveError(null);
    const json = await fetchJson<OwnerLiveResponse>(
      `/api/owner/live?nodeId=${encodeURIComponent(selectedNodeId)}`,
      { cache: 'no-store', signal },
      { timeoutMs: POLLING.ownerTimeout, maxBytes: 4 * 1024 * 1024, validate: isOwnerLiveResponse },
    );
    if (signal.aborted) return;
    setLiveResult({ scope: liveScope, data: json });
    setLiveLoading(false);
    setLiveError(null);
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey) && Boolean(selectedNodeId),
    scopeKey: liveScope,
    intervalMs: POLLING.ownerLive,
    timeoutMs: POLLING.ownerTimeout,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
        return;
      }
      setLiveLoading(false);
      setLiveError(pollError instanceof Error ? pollError.message : 'Unable to load live data');
    },
  });

  const lastHopScope = `owner:${ownerSessionGeneration}:${ownerSessionKey ?? 'none'}|privacy:${privacyGeneration}|window:${lastHopWindow}`;
  useEffect(() => {
    lastHopSeriesCache.activateScope(lastHopScope);
    const cached = selectedNodeId ? lastHopSeriesCache.get(lastHopScope, selectedNodeId) : undefined;
    setLastHopResult(cached ? { scope: lastHopViewScope, points: cached } : null);
    setLastHopError(null);
    setLastHopLoading(false);
  }, [lastHopScope, lastHopViewScope, selectedNodeId]);

  const retryLastHop = useVisibilityPoll(async (signal) => {
    setLastHopLoading(true);
    setLastHopError(null);
    const points = await lastHopSeriesCache.getOrLoad(lastHopScope, selectedNodeId, async (loadSignal) => {
      const json = await fetchJson<OwnerLastHopStrengthResponse>(
        `/api/owner/live-last-hop?nodeId=${encodeURIComponent(selectedNodeId)}&window=${lastHopWindow}`,
        { cache: 'no-store', signal: loadSignal },
        { timeoutMs: POLLING.ownerHistoryTimeout, maxBytes: 8 * 1024 * 1024, validate: isOwnerLastHopStrengthResponse },
      );
      return json.points;
    }, signal);
    if (!signal.aborted) {
      setLastHopResult({ scope: lastHopViewScope, points });
      setLastHopLoading(false);
    }
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey) && Boolean(selectedNodeId),
    scopeKey: `owner-last-hop:${lastHopScope}:${selectedNodeId}`,
    intervalMs: POLLING.ownerHistory,
    timeoutMs: POLLING.ownerHistoryTimeout,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
        return;
      }
      setLastHopLoading(false);
      setLastHopError(pollError instanceof Error ? pollError.message : 'Unable to load RX strength');
    },
  });

  // Multi-repeater comparison: keep the last-hop series for every repeater in the
  // compare set warm (same per-scope cache as the primary selection) so the RX
  // chart can overlay them. Individual repeater failures are skipped.
  useVisibilityPoll(async (signal) => {
    const results: Array<{ id: string; points: LastHopStrengthPoint[] }> = [];
    for (const nodeId of compareNodeIds) {
      if (signal.aborted) return;
      try {
        const points = await lastHopSeriesCache.getOrLoad(lastHopScope, nodeId, async (loadSignal) => {
          const json = await fetchJson<OwnerLastHopStrengthResponse>(
            `/api/owner/live-last-hop?nodeId=${encodeURIComponent(nodeId)}&window=${lastHopWindow}`,
            { cache: 'no-store', signal: loadSignal },
            { timeoutMs: POLLING.ownerHistoryTimeout, maxBytes: 8 * 1024 * 1024, validate: isOwnerLastHopStrengthResponse },
          );
          return json.points;
        }, signal);
        results.push({ id: nodeId, points });
      } catch {
        // Skip repeaters that fail to load; the chart shows the ones that did.
      }
    }
    if (!signal.aborted) setComparisonResult({ scope: comparisonScope, series: results });
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey) && compareActive,
    scopeKey: `owner-last-hop-compare:${lastHopScope}:${compareKey}`,
    intervalMs: POLLING.ownerHistory,
    timeoutMs: POLLING.ownerHistoryPollTimeout,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
      }
    },
  });

  const livePanelState = { hasData: live !== null, error: liveError, onRetry: retryLive };

  const mapPoints = useMemo(() => {
    const ownerNode = live?.ownerNode;
    const points: Array<{ lat: number; lon: number }> = [];
    if (isValidMapCoord(ownerNode?.lat ?? null, ownerNode?.lon ?? null)) {
      points.push({ lat: ownerNode!.lat as number, lon: ownerNode!.lon as number });
    }
    for (const peer of live?.incomingPeers ?? []) {
      if (!isValidMapCoord(peer.lat, peer.lon)) continue;
      points.push({ lat: peer.lat as number, lon: peer.lon as number });
    }
    return points;
  }, [live]);

  const ownerCoord = useMemo(() => {
    const ownerNode = live?.ownerNode;
    const lat = ownerNode?.lat ?? null;
    const lon = ownerNode?.lon ?? null;
    if (!isValidMapCoord(lat, lon)) return null;
    return { lat: lat as number, lon: lon as number };
  }, [live]);

  const mapPeers = useMemo<MappedPeer[]>(
    () => (live?.incomingPeers ?? [])
      .filter((peer) => isValidMapCoord(peer.lat, peer.lon))
      .map((peer) => ({ ...peer, lat: peer.lat as number, lon: peer.lon as number })),
    [live],
  );

  const strongestLink = useMemo(() => {
    const links = live?.linkHealth ?? [];
    return links
      .filter((link) => link.itm_path_loss_db != null)
      .sort((a, b) => (a.itm_path_loss_db ?? Number.POSITIVE_INFINITY) - (b.itm_path_loss_db ?? Number.POSITIVE_INFINITY))[0] ?? null;
  }, [live]);

  const viableLinkCount = useMemo(
    () => (live?.linkHealth ?? []).filter((link) => link.itm_viable || link.force_viable).length,
    [live],
  );

  const latestTelemetry = useMemo(() => {
    const points = live?.telemetry24h ?? [];
    return points.length > 0 ? points[points.length - 1]! : null;
  }, [live]);
  return { live, livePanelState, mapPoints, ownerCoord, mapPeers, strongestLink, viableLinkCount,
    latestTelemetry, compareNodeIds, setCompareNodeIds, toggleCompareNode, compareChartSeries,
    lastHopResult, lastHopViewScope, lastHopError, retryLastHop, lastHopStrength, lastHopWindow, setLastHopWindow };
}
