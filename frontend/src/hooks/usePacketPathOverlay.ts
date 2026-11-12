import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import type { AggregatedPacket } from '../types/domain.js';
import { useNodeMap, useMessages, usePackets } from './useNodes.js';
import { createPredictionResource } from '../features/pathing/predictions.js';
import { useCommittedRef } from './useCommittedRef.js';
import type { Filters } from '../types/filters.js';
import {
  aggregateCanonicalPath,
  buildRegularPacketPaths,
  packetObserverIds,
  type AggregatedPredictionState,
  type CanonicalPathNode,
  type MultiObserverBetaResponse,
  type ResolvedPathRoute,
} from '../features/pathing/model.js';
import { useOverlayStore } from '../store/overlayStore.js';
import { PATH_LINE_FADE_MS, PATH_LINE_TTL_MS } from '../features/pathing/style.js';

// Retain resolved data beyond the renderer's 60-second visible lifetime so the
// hop animation and final fade can finish before React clears the path state.
const PATH_DATA_RETENTION_MS = PATH_LINE_TTL_MS + PATH_LINE_FADE_MS + 30_000;
type UsePacketPathOverlayParams = {
  scopeKey?: string;
  filters: Filters;
  network?: string;
  observer?: string;
};

type UsePacketPathOverlayResult = {
  packetPaths: [number, number][][];
  betaPacketPaths: [number, number][][];
  betaPathPacketHash: string | null;
  betaCanonicalPath: CanonicalPathNode[];
  betaPathRoutes: ResolvedPathRoute[];
  betaObserverIds: string[];
  betaPathConfidence: number | null;
  betaPermutationCount: number | null;
  betaRemainingHops: number | null;
  /** True during the 1-second CSS fade-out before paths are cleared. */
  pathFadingOut: boolean;
  pinnedPacketId: string | null;
  pinnedPacketSnapshot: AggregatedPacket | null;
  activePacketSnapshot: AggregatedPacket | null;
  handlePacketPin: (packet: AggregatedPacket) => void;
};

function packetResolutionKey(packet: AggregatedPacket | null | undefined, network?: string, observer?: string): string | null {
  if (!packet) return null;
  return [
    packet.id,
    packet.packetHash,
    packet.packetType ?? '',
    packet.srcNodeId ?? '',
    packet.rxNodeId ?? '',
    packet.hopCount ?? '',
    packet.pathHashSizeBytes ?? '',
    packet.path?.join(',') ?? '',
    [...packet.observerIds].sort().join(','),
    packet.ts,
    network ?? 'all',
    observer ?? 'all',
  ].join('|');
}

export function usePacketPathOverlay({
  scopeKey,
  filters,
  network,
  observer,
}: UsePacketPathOverlayParams): UsePacketPathOverlayResult {
  const packets = usePackets();
  const messages = useMessages();
  const nodes = useNodeMap();
  const [packetPaths, setPacketPaths] = useState<[number, number][][]>([]);
  const [betaPacketPaths, setBetaPacketPaths] = useState<[number, number][][]>([]);
  const [betaPathPacketHash, setBetaPathPacketHash] = useState<string | null>(null);
  const [betaCanonicalPath, setBetaCanonicalPath] = useState<CanonicalPathNode[]>([]);
  const [betaPathRoutes, setBetaPathRoutes] = useState<ResolvedPathRoute[]>([]);
  const [betaObserverIds, setBetaObserverIds] = useState<string[]>([]);
  const [betaPathConfidence, setBetaPathConfidence] = useState<number | null>(null);
  // The canonical DTO intentionally does not expose alternative permutations
  // or an unresolved-hop completion path. Keep these metrics null for callers
  // that still render the existing evidence popover fields.
  const [betaPermutationCount, setBetaPermutationCount] = useState<number | null>(null);
  const [betaRemainingHops, setBetaRemainingHops] = useState<number | null>(null);
  const pinnedPacketId = useOverlayStore((state) => state.pinnedPacketId);
  const pinnedPacketSnapshot = useOverlayStore((state) => state.pinnedPacketSnapshot);
  const togglePinnedPacket = useOverlayStore((state) => state.togglePinnedPacket);
  const clearPinnedPacket = useOverlayStore((state) => state.clearPinnedPacket);
  // CSS-based fade: instead of animating opacity via 60fps rAF, set one boolean
  // that triggers the existing CSS transition on the path pane.
  const [pathFadingOut, setPathFadingOut] = useState(false);
  const [isPageVisible, setIsPageVisible] = useState(
    () => (typeof document === 'undefined' ? true : document.visibilityState === 'visible'),
  );

  const pinnedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathFadeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const activeReqSeqRef = useRef(0);
  const pinnedOverlayKeyRef = useRef('');
  const [predictions] = useState(() => createPredictionResource(scopeKey ?? ''));
  useEffect(() => () => predictions.dispose(), [predictions]);

  const stopPathTimers = useCallback(() => {
    if (pathTimerRef.current) {
      clearTimeout(pathTimerRef.current);
      pathTimerRef.current = null;
    }
    if (pathFadeTimerRef.current !== null) {
      clearTimeout(pathFadeTimerRef.current);
      pathFadeTimerRef.current = null;
    }
  }, []);

  const clearBetaState = useCallback(() => {
    setBetaPacketPaths([]);
    setBetaPathPacketHash(null);
    setBetaCanonicalPath([]);
    setBetaPathRoutes([]);
    setBetaObserverIds([]);
    setBetaPathConfidence(null);
    setBetaPermutationCount(null);
    setBetaRemainingHops(null);
    useOverlayStore.getState().setPathExplanation(null);
  }, []);

  const clearPathState = useCallback(() => {
    setPacketPaths([]);
    clearBetaState();
    setPathFadingOut(false);
  }, [clearBetaState]);

  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const updateVisibility = () => setIsPageVisible(document.visibilityState === 'visible');
    document.addEventListener('visibilitychange', updateVisibility);
    return () => document.removeEventListener('visibilitychange', updateVisibility);
  }, []);

  const applyAggregatedPrediction = useCallback((aggregated: AggregatedPredictionState) => {
    setBetaPacketPaths(aggregated.routes.map((route) => (
      route.nodes.map((node) => [node.lat, node.lon] as [number, number])
    )));
    setBetaPathPacketHash(aggregated.packetHash);
    setBetaCanonicalPath(aggregated.canonicalPath);
    setBetaPathRoutes(aggregated.routes);
    setBetaObserverIds(aggregated.observerIds);
    setBetaPathConfidence(aggregated.confidence);
    setBetaPermutationCount(null);
    setBetaRemainingHops(null);
    useOverlayStore.getState().setPathExplanation(null);
  }, []);

  const applyServerPrediction = useCallback((
    packetHash: string,
    response: MultiObserverBetaResponse | null,
  ) => {
    const aggregated = aggregateCanonicalPath(response);
    if (aggregated) {
      const state = { ...aggregated, ts: Date.now() };
      applyAggregatedPrediction(state);
      predictions.remember(packetHash, state);
      return;
    }

    const recent = predictions.recent(packetHash);
    if (recent) {
      applyAggregatedPrediction(recent);
      return;
    }
    clearBetaState();
  }, [applyAggregatedPrediction, clearBetaState, predictions]);

  const getPacketObserverIds = useCallback((packet: AggregatedPacket | undefined): string[] => packetObserverIds(packet), []);

  const buildLocalPaths = useCallback((packet: AggregatedPacket | undefined, observerIds: string[]) => (
    buildRegularPacketPaths(packet, observerIds, nodes)
  ), [nodes]);

  const resolveMultiPrediction = useCallback((
    packetHash: string,
    observerIds: string[],
    networkName?: string,
    minFreshTs = 0,
    mode?: 'slow',
  ): Promise<MultiObserverBetaResponse | null> => {
    return predictions.resolve(scopeKey ?? '', packetHash, observerIds, networkName, minFreshTs, mode);
  }, [predictions, scopeKey]);

  const latestPacket = messages[0] ?? null;
  const latestResolutionKey = packetResolutionKey(latestPacket, network, observer);
  const activePacketSnapshot = pinnedPacketId !== null
    ? (packets.find((packet) => packet.id === pinnedPacketId) ?? pinnedPacketSnapshot)
    : (filters.betaPaths ? latestPacket : null);
  const betaEffectThrottleRef = useRef<number | null>(null);
  const previousScopeRef = useRef(scopeKey);
  useLayoutEffect(() => {
    if (previousScopeRef.current === scopeKey) return;
    previousScopeRef.current = scopeKey;
    activeReqSeqRef.current += 1;
    pinnedOverlayKeyRef.current = '';
    stopPathTimers();
    if (pinnedTimerRef.current) clearTimeout(pinnedTimerRef.current);
    if (betaEffectThrottleRef.current !== null) clearTimeout(betaEffectThrottleRef.current);
    pinnedTimerRef.current = null;
    betaEffectThrottleRef.current = null;
    predictions.setScope(scopeKey ?? '');
    clearPathState();
  }, [scopeKey, stopPathTimers, clearPathState]);

  // Keep a ref to the latest message so this effect runs only when the active
  // packet identity changes, not on every packet-feed update.
  const messagesRef = useCommittedRef(messages);

  useEffect(() => {
    if (pinnedPacketId !== null) return;

    if (betaEffectThrottleRef.current !== null) return;
    betaEffectThrottleRef.current = window.setTimeout(() => {
      betaEffectThrottleRef.current = null;
    }, 50);

    stopPathTimers();

    const latest = messagesRef.current[0];
    const observerIds = getPacketObserverIds(latest);
    clearBetaState();
    setPacketPaths(buildLocalPaths(latest, observerIds));

    if (!isPageVisible) {
      setPathFadingOut(false);
      return;
    }

    if (filters.betaPaths && latest?.packetHash && latest.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      // The feed renders its local path immediately, then upgrades once the
      // bounded slow-mode request sees the complete propagation window.
      void resolveMultiPrediction(latest.packetHash, observerIds, network, latest.ts, 'slow')
        .then((response) => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPrediction(latest.packetHash, response);
        })
        .catch(() => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPrediction(latest.packetHash, null);
        });
    }

    if (!filters.betaPaths || !latest) {
      setPathFadingOut(false);
      return;
    }

    // Start a TTL timer: after PATH_TTL - FADE_MS, begin CSS fade-out. After
    // FADE_MS more, clear the path state entirely.
    const FADE_MS = PATH_LINE_FADE_MS;
    setPathFadingOut(false);
    pathTimerRef.current = setTimeout(() => {
      setPathFadingOut(true);
      pathFadeTimerRef.current = setTimeout(() => {
        pathFadeTimerRef.current = null;
        clearPathState();
      }, FADE_MS);
    }, PATH_DATA_RETENTION_MS - FADE_MS);
  // `packets` intentionally omitted — only the latest message drives this
  // effect, so it does not run on every packet-feed update.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [latestResolutionKey, filters.betaPaths, pinnedPacketId, network, observer, getPacketObserverIds, buildLocalPaths, resolveMultiPrediction, applyServerPrediction, isPageVisible, stopPathTimers, clearPathState, clearBetaState]);

  const handlePacketPin = useCallback((packet: AggregatedPacket) => {
    togglePinnedPacket(packet);
  }, [togglePinnedPacket]);

  useEffect(() => {
    if (pinnedTimerRef.current) {
      clearTimeout(pinnedTimerRef.current);
      pinnedTimerRef.current = null;
    }

    if (pinnedPacketId === null) {
      pinnedOverlayKeyRef.current = '';
      stopPathTimers();
      clearPathState();
      return;
    }

    setPathFadingOut(false);
    const FADE_MS = PATH_LINE_FADE_MS;
    pinnedTimerRef.current = setTimeout(() => {
      setPathFadingOut(true);
      pathFadeTimerRef.current = setTimeout(() => {
        pathFadeTimerRef.current = null;
        clearPathState();
        clearPinnedPacket();
        pinnedOverlayKeyRef.current = '';
        pinnedTimerRef.current = null;
      }, FADE_MS);
    }, PATH_DATA_RETENTION_MS);
  }, [clearPathState, clearPinnedPacket, pinnedPacketId, stopPathTimers]);

  useEffect(() => {
    if (pinnedPacketId === null) return;

    if (!isPageVisible) return;

    const pinnedPacket = packets.find((packet) => packet.id === pinnedPacketId) ?? pinnedPacketSnapshot;
    if (!pinnedPacket) return;

    const observerIds = getPacketObserverIds(pinnedPacket);
    const overlayKey = [
      pinnedPacket.id,
      pinnedPacket.packetHash ?? '',
      pinnedPacket.srcNodeId ?? '',
      pinnedPacket.path?.join(',') ?? '',
      observerIds.join(','),
      filters.betaPaths ? 'beta-on' : 'beta-off',
      network ?? 'all',
      observer ?? 'all',
      pinnedPacket.ts,
    ].join('|');

    if (overlayKey === pinnedOverlayKeyRef.current) return;
    pinnedOverlayKeyRef.current = overlayKey;

    clearBetaState();
    setPacketPaths(buildLocalPaths(pinnedPacket, observerIds));

    if (filters.betaPaths && pinnedPacket.packetHash && pinnedPacket.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      // Pinned/detail views use the fast path; the local path renders while
      // the server prediction is fetched.
      void resolveMultiPrediction(pinnedPacket.packetHash, observerIds, network, pinnedPacket.ts)
        .then((response) => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPrediction(pinnedPacket.packetHash!, response);
        })
        .catch(() => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPrediction(pinnedPacket.packetHash!, null);
        });
    }
  }, [
    pinnedPacketId,
    packets,
    pinnedPacketSnapshot,
    filters.betaPaths,
    network,
    observer,
    getPacketObserverIds,
    buildLocalPaths,
    resolveMultiPrediction,
    applyServerPrediction,
    isPageVisible,
    clearBetaState,
  ]);

  useEffect(() => () => {
    stopPathTimers();
    if (pinnedTimerRef.current) clearTimeout(pinnedTimerRef.current);
    if (betaEffectThrottleRef.current !== null) clearTimeout(betaEffectThrottleRef.current);
  }, [stopPathTimers]);

  return {
    packetPaths,
    betaPacketPaths,
    betaPathPacketHash,
    betaCanonicalPath,
    betaPathRoutes,
    betaObserverIds,
    betaPathConfidence,
    betaPermutationCount,
    betaRemainingHops,
    pathFadingOut,
    pinnedPacketId,
    pinnedPacketSnapshot,
    activePacketSnapshot,
    handlePacketPin,
  };
}
