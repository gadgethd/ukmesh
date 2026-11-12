import { useCallback, useEffect, useRef, useState } from 'react';
import { useCommittedRef } from './useCommittedRef.js';
import { ApiResponseError } from '../utils/api.js';
import { scopeKey, type PublicScope } from '../utils/scope.js';
import { feedPathCache, loadFeedPath } from '../features/packets/pathResource.js';
import { feedPathCacheKey, type FeedPacket } from '../features/packets/feedModel.js';
import { initialPathTreeStatus, type PathTreeStatus } from '../features/packets/feedState.js';
import { lazyPathSettleRemainingMs } from '../features/pathing/timing.js';

export function useFeedPathSelection(scope: PublicScope, epoch: number, selected: FeedPacket | null) {
  const key = scopeKey(scope);
  const pathKey = selected ? feedPathCacheKey(selected) : null;
  const current = useCommittedRef({ key, epoch, selected });
  const [pathTreeOpen, setPathTreeOpen] = useState(false);
  const [pathTreeStatus, setPathTreeStatus] = useState<PathTreeStatus>('idle');
  const [, changed] = useState(0);
  const settle = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const consumer = useRef<AbortController | null>(null);
  const selectedLazyPath = pathKey ? feedPathCache.get(key, pathKey) ?? null : null;
  useEffect(() => {
    feedPathCache.activateScope(key);
  }, [key]);
  useEffect(() => {
    clearTimeout(settle.current);
    consumer.current?.abort();
    setPathTreeOpen(false);
    setPathTreeStatus('idle');
    return () => { clearTimeout(settle.current); consumer.current?.abort(); };
  }, [key, pathKey]);
  useEffect(() => {
    if (!selected) { setPathTreeStatus('idle'); return; }
    const next = initialPathTreeStatus(Boolean(selected.path_hashes?.length), selectedLazyPath !== null);
    if (next !== 'idle') setPathTreeStatus(next);
  }, [selected, selectedLazyPath]);
  const fetchSelectedLazyPath = useCallback(async (packet: FeedPacket, skipSettle = false) => {
    const hash = packet.packet_hash;
    const status = (next: PathTreeStatus) => {
      if (current.current.key === key && current.current.epoch === epoch
        && current.current.selected?.packet_hash === hash) setPathTreeStatus(next);
    };
    if (!packet.path_hashes?.length) { status('unavailable'); return; }
    if (feedPathCache.peek(key, feedPathCacheKey(packet))) { status('ready'); return; }
    const remaining = skipSettle ? 0 : lazyPathSettleRemainingMs(packet.time);
    if (remaining > 0) {
      status('settling');
      clearTimeout(settle.current);
      settle.current = setTimeout(() => {
        settle.current = undefined;
        const latest = current.current.selected;
        if (current.current.key === key && latest?.packet_hash === hash) void run.current(latest, true);
      }, remaining);
      return;
    }
    consumer.current?.abort();
    const controller = new AbortController();
    consumer.current = controller;
    status('loading');
    try {
      await loadFeedPath(packet, scope, controller.signal);
      if (controller.signal.aborted) return;
      changed(version => version + 1);
      status('ready');
    } catch (error) {
      if (controller.signal.aborted) return;
      status(error instanceof ApiResponseError && error.status === 404 ? 'unavailable' : 'error');
    }
  }, [key, epoch, scope.network, scope.observer, scope.region, scope.privacyGeneration]);
  const run = useCommittedRef(fetchSelectedLazyPath);
  const openPathTree = useCallback(() => {
    if (!selected) return;
    setPathTreeOpen(true);
    void fetchSelectedLazyPath(selected);
  }, [selected, fetchSelectedLazyPath]);
  return { selectedLazyPath, pathTreeStatus, pathTreeOpen, setPathTreeOpen, fetchSelectedLazyPath, openPathTree };
}
