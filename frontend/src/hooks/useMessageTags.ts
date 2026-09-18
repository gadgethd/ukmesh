import { useEffect, useRef } from 'react';
import { fetchJson, uncachedEndpoint, withScopeParams } from '../utils/api.js';
import { nodeStore } from './useNodes.js';
import type { TaggedMessage } from './packetFeed.js';

const TAGS_POLL_MS = 15_000;

type TagScope = { network?: string; observer?: string };

/** Polls GET /api/tags/recent and patches tags onto store rows by packet hash.
 *  Tags are written by tagger-worker ~2s after a packet arrives, so live WS rows
 *  start untagged and get their chips on the next poll. Best-effort: failures
 *  are ignored (the feed keeps working untagged). */
export function useMessageTags(scope: TagScope, scopeKey: string): void {
  const scopeRef = useRef(scope);
  scopeRef.current = scope;

  useEffect(() => {
    let cancelled = false;
    let timer: number | null = null;
    const schedule = () => {
      if (cancelled) return;
      timer = window.setTimeout(() => void tick(), TAGS_POLL_MS);
    };
    const tick = async () => {
      if (cancelled) return;
      if (typeof document !== 'undefined' && document.visibilityState === 'hidden') {
        schedule();
        return;
      }
      try {
        const rows = await fetchJson<TaggedMessage[]>(
          uncachedEndpoint(withScopeParams('/api/tags/recent?limit=300', scopeRef.current)),
          { cache: 'no-store' },
          { timeoutMs: 15_000, maxBytes: 2 * 1024 * 1024 },
        );
        if (!cancelled && Array.isArray(rows)) nodeStore.applyMessageTags(rows);
      } catch {
        /* enrichment is best-effort */
      }
      schedule();
    };
    void tick();
    return () => {
      cancelled = true;
      if (timer !== null) window.clearTimeout(timer);
    };
  }, [scopeKey]);
}
