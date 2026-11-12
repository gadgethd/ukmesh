import { fetchJson, uncachedEndpoint, withScopeParams } from '../utils/api.js';
import { nodeStore } from './useNodes.js';
import type { TaggedMessage } from './packetFeed.js';
import { useVisibilityPoll } from './useVisibilityPoll.js';
import { POLLING } from '../config/polling.js';

type TagScope = { network?: string; observer?: string };

export function isTaggedMessages(value: unknown): value is TaggedMessage[] {
  const record = (row: unknown): row is Record<string, unknown> => !!row && typeof row === 'object' && !Array.isArray(row);
  return Array.isArray(value) && value.length <= 300 && value.every(row => {
    if (!record(row) || !record(row.tags)) return false;
    const tags = row.tags;
    const confidence = row.confidence;
    return typeof row.packet_hash === 'string' && row.packet_hash.length <= 128
      && ['kind', 'topic', 'speaker'].every(key => tags[key] === undefined
        || (typeof tags[key] === 'string' && tags[key].length <= 256))
      && ['safety', 'directed', 'mentions_location'].every(key => tags[key] === undefined
        || (typeof tags[key] === 'number' && Number.isFinite(tags[key])))
      && (confidence == null || (record(confidence) && ['kind', 'topic', 'speaker'].every(key =>
        confidence[key] === undefined || (typeof confidence[key] === 'number' && Number.isFinite(confidence[key])))))
      && (row.tagged_at === undefined || (typeof row.tagged_at === 'string' && Number.isFinite(Date.parse(row.tagged_at))));
  });
}

/** Tags arrive after the packet; enrichment failures leave the feed usable. */
export function useMessageTags(scope: TagScope, scopeKey: string): void {
  useVisibilityPoll(async signal => {
    const epoch = nodeStore.getState().epoch;
    const rows = await fetchJson<TaggedMessage[]>(
      uncachedEndpoint(withScopeParams('/api/tags/recent?limit=300', scope)),
      { cache: 'no-store', signal },
      { timeoutMs: POLLING.ordinaryTimeout, maxBytes: 2 * 1024 * 1024, validate: isTaggedMessages },
    );
    if (!signal.aborted && nodeStore.getState().epoch === epoch) nodeStore.applyMessageTags(rows);
  }, {
    scopeKey: `message-tags:${scopeKey}`,
    intervalMs: POLLING.feedTags,
    timeoutMs: POLLING.ordinaryTimeout,
  });
}
