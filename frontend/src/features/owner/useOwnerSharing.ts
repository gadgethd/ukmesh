import { useLayoutEffect, useMemo, useRef, useState } from 'react';
import { POLLING } from '../../config/polling.js';
import { useVisibilityPoll } from '../../hooks/useVisibilityPoll.js';
import { ApiResponseError, fetchJson } from '../../utils/api.js';
import { fetchOwnerCsrfToken, fetchOwnerPacketSharing, fetchOwnerPublicFeed,
  isOwnerPublicFeedResponse, isPacketSharingOwner, isRecord,
  type OwnerPacketSharing, type OwnerPublicFeed } from './model.js';

/** A new scope has a new identity even after A → B → A. Writes also fence earlier reads/writes. */
function useOwnerState<Value extends object>(key: string, empty: Value) {
  const owner = useMemo(() => ({ key, operation: 0, active: false, writing: false }), [key]);
  const current = useRef(owner);
  const [snapshot, setSnapshot] = useState<{ owner: typeof owner; value: Value } | null>(null);
  useLayoutEffect(() => {
    current.current = owner;
    owner.active = true;
    return () => { owner.active = false; };
  }, [owner]);
  const isCurrent = () => current.current === owner && owner.active;
  const capture = () => {
    const operation = owner.operation;
    return () => isCurrent() && operation === owner.operation;
  };
  const publish = (update: Partial<Value> | ((value: Value) => Partial<Value>)) => {
    if (!isCurrent()) return;
    setSnapshot(previous => {
      if (!isCurrent()) return previous;
      const value = previous?.owner === owner ? previous.value : empty;
      return { owner, value: { ...value, ...(typeof update === 'function' ? update(value) : update) } };
    });
  };
  return { value: snapshot?.owner === owner ? snapshot.value : empty, publish, capture,
    isWriting: () => isCurrent() && owner.writing,
    beginWrite: () => {
      owner.operation++;
      owner.writing = true;
      const valid = capture();
      return { valid, finish: () => { if (valid()) owner.writing = false; } };
    } };
}

type PacketState = { settings: OwnerPacketSharing | null; loading: boolean; saving: boolean; error: string | null; destinationIds: string[]; draft: boolean };
type FeedState = { settings: OwnerPublicFeed | null; loading: boolean; saving: boolean; error: string | null; draft: boolean | null };

export function useOwnerSharing({ hasDashboard, ownerSessionKey, ownerSessionGeneration, privacyGeneration, selectedNodeId, clearOwnerSession }: {
  hasDashboard: boolean; ownerSessionKey: string | null; ownerSessionGeneration: number;
  privacyGeneration: number; selectedNodeId: string; clearOwnerSession: () => void;
}) {
  const actorKey = JSON.stringify([ownerSessionKey, ownerSessionGeneration]);
  const actor = useRef<string | null>(null);
  useLayoutEffect(() => {
    actor.current = actorKey;
    return () => { actor.current = null; };
  }, [actorKey]);
  const packetKey = JSON.stringify([actorKey, privacyGeneration, selectedNodeId, hasDashboard]);
  const feedKey = JSON.stringify([actorKey, privacyGeneration, hasDashboard]);
  const packetEnabled = hasDashboard && isPacketSharingOwner(ownerSessionKey) && Boolean(selectedNodeId);
  const feedEnabled = hasDashboard && Boolean(ownerSessionKey);
  const packet = useOwnerState<PacketState>(packetKey, { settings: null, loading: packetEnabled, saving: false, error: null, destinationIds: [], draft: false });
  const feed = useOwnerState<FeedState>(feedKey, { settings: null, loading: feedEnabled, saving: false, error: null, draft: null });

  const refreshPacketSharing = useVisibilityPoll(async signal => {
    if (packet.isWriting()) return;
    const publish = packet.capture();
    try {
      const settings = await fetchOwnerPacketSharing(selectedNodeId, signal);
      if (signal.aborted || !publish()) return;
      packet.publish(value => {
        const confirmed = settings.destinations.filter(item => item.selected).map(item => item.id).sort();
        const draft = value.draft && JSON.stringify(confirmed) !== JSON.stringify([...value.destinationIds].sort());
        return { settings, destinationIds: draft ? value.destinationIds : confirmed, draft, loading: false, error: draft ? value.error : null };
      });
    } catch (error) {
      if (!signal.aborted && publish()) {
        if (error instanceof ApiResponseError && error.status === 401) clearOwnerSession();
        else packet.publish({ loading: false, error: error instanceof Error ? error.message : 'Unable to load packet-sharing status' });
      }
      throw error;
    }
  }, { enabled: packetEnabled, scopeKey: packetKey, intervalMs: POLLING.ownerQueue, timeoutMs: POLLING.ownerTimeout });

  const refreshPublicFeed = useVisibilityPoll(async signal => {
    if (feed.isWriting()) return;
    const publish = feed.capture();
    try {
      const settings = await fetchOwnerPublicFeed(signal);
      if (signal.aborted || !publish()) return;
      feed.publish(value => {
        const draft = value.draft !== null && value.draft !== settings.enabled ? value.draft : null;
        return { settings, loading: false, draft, error: draft !== null ? value.error : null };
      });
    } catch (error) {
      if (!signal.aborted && publish()) {
        if (error instanceof ApiResponseError && error.status === 401) clearOwnerSession();
        else feed.publish({ loading: false, error: error instanceof Error ? error.message : 'Unable to load public-feed status' });
      }
      throw error;
    }
  }, { enabled: feedEnabled, scopeKey: feedKey, intervalMs: POLLING.ownerQueue, timeoutMs: POLLING.ownerTimeout });

  const requireActor = () => {
    if (actor.current !== actorKey) throw new DOMException('Owner session changed before submission', 'AbortError');
  };
  const savePublicFeed = (enabled: boolean) => {
    if (!feed.value.settings?.featureEnabled) return;
    const operation = feed.beginWrite();
    const publish = operation.valid;
    feed.publish({ saving: true, error: null, draft: enabled });
    fetchOwnerCsrfToken()
      .then(csrfToken => {
        requireActor();
        return fetchJson<OwnerPublicFeed>('/api/owner/public-feed', {
          method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
          body: JSON.stringify({ enabled }),
        }, { timeoutMs: POLLING.ownerTimeout, maxBytes: 32 * 1024, validate: isOwnerPublicFeedResponse });
      })
      .then(settings => { if (publish()) feed.publish({ settings, draft: null }); })
      .catch(error => { if (publish()) feed.publish({ error: error.message }); })
      .finally(() => { if (publish()) { operation.finish(); feed.publish({ saving: false }); refreshPublicFeed(); } });
  };
  const savePacketSharing = (next: string[]) => {
    if (!packet.value.settings?.featureEnabled) return;
    const destinationIds = [...new Set(next)].sort();
    const operation = packet.beginWrite();
    const publish = operation.valid;
    packet.publish({ saving: true, error: null, destinationIds, draft: true });
    fetchOwnerCsrfToken()
      .then(csrfToken => {
        requireActor();
        return fetchJson<{ nodeId: string; enabled: boolean; destinationIds: string[] }>('/api/owner/packet-sharing', {
          method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
          body: JSON.stringify({ nodeId: selectedNodeId, enabled: destinationIds.length > 0, destinationIds }),
        }, { timeoutMs: POLLING.ownerTimeout, maxBytes: 32 * 1024,
          validate: (value): value is { nodeId: string; enabled: boolean; destinationIds: string[] } => isRecord(value)
            && value.nodeId === selectedNodeId && typeof value.enabled === 'boolean'
            && Array.isArray(value.destinationIds) && value.destinationIds.every(id => typeof id === 'string') });
      })
      .then(result => {
        if (!publish()) return;
        packet.publish(value => ({ destinationIds: result.destinationIds, draft: false, settings: value.settings ? {
          ...value.settings, enabled: result.enabled,
          destinations: value.settings.destinations.map(item => ({ ...item, selected: result.destinationIds.includes(item.id) })),
        } : null }));
      })
      .catch(error => { if (publish()) packet.publish({ error: error.message }); })
      .finally(() => { if (publish()) { operation.finish(); packet.publish({ saving: false }); refreshPacketSharing(); } });
  };
  const changePacketSharingDestination = (id: string, selected: boolean) => {
    savePacketSharing(selected ? [...new Set([...packet.value.destinationIds, id])].sort()
      : packet.value.destinationIds.filter(value => value !== id));
  };
  return { packetSharing: packet.value.settings, packetSharingLoading: packet.value.loading,
    packetSharingSaving: packet.value.saving, packetSharingError: packet.value.error,
    packetSharingDestinationIds: packet.value.destinationIds, packetSharingHasDraft: packet.value.draft,
    retryPacketSharing: () => savePacketSharing(packet.value.destinationIds), changePacketSharingDestination,
    publicFeed: feed.value.settings, publicFeedLoading: feed.value.loading,
    publicFeedSaving: feed.value.saving, publicFeedError: feed.value.error, publicFeedDraft: feed.value.draft,
    retryPublicFeed: () => { if (feed.value.draft !== null) savePublicFeed(feed.value.draft); }, savePublicFeed };
}
