import React, { useEffect, useRef, useState } from 'react';
import { useMessages, useNodeMap } from '../hooks/useNodes.js';
import { aggregatedPacketObserverIataLabel } from '../hooks/packetFeed.js';
import { useOverlayStore } from '../store/overlayStore.js';
import { useWatchlist } from '../hooks/useWatchlist.js';
import type { AggregatedPacket } from '../hooks/useNodes.js';

const TYPE_LABELS: Record<number, string> = {
  0:  'REQ',
  1:  'RSP',
  2:  'DM',
  3:  'ACK',
  4:  'ADV',
  5:  'GRP',
  6:  'DAT',
  7:  'ANON',
  8:  'PATH',
  9:  'TRC',
  11: 'CTL',
};

type PacketFeedItemProps = {
  packet: AggregatedPacket;
  observerIata?: string;
  isPinned: boolean;
  isNew: boolean;
  isWatched: boolean;
  onTogglePacket: (packet: AggregatedPacket) => void;
  onToggleWatch: (category: 'packet_type', id: string, label: string) => void;
};

const PacketFeedItem: React.FC<PacketFeedItemProps> = React.memo(({
  packet: p,
  observerIata,
  isPinned,
  isNew,
  isWatched,
  onTogglePacket,
  onToggleWatch,
}) => {
  const typeLabel = p.packetType !== undefined
    ? (TYPE_LABELS[p.packetType] ?? `T${p.packetType}`)
    : '???';
  const display = p.summary?.includes('🚫') ? '[redacted]' : p.summary;
  const messageText = display ?? 'No decoded message text';
  const advertBadge = p.packetType === 4 && typeof p.advertCount === 'number'
    ? (p.advertCount === 1 ? 'NEW' : `${p.advertCount}`)
    : undefined;
  const packetTypeId = String(p.packetType ?? 'unknown');

  return (
    <div
      className={`packet-item packet-item--clickable${isPinned ? ' packet-item--pinned' : ''}${isNew ? ' packet-item--new' : ''}`}
      onClick={() => onTogglePacket(p)}
      role="button"
      tabIndex={0}
      aria-pressed={isPinned}
      aria-label={`${isPinned ? 'Unpin' : 'Pin'} ${typeLabel} packet: ${messageText}`}
      onKeyDown={(event) => {
        if (event.key !== 'Enter' && event.key !== ' ') return;
        event.preventDefault();
        onTogglePacket(p);
      }}
    >
      <div className="packet-item__body">
        <span className="packet-item__summary" title={display ?? undefined}>{messageText}</span>
        <span className="packet-item__meta" aria-label="Packet metadata">
          <span className="packet-item__type" title={typeLabel}>{typeLabel}</span>
          <span className="packet-item__meta-separator" aria-hidden="true">·</span>
          <span
            className={'packet-item__iata' + (observerIata ? '' : ' packet-item__placeholder')}
            title={observerIata ?? 'Observer IATA unavailable'}
          >{observerIata ?? '—'}</span>
          {p.hopCount !== undefined && p.hopCount > 0 && <>
            <span className="packet-item__meta-separator" aria-hidden="true">·</span>
            <span className="packet-item__hops">↑{p.hopCount} hops</span>
          </>}
          {p.pathHashSizeBytes !== undefined && p.pathHashSizeBytes > 0 && <>
            <span className="packet-item__meta-separator" aria-hidden="true">·</span>
            <span className="packet-item__path-bytes">{p.pathHashSizeBytes}B path</span>
          </>}
          {advertBadge && <>
            <span className="packet-item__meta-separator" aria-hidden="true">·</span>
            <span className="packet-item__advert-badge">{advertBadge}</span>
          </>}
          {(p.observerIds.length > 0 || p.txCount > 0) && <>
            <span className="packet-item__meta-separator" aria-hidden="true">·</span>
            <span className="packet-item__counts">
              {p.observerIds.length > 0 && <span className="count count--rx">{p.observerIds.length}rx</span>}
              {p.txCount > 0 && <span className="count count--tx">{p.txCount}tx</span>}
            </span>
          </>}
        </span>
      </div>
      <div className="packet-item__actions">
        <button
          type="button"
          className="packet-item__watch"
          aria-label={ (isWatched ? 'Stop watching' : 'Watch') + ' ' + typeLabel + ' packets' }
          onClick={(event) => {
            event.stopPropagation();
            onToggleWatch('packet_type', packetTypeId, typeLabel + ' packets');
          }}
        >{isWatched ? '★' : '☆'}</button>
        {isPinned && <span className="packet-item__pin" aria-label="Pinned">●</span>}
      </div>
    </div>
  );
});

export const PacketFeed: React.FC = React.memo(() => {
  // GRP messages only — kept in their own store, never evicted by ADV packets
  const messages = useMessages();
  const nodes = useNodeMap();
  const pinnedPacketId = useOverlayStore((state) => state.pinnedPacketId);
  const togglePinnedPacket = useOverlayStore((state) => state.togglePinnedPacket);
  // Oldest first so newest is at the bottom (natural chat order)
  const visible = [...messages].reverse();
  const [newestVisibleId, setNewestVisibleId] = useState<string | null>(null);
  const latestIdRef = useRef<string | null>(null);
  const animationThrottleRef = useRef<number | null>(null);
  const feedRef = useRef<HTMLDivElement>(null);
  const watchlist = useWatchlist();

  useEffect(() => {
    const latestId = messages[0]?.id ?? null;
    if (!latestId || latestIdRef.current === latestId) return;
    latestIdRef.current = latestId;

    // Scroll immediately so new GRP lines are visible the frame they arrive.
    if (feedRef.current) {
      feedRef.current.scrollTop = feedRef.current.scrollHeight;
    }

    // Flash animation is best-effort; never gate scroll/render on it.
    if (animationThrottleRef.current === null) {
      setNewestVisibleId(latestId);
      animationThrottleRef.current = window.setTimeout(() => {
        animationThrottleRef.current = null;
      }, 120);
      const timer = window.setTimeout(
        () => setNewestVisibleId((current) => (current === latestId ? null : current)),
        180,
      );
      return () => clearTimeout(timer);
    }
    return undefined;
  }, [messages]);

  // Scroll to bottom on initial load
  useEffect(() => {
    if (feedRef.current) {
      feedRef.current.scrollTop = feedRef.current.scrollHeight;
    }
  }, []);

  return (
  <div className="packet-feed" ref={feedRef}>
    {visible.map((p) => {
      const observerIata = aggregatedPacketObserverIataLabel(p, nodes);
      return (
        <PacketFeedItem
          key={p.packetHash || p.id}
          packet={p}
          observerIata={observerIata}
          isPinned={pinnedPacketId === p.id}
          isNew={newestVisibleId === p.id}
          isWatched={watchlist.isWatched('packet_type', String(p.packetType ?? 'unknown'))}
          onTogglePacket={togglePinnedPacket}
          onToggleWatch={watchlist.toggle}
        />
      );
    })}
  </div>
  );
});
