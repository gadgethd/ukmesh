import React, { useLayoutEffect, useRef, useState } from 'react';
import { useMessages, useNodeMap } from '../hooks/useNodes.js';
import { PROHIBITED_NODE_MARKER } from '../utils/pathing.js';
import { aggregatedPacketObserverIataLabel, FEED_MAX_MESSAGES, messageTagKind, messageTagTitle } from '../features/packets/liveFeed.js';
import { useOverlayStore } from '../store/overlayStore.js';
import { useWatchlist } from '../hooks/useWatchlist.js';
import type { AggregatedPacket } from '../types/domain.js';
import { PACKET_TYPE_LABELS } from '../config/packetTypes.js';

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
    ? (PACKET_TYPE_LABELS[p.packetType] ?? `T${p.packetType}`)
    : '???';
  const display = p.summary?.includes(PROHIBITED_NODE_MARKER) ? '[redacted]' : p.summary;
  const messageText = display ?? 'No decoded message text';
  const advertBadge = p.packetType === 4 && typeof p.advertCount === 'number'
    ? (p.advertCount === 1 ? 'NEW' : `${p.advertCount}`)
    : undefined;
  const packetTypeId = String(p.packetType ?? 'unknown');

  return (
    <div
      className={`packet-item packet-item--clickable${isPinned ? ' packet-item--pinned' : ''}${isNew ? ' packet-item--new' : ''}`}
      data-packet-key={p.packetHash || p.id}
    >
      <button type="button" className="packet-item__body"
        onClick={() => onTogglePacket(p)} aria-pressed={isPinned}
        aria-label={`${isPinned ? 'Unpin' : 'Pin'} ${typeLabel} packet: ${messageText}`}>
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
        {messageTagKind(p) && (
          <span className="packet-item__tags">
            <span className="packet-item__tag msg-tag-chip" title={messageTagTitle(p)}>{messageTagKind(p)}</span>
          </span>
        )}
      </button>
      <div className="packet-item__actions">
        <button
          type="button"
          className="packet-item__watch"
          aria-pressed={isWatched}
          aria-label={ (isWatched ? 'Stop watching' : 'Watch') + ' ' + typeLabel + ' packets' }
          onClick={() => onToggleWatch('packet_type', packetTypeId, typeLabel + ' packets')}
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
  // Observation updates keep the same packet key; the store bounds this list.
  const visible = [...messages].reverse();
  const [unread, setUnread] = useState(0);
  const previousKeys = useRef<Set<string> | null>(null);
  const following = useRef(true);
  const anchor = useRef<{ key: string; offset: number } | null>(null);
  const feedRef = useRef<HTMLDivElement>(null);
  const watchlist = useWatchlist();

  const rememberAnchor = () => {
    const feed = feedRef.current;
    if (!feed) return;
    const top = feed.getBoundingClientRect().top;
    const rows = feed.querySelectorAll<HTMLElement>('[data-packet-key]');
    let low = 0, high = rows.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (rows[middle].getBoundingClientRect().bottom <= top) low = middle + 1;
      else high = middle;
    }
    const row = rows[low];
    anchor.current = row ? { key: row.dataset.packetKey!, offset: row.getBoundingClientRect().top - top } : null;
  };

  useLayoutEffect(() => {
    const feed = feedRef.current;
    if (!feed) return;
    const keys = new Set(messages.map(packet => packet.packetHash || packet.id));
    const added = previousKeys.current === null ? 0 : [...keys].filter(key => !previousKeys.current!.has(key)).length;
    previousKeys.current = keys;
    if (following.current) {
      feed.scrollTop = feed.scrollHeight;
    } else {
      const saved = anchor.current;
      const row = saved && [...feed.querySelectorAll<HTMLElement>('[data-packet-key]')]
        .find(element => element.dataset.packetKey === saved.key);
      if (row && saved) feed.scrollTop += row.getBoundingClientRect().top - feed.getBoundingClientRect().top - saved.offset;
      // An evicted anchor falls back to the oldest retained content.
      else if (saved) feed.scrollTop = 0;
      if (added) setUnread(count => Math.min(FEED_MAX_MESSAGES + 1, count + added));
    }
    if (!following.current) rememberAnchor();
  }, [messages, unread]);

  useLayoutEffect(() => {
    const resize = new ResizeObserver(() => {
      const feed = feedRef.current;
      if (feed && following.current) feed.scrollTop = feed.scrollHeight;
    });
    if (feedRef.current) resize.observe(feedRef.current);
    return () => resize.disconnect();
  }, []);

  return (
  <div className="packet-feed-frame">
  <div className="packet-feed" ref={feedRef} onScroll={() => {
    const feed = feedRef.current!;
    following.current = feed.scrollHeight - feed.scrollTop - feed.clientHeight <= 48;
    if (following.current) setUnread(0);
    if (!following.current) rememberAnchor();
  }}>
    {visible.map((p) => {
      const observerIata = aggregatedPacketObserverIataLabel(p, nodes);
      return (
        <PacketFeedItem
          key={p.packetHash || p.id}
          packet={p}
          observerIata={observerIata}
          isPinned={pinnedPacketId === p.id}
          isNew={messages[0] === p}
          isWatched={watchlist.isWatched('packet_type', String(p.packetType ?? 'unknown'))}
          onTogglePacket={togglePinnedPacket}
          onToggleWatch={watchlist.toggle}
        />
      );
    })}
  </div>
  {unread > 0 && <button type="button" className="packet-feed__jump" onClick={() => {
    following.current = true;
    setUnread(0);
    if (feedRef.current) feedRef.current.scrollTop = feedRef.current.scrollHeight;
  }}>{unread > FEED_MAX_MESSAGES ? `${FEED_MAX_MESSAGES}+` : unread} new message{unread === 1 ? '' : 's'}. Jump to latest</button>}
  </div>
  );
});
