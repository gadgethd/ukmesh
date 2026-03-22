import React, { useEffect, useRef, useState } from 'react';
import { useMessages, useNodeMap } from '../hooks/useNodes.js';
import { useOverlayStore } from '../store/overlayStore.js';

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

  useEffect(() => {
    const latestId = messages[0]?.id ?? null;
    if (!latestId || latestIdRef.current === latestId) return;
    latestIdRef.current = latestId;

    // Scroll to bottom when a new message arrives
    if (feedRef.current) {
      feedRef.current.scrollTop = feedRef.current.scrollHeight;
    }

    // Throttle animations to max once per 300ms
    if (animationThrottleRef.current !== null) return;
    animationThrottleRef.current = window.setTimeout(() => {
      animationThrottleRef.current = null;
    }, 300);

    setNewestVisibleId(latestId);
    const timer = setTimeout(() => setNewestVisibleId((current) => (current === latestId ? null : current)), 220);
    return () => clearTimeout(timer);
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
      const typeLabel = p.packetType !== undefined
        ? (TYPE_LABELS[p.packetType] ?? `T${p.packetType}`)
        : '???';
      const observerIata = p.rxNodeId ? nodes.get(p.rxNodeId)?.iata : undefined;
      const rawContent = p.summary;
      const display = rawContent?.includes('🚫') ? '[redacted]' : rawContent;

      const advertBadge = p.packetType === 4 && typeof p.advertCount === 'number'
        ? (p.advertCount === 1 ? 'NEW' : `${p.advertCount}`)
        : undefined;

      const isPinned = pinnedPacketId === p.id;

      return (
        <div
          key={p.id}
          className={`packet-item packet-item--clickable${isPinned ? ' packet-item--pinned' : ''}${newestVisibleId === p.id ? ' packet-item--new' : ''}`}
          onClick={() => togglePinnedPacket(p)}
          role="button"
          tabIndex={0}
          onKeyDown={(e) => e.key === 'Enter' && togglePinnedPacket(p)}
        >
          {observerIata && (
            <span className="packet-item__iata">{observerIata}</span>
          )}
          {p.pathHashSizeBytes !== undefined && p.pathHashSizeBytes > 0 && (
            <span className="packet-item__path-bytes">{p.pathHashSizeBytes}</span>
          )}
          <span className="packet-item__type">{typeLabel}</span>
          {advertBadge && (
            <span className="packet-item__advert-badge">{advertBadge}</span>
          )}
          <span className={`packet-item__summary${display ? '' : ' packet-item__summary--empty'}`}>
            {display ?? '\u00A0'}
          </span>
          {p.hopCount !== undefined && p.hopCount > 0 && (
            <span className="packet-item__hops">↑{p.hopCount}</span>
          )}
          <span className="packet-item__counts">
            {p.observerIds.length > 0 && (
              <span className="count count--rx">{p.observerIds.length}rx</span>
            )}
            {p.txCount > 0 && <span className="count count--tx">{p.txCount}tx</span>}
          </span>
          {isPinned && <span className="packet-item__pin">●</span>}
        </div>
      );
    })}
  </div>
  );
});
