import React, { useEffect, useMemo, useRef, useState } from 'react';
import type { AggregatedPacket, MeshNode } from '../hooks/useNodes.js';

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

interface Props {
  packets:        AggregatedPacket[];
  nodes:          Map<string, MeshNode>;
  mqttObserverCount?: number;
  onPacketClick?: (packet: AggregatedPacket) => void;
  pinnedPacketId?: string | null;
}

const VISIBLE_ROWS = 8;

export const PacketFeed: React.FC<Props> = React.memo(({ packets, nodes, mqttObserverCount = 0, onPacketClick, pinnedPacketId }) => {
  const visible = useMemo(
    () => packets.filter((p) => p.packetType === 4 || p.packetType === 5).slice(0, VISIBLE_ROWS).reverse(),
    [packets],
  );
  const [newestVisibleId, setNewestVisibleId] = useState<string | null>(null);
  const latestIdRef = useRef<string | null>(null);
  const animationThrottleRef = useRef<number | null>(null);

  useEffect(() => {
    const latestId = packets.find((p) => p.packetType === 4 || p.packetType === 5)?.id ?? null;
    if (!latestId || latestIdRef.current === latestId) return;
    latestIdRef.current = latestId;
    
    // Throttle animations to max once per 300ms
    if (animationThrottleRef.current !== null) return;
    animationThrottleRef.current = window.setTimeout(() => {
      animationThrottleRef.current = null;
    }, 300);
    
    setNewestVisibleId(latestId);
    const timer = setTimeout(() => setNewestVisibleId((current) => (current === latestId ? null : current)), 220);
    return () => clearTimeout(timer);
  }, [packets]);

  return (
  <div className="packet-feed">
    {visible.map((p) => {
      const typeLabel = p.packetType !== undefined
        ? (TYPE_LABELS[p.packetType] ?? `T${p.packetType}`)
        : '???';
      const observerIata = p.rxNodeId ? nodes.get(p.rxNodeId)?.iata : undefined;
      const allObserversHeard = mqttObserverCount > 1 && p.observerIds.length >= mqttObserverCount;

      const rawContent = p.summary;
      const content = rawContent?.includes('🚫') ? '[redacted]' : rawContent;
      const display = content && content.length > 28
        ? `${content.slice(0, 26)}…`
        : content;

      const advertBadge = p.packetType === 4 && typeof p.advertCount === 'number'
        ? (p.advertCount === 1 ? 'NEW' : `${p.advertCount}`)
        : undefined;

      const isPinned = pinnedPacketId === p.id;

      return (
        <div
          key={p.id}
          className={`packet-item packet-item--clickable${isPinned ? ' packet-item--pinned' : ''}${newestVisibleId === p.id ? ' packet-item--new' : ''}`}
          onClick={() => onPacketClick?.(p)}
          role="button"
          tabIndex={0}
          onKeyDown={(e) => e.key === 'Enter' && onPacketClick?.(p)}
        >
          {(observerIata || allObserversHeard) && (
            <span className={`packet-item__iata${allObserversHeard ? ' packet-item__iata--all' : ''}`}>
              {allObserversHeard ? 'ALL' : observerIata}
            </span>
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
            {p.rxCount > 0 && <span className="count count--rx">{p.rxCount}rx</span>}
            {p.txCount > 0 && <span className="count count--tx">{p.txCount}tx</span>}
          </span>
          {isPinned && <span className="packet-item__pin">●</span>}
        </div>
      );
    })}
  </div>
  );
});
