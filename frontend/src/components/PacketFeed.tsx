import React, { useMemo, useState } from 'react';
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
};

interface Props {
  packets:        AggregatedPacket[];
  nodes:          Map<string, MeshNode>;
  onPacketClick?: (packet: AggregatedPacket) => void;
  pinnedPacketId?: string | null;
}

const ROW_HEIGHT = 32;
const VISIBLE_ROWS = 8;
const OVERSCAN = 4;

export const PacketFeed: React.FC<Props> = React.memo(({ packets, nodes, onPacketClick, pinnedPacketId }) => {
  const [scrollTop, setScrollTop] = useState(0);
  const total = packets.length;
  const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const end = Math.min(total, start + VISIBLE_ROWS + OVERSCAN * 2);
  const visible = useMemo(
    () => packets.slice(start, end).map((packet, idx) => ({ packet, index: start + idx })),
    [packets, start, end],
  );

  return (
  <div
    className={`packet-feed${packets.length >= VISIBLE_ROWS ? ' packet-feed--overflow' : ''}`}
    onScroll={(e) => setScrollTop((e.target as HTMLDivElement).scrollTop)}
  >
    <div style={{ height: total * ROW_HEIGHT, position: 'relative' }}>
    {visible.map(({ packet: p, index }) => {
      const typeLabel = p.packetType !== undefined
        ? (TYPE_LABELS[p.packetType] ?? `T${p.packetType}`)
        : '???';

      const rawContent = p.summary
        ?? (p.rxNodeId ? nodes.get(p.rxNodeId)?.name : undefined);
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
          className={`packet-item packet-item--clickable${isPinned ? ' packet-item--pinned' : ''}`}
          style={{ position: 'absolute', top: index * ROW_HEIGHT, left: 0, right: 0 }}
          onClick={() => onPacketClick?.(p)}
          role="button"
          tabIndex={0}
          onKeyDown={(e) => e.key === 'Enter' && onPacketClick?.(p)}
        >
          <span className="packet-item__type">{typeLabel}</span>
          {advertBadge && (
            <span className="packet-item__advert-badge">{advertBadge}</span>
          )}
          {display && (
            <span className="packet-item__summary">{display}</span>
          )}
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
  </div>
  );
});
