import React, { useEffect, useMemo, useRef, useState } from 'react';
import type { MeshNode } from '../../types/domain.js';
import { useMeasuredVirtualRows } from '../../hooks/useMeasuredVirtualRows.js';
import { createFeedTimestampFormatter, formatFeedTimestamp, packetObserverIatas, packetSummary, type FeedPacket } from '../../features/packets/feedModel.js';
import { messageTagKind, messageTagTitle } from '../../features/packets/liveFeed.js';
import { PACKET_TYPE_LABELS } from '../../config/packetTypes.js';

const PacketRow = React.memo(function PacketRow({ packet, nodes, selected, select, measure, formatter }: {
  packet: FeedPacket; nodes: Map<string, MeshNode>; selected: boolean;
  select: (hash: string | null) => void; measure: React.RefCallback<HTMLElement>;
  formatter: Intl.DateTimeFormat;
}) {
  const timestamp = useMemo(() => formatFeedTimestamp(packet.time, formatter), [packet.time, formatter]);
  const observers = packetObserverIatas(packet, nodes);
  const label = `${selected ? 'Collapse' : 'Open'} packet ${packet.packet_hash}`;
  return <article ref={measure} aria-label={label}
    className={`uk-feed-packet-row${selected ? ' uk-feed-packet-row--selected' : ''}`}
  ><button type="button" className="uk-feed-packet-activate"
    onClick={() => select(selected ? null : packet.packet_hash)} aria-pressed={selected}
    aria-label={label}>
    <span className="uk-feed-packet-row__meta">
      <span>{timestamp}</span>
      <span>{packet.packet_type != null ? (PACKET_TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—'}</span>
      <span className="uk-feed-packet-row__hops">{packet.hop_count != null ? `${packet.hop_count} hop${packet.hop_count === 1 ? '' : 's'}` : '—'}</span>
      <span className="uk-feed-packet-row__hash dev-status-mono">{packet.packet_hash}</span>
      <span className="uk-feed-packet-row__observer">{observers.length ? observers.join(' · ') : 'unknown'}</span>
    </span>
    {messageTagKind(packet) && <span className="uk-feed-packet-row__tags">
      <span className="msg-tag-chip" title={messageTagTitle(packet)}>{messageTagKind(packet)}</span>
    </span>}
    <span className="uk-feed-packet-row__summary">{packetSummary(packet, nodes)}</span>
  </button></article>;
});

export const FeedPacketRows = React.memo(function FeedPacketRows({ packets, nodes, selectedHash, select, empty }: {
  packets: readonly FeedPacket[]; nodes: Map<string, MeshNode>; selectedHash: string | null;
  select: (hash: string | null) => void; empty: React.ReactNode;
}) {
  const container = useRef<HTMLDivElement>(null);
  const keys = useMemo(() => packets.map(packet => `${packet.packet_hash}-${packet.time}`), [packets]);
  const rows = useMeasuredVirtualRows(keys, container);
  const [formatter, setFormatter] = useState(createFeedTimestampFormatter);
  useEffect(() => {
    const refresh = () => {
      if (document.visibilityState === 'visible') setFormatter(createFeedTimestampFormatter());
    };
    window.addEventListener('languagechange', refresh);
    document.addEventListener('visibilitychange', refresh);
    return () => {
      window.removeEventListener('languagechange', refresh);
      document.removeEventListener('visibilitychange', refresh);
    };
  }, []);
  return <div className="uk-feed-packets-list" ref={container} onScroll={rows.onScroll}>
    {packets.length ? <>
      <div aria-hidden="true" style={{ height: rows.top }} />
      {packets.slice(rows.start, rows.end).map((packet, index) => <PacketRow
        key={keys[rows.start + index]} packet={packet} nodes={nodes} selected={selectedHash === packet.packet_hash}
        select={select} formatter={formatter} measure={rows.measureRow(keys[rows.start + index]!)} />)}
      <div aria-hidden="true" style={{ height: rows.bottom }} />
    </> : empty}
  </div>;
});
