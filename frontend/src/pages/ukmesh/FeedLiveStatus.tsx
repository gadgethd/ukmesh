import React, { useMemo } from 'react';
import { useVisibleClock } from '../../hooks/useVisibleClock.js';
import { feedConnectionDot, feedConnectionLabel, feedConnectionStatus } from '../../features/packets/feedState.js';
import type { WSConnectionState } from '../../features/realtime/protocol.js';
import type { FeedPacket } from '../../features/packets/feedModel.js';
import { PACKET_TYPE_LABELS } from '../../config/packetTypes.js';
import { timeAgo } from '../../utils/time.js';

export const FeedLiveStatus = React.memo(function FeedLiveStatus({ packets, connection, observerCount, children }: {
  packets: readonly FeedPacket[];
  connection: WSConnectionState;
  observerCount: number;
  children?: React.ReactNode;
}) {
  const now = useVisibleClock();
  const status = feedConnectionStatus(connection.readyState, connection.lastMessageAt, packets.length > 0, now);
  const activity = useMemo(() => {
    let packetsLastMin = 0;
    const types: Record<number, number> = {};
    for (const packet of packets) {
      const at = Date.parse(packet.time);
      if (at >= now - 60_000) {
        packetsLastMin++;
        if (packet.packet_type != null) types[packet.packet_type] = (types[packet.packet_type] ?? 0) + 1;
      }
    }
    return { packetsLastMin, types: Object.entries(types).sort((a, b) => b[1] - a[1]).slice(0, 5) };
  }, [packets, now]);
  return <div className="uk-feed-stats">
    <div className="uk-feed-stats__row">
      <span className={`uk-feed-live-dot uk-feed-live-dot--${feedConnectionDot(status)}`} aria-hidden="true" />
      <span className="uk-feed-stats__label" role="status">{feedConnectionLabel(status)}</span>
      <span className="uk-feed-stats__sep" aria-hidden="true">·</span>
      <span className="uk-feed-stats__label">{observerCount} observer{observerCount === 1 ? '' : 's'}</span>
      <span className="uk-feed-stats__sep" aria-hidden="true">·</span>
      <span className="uk-feed-stats__label">{activity.packetsLastMin} pkt/min</span>
      <span className="uk-feed-stats__sep" aria-hidden="true">·</span>
      <span className="uk-feed-stats__label">Last packet: {timeAgo(packets[0]?.time, { now, empty: 'None received' })}</span>
    </div>
    {activity.types.length > 0 && <div className="uk-feed-type-tags">
      {activity.types.map(([type, count]) => <span key={type} className="uk-feed-type-tag">
        {PACKET_TYPE_LABELS[Number(type)] ?? `T${type}`} <strong>{count}</strong>
      </span>)}
    </div>}
    {children}
  </div>;
});
