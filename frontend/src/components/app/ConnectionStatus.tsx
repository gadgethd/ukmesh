import type { WSConnectionState } from '../../features/realtime/protocol.js';
import { feedConnectionLabel, feedConnectionStatus } from '../../features/packets/feedState.js';
import { useVisibleClock } from '../../hooks/useVisibleClock.js';
import { timeAgo } from '../../utils/time.js';

export function ConnectionStatus({ connection, hasData, refreshedAt, refreshing, failed }: {
  connection: WSConnectionState; hasData: boolean; refreshedAt: number | null; refreshing: boolean; failed: boolean;
}) {
  const now = useVisibleClock();
  const status = feedConnectionStatus(connection.readyState, connection.lastMessageAt, hasData, now);
  const label = feedConnectionLabel(status);
  const age = refreshedAt === null ? 'None' : timeAgo(refreshedAt, { now });
  return <div className="conn-indicator" title={`${label}. ${failed ? 'Refresh failed. ' : ''}Last successful data refresh: ${age}`}>
    <span className={`conn-dot${status === 'live' ? ' conn-dot--connected' : ''}`} aria-hidden="true" />
    <span className="conn-indicator__labels">
      <span role="status" aria-label={`Live connection: ${connection.readyState}`}>{label}</span>
      <span className="conn-indicator__refresh">{refreshing ? 'Refreshing...' : refreshedAt === null ? 'No refresh' : `Data ${age}`}</span>
    </span>
  </div>;
}
