import React, { useMemo } from 'react';
import { formatNeighborAge, type HeardNeighbor } from './ownerPortalModel.js';

function signalValue(value: number | null, suffix: string): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${value.toFixed(1)} ${suffix}`;
}

export const OwnerHeardNeighbors: React.FC<{
  neighbors: HeardNeighbor[];
  source?: 'reported' | 'direct_packets' | 'none';
  sampledAt?: string | null;
}> = ({ neighbors, source, sampledAt: reportedSampleTime }) => {
  const rows = useMemo(
    () => [...neighbors]
      .sort((a, b) => {
        const aTime = Date.parse(a.last_seen_at ?? '');
        const bTime = Date.parse(b.last_seen_at ?? '');
        return (Number.isFinite(bTime) ? bTime : Number.NEGATIVE_INFINITY)
          - (Number.isFinite(aTime) ? aTime : Number.NEGATIVE_INFINITY);
      })
      .slice(0, 32),
    [neighbors],
  );
  const sampledAt = reportedSampleTime ?? rows.find((neighbor) => neighbor.sampled_at)?.sampled_at ?? null;
  const directPackets = source === 'direct_packets';

  if (rows.length === 0) {
    return <p className="prose-note owner-neighbors-empty">{source === 'reported'
      ? `The latest reported neighbor list is empty.${sampledAt ? ` Sampled ${new Date(sampledAt).toLocaleString()}.` : ''}`
      : 'No neighbor list was reported in the last 7 days and no direct senders were observed in the last 24 hours.'}</p>;
  }

  return (
    <div className="owner-neighbors">
      <div className="owner-neighbors__meta">{directPackets
        ? 'No neighbor list reported in the last 7 days. Showing direct senders from received packets in the last 24 hours, with average signal strength.'
        : `Latest reported sample: ${sampledAt ? new Date(sampledAt).toLocaleString() : '—'}`}</div>
      <div className="owner-neighbors__table-wrap">
        <table className="owner-neighbors__table">
          <thead>
            <tr><th scope="col">ID</th><th scope="col">{directPackets ? 'Avg RSSI' : 'RSSI'}</th><th scope="col">{directPackets ? 'Avg SNR' : 'SNR'}</th><th scope="col">Last seen</th></tr>
          </thead>
          <tbody>
            {rows.map((neighbor, index) => (
              <tr key={`${neighbor.id}-${neighbor.last_seen_at ?? neighbor.sampled_at ?? 'unknown'}-${index}`}>
                <td><code title={neighbor.id}>{neighbor.id}</code></td>
                <td>{signalValue(neighbor.rssi, 'dBm')}</td>
                <td>{signalValue(neighbor.snr, 'dB')}</td>
                <td>{formatNeighborAge(neighbor.last_seen_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
