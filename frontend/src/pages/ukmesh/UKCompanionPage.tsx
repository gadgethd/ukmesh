import React, { useState, useEffect, useCallback, useRef } from 'react';
import { useWebSocket } from '../../hooks/useWebSocket.js';
import type { WSMessage } from '../../hooks/useWebSocket.js';

interface CompanionEntry {
  sender: string;
  message_count: number;
  last_message_at: string;
}

interface LivePacketData {
  packetType?: number;
  network?: string;
  payload?: {
    decrypted?: {
      sender?: string;
    };
  };
}

function timeAgo(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

const WS_SCOPE = { network: 'ukmesh' };

export const UKCompanionPage: React.FC = () => {
  const [entries, setEntries] = useState<CompanionEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [liveCount, setLiveCount] = useState(0);
  // Track which packet hashes we've already counted to avoid double-counting
  const seenHashes = useRef(new Set<string>());

  const fetchData = useCallback(() => {
    fetch('/api/companion-activity?network=ukmesh')
      .then(r => r.json())
      .then((data: CompanionEntry[]) => {
        setEntries(Array.isArray(data) ? data : []);
        setLastUpdated(new Date());
        setLoading(false);
        // Reset live counter and seen hashes on each full resync
        setLiveCount(0);
        seenHashes.current.clear();
      })
      .catch(() => setLoading(false));
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 60_000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const handleMessage = useCallback((msg: WSMessage) => {
    if (msg.type !== 'packet') return;
    const data = msg.data as LivePacketData & { packetHash?: string };
    if (data.packetType !== 5) return;
    const sender = data.payload?.decrypted?.sender;
    if (!sender) return;

    // Deduplicate by packet hash — multiple observers see the same packet
    const hash = data.packetHash;
    if (hash) {
      if (seenHashes.current.has(hash)) return;
      seenHashes.current.add(hash);
      // Keep the seen set from growing unboundedly
      if (seenHashes.current.size > 10_000) seenHashes.current.clear();
    }

    const now = new Date().toISOString();
    setEntries(prev => {
      const idx = prev.findIndex(e => e.sender === sender);
      let next: CompanionEntry[];
      if (idx >= 0) {
        next = prev.map((e, i) =>
          i === idx ? { ...e, message_count: e.message_count + 1, last_message_at: now } : e
        );
      } else {
        next = [...prev, { sender, message_count: 1, last_message_at: now }];
      }
      return next.sort((a, b) => b.message_count - a.message_count);
    });
    setLiveCount(n => n + 1);
  }, []);

  useWebSocket(handleMessage, WS_SCOPE);

  const topCount = entries[0]?.message_count ?? 1;

  return (
    <>
      <section className="site-home">
        <div className="site-content">
          <div className="site-home__intro">
            <h1 className="site-home__title">Companion Activity</h1>
            <p className="site-home__body">
              Most active companions on the UK MeshCore network — ranked by unique messages sent in the last 24 hours across all decryptable channels.
            </p>
          </div>
        </div>
      </section>

      <section className="site-section site-section--dark">
        <div className="site-content">
          {loading ? (
            <p style={{ color: 'var(--text-muted)', textAlign: 'center', padding: '48px 0' }}>Loading…</p>
          ) : entries.length === 0 ? (
            <p style={{ color: 'var(--text-muted)', textAlign: 'center', padding: '48px 0' }}>No data available.</p>
          ) : (
            <div className="companion-leaderboard">
              {entries.map((entry, i) => {
                const barPct = Math.max(4, Math.round((entry.message_count / topCount) * 100));
                return (
                  <div key={entry.sender} className="companion-row">
                    <span className="companion-row__rank">#{i + 1}</span>
                    <div className="companion-row__main">
                      <div className="companion-row__header">
                        <span className="companion-row__name">{entry.sender}</span>
                        <span className="companion-row__count">{entry.message_count.toLocaleString()} msgs</span>
                      </div>
                      <div className="companion-row__bar-track">
                        <div className="companion-row__bar" style={{ width: `${barPct}%` }} />
                      </div>
                    </div>
                    <span className="companion-row__last">{timeAgo(entry.last_message_at)}</span>
                  </div>
                );
              })}
            </div>
          )}
          {lastUpdated && (
            <p className="companion-updated">
              {liveCount > 0 && <span className="companion-updated__live">+{liveCount} live · </span>}
              Synced {timeAgo(lastUpdated.toISOString())} · resyncs every minute
            </p>
          )}
        </div>
      </section>
    </>
  );
};
