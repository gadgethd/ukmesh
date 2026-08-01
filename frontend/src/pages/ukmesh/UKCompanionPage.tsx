import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useWebSocket } from '../../hooks/useWebSocket.js';
import type { WSMessage } from '../../hooks/useWebSocket.js';
import { LoadingIndicator } from '../../components/LoadingIndicator.js';
import { ObserverRegistrationForm } from '../../components/ObserverRegistrationForm.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';
import { getCurrentSite } from '../../config/site.js';
import { useVisibilityPoll } from '../../hooks/useVisibilityPoll.js';
import { fetchJson, withScopeParams } from '../../utils/api.js';

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

function isCompanionEntries(value: unknown): value is CompanionEntry[] {
  return Array.isArray(value) && value.every((entry) => (
    typeof entry === 'object'
    && entry !== null
    && typeof (entry as Record<string, unknown>)['sender'] === 'string'
    && typeof (entry as Record<string, unknown>)['message_count'] === 'number'
    && typeof (entry as Record<string, unknown>)['last_message_at'] === 'string'
  ));
}

export const UKCompanionPage: React.FC = () => {
  const site = getCurrentSite();
  const network = site.networkFilter ?? site.network;
  const observer = site.observerId;
  const { privacyGeneration } = useRuntimeFeatures();
  const requestScope = useMemo(() => ({ network, observer }), [network, observer]);
  const scopeKey = `${network}:${observer ?? 'all'}:${privacyGeneration}`;
  const [entries, setEntries] = useState<CompanionEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [liveCount, setLiveCount] = useState(0);
  // Track which packet hashes we've already counted to avoid double-counting
  const seenHashes = useRef(new Set<string>());

  useEffect(() => {
    setEntries([]);
    setLoading(true);
    setLastUpdated(null);
    setLiveCount(0);
    seenHashes.current.clear();
  }, [scopeKey]);

  useVisibilityPoll(async (signal) => {
    const data = await fetchJson<CompanionEntry[]>(
      withScopeParams('/api/companion-activity', requestScope),
      { cache: 'no-store', signal },
      { timeoutMs: 15_000, maxBytes: 2 * 1024 * 1024, validate: isCompanionEntries },
    );
    if (signal.aborted) return;
    setEntries(data.slice(0, 2_000));
    setLastUpdated(new Date());
    setLoading(false);
    // A full resync is authoritative for both the live delta and dedupe window.
    setLiveCount(0);
    seenHashes.current.clear();
  }, {
    scopeKey: `companion-activity:${scopeKey}`,
    intervalMs: 60_000,
    timeoutMs: 15_000,
    onError: () => setLoading(false),
  });

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
      return next.sort((a, b) => b.message_count - a.message_count).slice(0, 2_000);
    });
    setLiveCount(n => n + 1);
  }, []);

  useWebSocket(handleMessage, requestScope);

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

      <section className="site-section site-section--dark companion-page">
        <div className="site-content">
          {loading ? (
            <LoadingIndicator label="Loading companion activity..." variant="block" />
          ) : entries.length === 0 ? (
            <p style={{ color: 'var(--text-muted)', textAlign: 'center', padding: '48px 0' }}>No data available.</p>
          ) : (
            <>
              <div className="companion-leaderboard__legend" aria-label="Activity bars are relative to number one">
              <span>Activity scale</span>
              <span>100% = #1 - {topCount.toLocaleString()} msgs</span>
            </div>
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
            </>
          )}
          {lastUpdated && (
            <p className="companion-updated">
              {liveCount > 0 && <span className="companion-updated__live">+{liveCount} live · </span>}
              Synced {timeAgo(lastUpdated.toISOString())} · resyncs every minute
            </p>
          )}
        </div>
      </section>
      <section className="site-section">
        <div className="site-content site-prose">
          <h2>Register an observer station</h2>
          <p>Submit the station key and region for rate-limited operator review. Broker credentials are issued out of band.</p>
          <ObserverRegistrationForm />
        </div>
      </section>
    </>
  );
};
