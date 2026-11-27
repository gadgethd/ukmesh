import '../site-content.css';
import React, { lazy, Suspense, useEffect, useState } from 'react';
import { Link } from 'react-router';
import { LiveStatsSection } from '../../components/LiveStatsSection.js';
import { getCurrentSite } from '../../config/site.js';
import { fetchJson, withScopeParams } from '../../utils/api.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';
import { scopeKey } from '../../utils/scope.js';

const HomeRepeaterSearch = lazy(() => import('./HomeRepeaterSearch.js'));

type RecentSummary = { packet_hash: string; time: string; summary?: string | null; packet_type?: number | null };
function isRecentSummaries(value: unknown): value is RecentSummary[] {
  return Array.isArray(value) && value.length <= 5 && value.every(row => row && typeof row === 'object'
    && typeof row.packet_hash === 'string' && typeof row.time === 'string' && Number.isFinite(Date.parse(row.time))
    && (row.summary == null || typeof row.summary === 'string')
    && (row.packet_type == null || Number.isSafeInteger(row.packet_type)));
}

export const UKHomePage: React.FC = () => {
  const site = getCurrentSite();
  const { privacyGeneration } = useRuntimeFeatures();
  const scope = { network: site.networkFilter, observer: site.observerId, privacyGeneration };
  const key = scopeKey(scope);
  const [searchOpen, setSearchOpen] = useState(false);
  const [recentAttempt, setRecentAttempt] = useState(0);
  const [recentResult, setRecentResult] = useState<{ key: string; attempt: number; packets: RecentSummary[] | null }>();
  const recent = privacyGeneration < 1 ? null
    : recentResult?.key === key && recentResult.attempt === recentAttempt ? recentResult.packets : undefined;

  useEffect(() => {
    if (privacyGeneration < 1) return;
    const controller = new AbortController();
    void fetchJson(
      withScopeParams('/api/packets/recent?limit=5', {
        network: site.networkFilter,
        observer: site.observerId,
      }),
      { signal: controller.signal },
      { maxBytes: 256 * 1024, validate: isRecentSummaries },
    )
      .then((packets) => { if (!controller.signal.aborted) setRecentResult({ key, attempt: recentAttempt, packets }); })
      .catch(() => { if (!controller.signal.aborted) setRecentResult({ key, attempt: recentAttempt, packets: null }); });
    return () => controller.abort();
  }, [key, recentAttempt, privacyGeneration, site.networkFilter, site.observerId]);

  return (
    <>
      <section className="site-home">
        <div className="site-content site-home__grid">
          <div className="site-home__intro">
            <h1 className="site-home__title">UK Mesh Network</h1>
            <p className="site-home__body">
              Find the repeaters that relay MeshCore traffic near you and see when they were last heard.
            </p>
          </div>

          <section className="site-home__panel">
            <h2>Explore your area</h2>
            {searchOpen ? <Suspense fallback={<p className="site-home__search-loading" role="status">Opening repeater search…</p>}>
              <HomeRepeaterSearch scope={scope} />
            </Suspense> : <button type="button" className="site-btn site-btn--primary"
              onClick={() => setSearchOpen(true)}>Find a repeater</button>}
            <div className="site-home__actions">
              <Link to="/regions" className="site-btn site-btn--ghost">Browse regions</Link>
              {site.embeddedTools
                ? <Link to="/live-map" className="site-btn site-btn--ghost">Open Live Map</Link>
                : <a href={site.appUrl} className="site-btn site-btn--ghost">Open Live Map</a>}
            </div>
            <p>MeshCore regions show where repeaters relay traffic for a named radio group.</p>
            <Link to="/repeater">Browse repeaters and find nearby</Link>
          </section>
        </div>
      </section>

      <LiveStatsSection />

      <section className="site-section site-section--dark site-home-feed-section">
        <div className="site-content">
          <div className="site-section__head"><h2>Recent packets</h2><Link to="/feed/packets">Open Packet Feed</Link></div>
          <div className="site-home-feed">
            {recent?.map((packet) => (
              <article key={`${packet.packet_hash}:${packet.time}`}>
                <code>{packet.packet_hash.slice(0, 12)}</code>
                <span>{packet.summary || `Packet type ${packet.packet_type ?? 'unknown'}`}</span>
                <time>{new Date(packet.time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</time>
              </article>
            ))}
            {recent === undefined && <p role="status">Loading recent packets…</p>}
            {recent === null && <div role="status"><p>Could not load recent packets.</p><button type="button"
              className="site-btn site-btn--ghost" onClick={() => {
                if (privacyGeneration < 1) window.location.reload();
                else setRecentAttempt(value => value + 1);
              }}>Retry packets</button></div>}
            {recent?.length === 0 && <p>No recent public packets.</p>}
          </div>
        </div>
      </section>

      <section className="site-section">
        <div className="site-content">
          <div className="site-section__head">
            <h2>Join the MeshCore network</h2>
            <p>MeshCore is open-source firmware for LoRa radios. Repeaters forward packets so they can travel farther.</p>
          </div>
          <div className="site-home__cards">
            {[
              { title: 'Set up a radio', text: 'Install the firmware, pair your phone, and select the UK public profile.', label: 'Install MeshCore', to: '/install' },
              { title: 'Become an observer', text: 'Report packets heard by your repeater or room server over MQTT.', label: 'Observer setup', to: '/install#mqtt-observer' },
              { title: 'Check network activity', text: 'See recent packets and which observers received them.', label: 'Live packets', to: '/feed/packets' },
            ].map(card => <div className="site-home__card" key={card.title}>
              <h3>{card.title}</h3><p>{card.text}</p>
              <Link to={card.to} className="site-btn site-btn--ghost">{card.label}</Link>
            </div>)}
          </div>
        </div>
      </section>

      <section className="site-section site-section--dark">
        <div className="site-content">
          <div className="site-section__head">
            <h2>Radio profile</h2>
            <p>These are the network settings used across the UK public deployment.</p>
          </div>
          <div className="site-home__specs">
              {[['Profile', 'EU/UK Narrow'], ['Frequency', '869.618 MHz'], ['Bandwidth', '62.5 kHz'], ['Spreading factor', 'SF8'], ['Coding rate', 'CR8']].map(([label, value]) => <div className="site-home__spec-row" key={label}>
                <span>{label}</span><strong>{value}</strong>
              </div>)}
          </div>
        </div>
      </section>

      <section className="site-section">
        <div className="site-content">
          <div className="site-home__join">
            <div>
              <h2>Join the conversation</h2>
              <p>
                Coverage, observer credentials, and repeater coordination all run through the MeshCore Discord.
                That is where to go if you want to add another observer or compare notes with other UK operators.
              </p>
            </div>
            <div className="site-home__join-actions">
              <a href="https://meshcore.gg/" target="_blank" rel="noopener noreferrer" className="site-btn site-btn--primary">
                Join Discord
              </a>
              <Link to="/open-source" className="site-btn site-btn--ghost">View source</Link>
            </div>
          </div>
        </div>
      </section>
    </>
  );
};
