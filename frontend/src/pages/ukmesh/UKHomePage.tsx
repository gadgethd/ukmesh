import '../site-content.css';
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router';
import { LiveStatsSection } from '../../components/LiveStatsSection.js';
import { getCurrentSite } from '../../config/site.js';
import { fetchJson, withScopeParams } from '../../utils/api.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';

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
  const [recent, setRecent] = useState<RecentSummary[]>([]);
  useEffect(() => {
    setRecent([]);
    const controller = new AbortController();
    void fetchJson(
      withScopeParams('/api/packets/recent?limit=5', {
        network: site.networkFilter,
        observer: site.observerId,
      }),
      { signal: controller.signal },
      { maxBytes: 256 * 1024, validate: isRecentSummaries },
    )
      .then((packets) => { if (!controller.signal.aborted) setRecent(packets); })
      .catch(() => {});
    return () => controller.abort();
  }, [privacyGeneration, site.networkFilter, site.observerId]);

  return (
    <>
      <section className="site-home">
        <div className="site-content site-home__grid">
          <div className="site-home__intro">
            <h1 className="site-home__title">UK Mesh Network</h1>
            <p className="site-home__body">
              The UK-wide public site for MeshCore traffic, repeater coverage, observer ingestion, and the
              supporting documentation behind the live map.
            </p>
            <div className="site-home__actions">
              <a href={site.appUrl} className="site-btn site-btn--primary">Open live map</a>
              <Link to="/install" className="site-btn site-btn--ghost">Install MeshCore</Link>
              <Link to="/stats" className="site-btn site-btn--ghost">Network stats</Link>
            </div>
          </div>

          <section className="site-home__panel">
            <h2>Network overview</h2>
            <div className="site-home__meta">
              {[['Coverage', 'United Kingdom'], ['Band', 'LoRa 868 MHz'], ['Channel', 'Public'], ['Observer ingest', 'Shared MQTT broker']].map(([label, value]) => <div className="site-home__meta-row" key={label}>
                <span>{label}</span><strong>{value}</strong>
              </div>)}
            </div>
          </section>
        </div>
      </section>

      <LiveStatsSection />

      <section className="site-section site-section--dark site-home-feed-section">
        <div className="site-content">
          <div className="site-section__head"><h2>Recent feed</h2><Link to="/feed">Open live feed →</Link></div>
          <div className="site-home-feed">
            {recent.map((packet) => (
              <article key={`${packet.packet_hash}:${packet.time}`}>
                <code>{packet.packet_hash.slice(0, 12)}</code>
                <span>{packet.summary || `Packet type ${packet.packet_type ?? 'unknown'}`}</span>
                <time>{new Date(packet.time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</time>
              </article>
            ))}
            {recent.length === 0 && <p>Waiting for recent public packets…</p>}
          </div>
        </div>
      </section>

      <section className="site-section">
        <div className="site-content">
          <div className="site-section__head">
            <h2>What MeshCore is</h2>
            <p>
              MeshCore is open-source firmware for LoRa hardware. Nodes forward packets between each other, which is
              what makes long regional chains, repeater coverage, and the wider UK map possible. This site is the public
              documentation and analytics layer around that shared network.
            </p>
          </div>
        </div>
      </section>

      <section className="site-section">
        <div className="site-content">
          <div className="site-section__head">
            <h2>Use the network</h2>
            <p>The public site covers the national network, the observer feed, and the operational pages that sit around the live map.</p>
          </div>
          <div className="site-home__cards">
            {[
              { title: 'Get on the air', text: 'Flash a supported device, pair it to your phone, and use the UK public profile. That gets you onto the same channel used across the wider network.', label: 'Install guide', to: '/install' },
              { title: 'Become an observer', text: 'Connect a repeater or room server to the broker, publish packets over MQTT, and add another view of the network from your own location.', label: 'Observer setup', to: '/install' },
              { title: 'Inspect live traffic', text: 'The live app shows repeater positions, path predictions, coverage layers, decoded packets, and the supporting stats for the UK feed.', label: 'Open live map ↗', to: site.appUrl , external: true },
            ].map(card => <div className="site-home__card" key={card.title}>
              <h3>{card.title}</h3><p>{card.text}</p>
              {card.external ? <a href={card.to} className="site-btn site-btn--primary">{card.label}</a>
                : <Link to={card.to} className="site-btn site-btn--ghost">{card.label}</Link>}
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
