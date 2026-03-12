import React from 'react';
import { Link } from 'react-router-dom';
import { LiveStatsSection } from '../../components/LiveStatsSection.js';
import { getCurrentSite } from '../../config/site.js';

export const UKHomePage: React.FC = () => {
  const site = getCurrentSite();

  return (
    <>
      <section className="site-home">
        <div className="site-content site-home__grid">
          <div className="site-home__intro">
            <h1 className="site-home__title">UK Mesh Network</h1>
            <p className="site-home__body">
              The UK-wide public site for MeshCore traffic, repeater coverage, observer ingestion, and the
              supporting documentation behind the live map. Teesside is part of this wider feed rather than a
              separate stack.
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
              <div className="site-home__meta-row">
                <span>Coverage</span>
                <strong>United Kingdom</strong>
              </div>
              <div className="site-home__meta-row">
                <span>Band</span>
                <strong>LoRa 868 MHz</strong>
              </div>
              <div className="site-home__meta-row">
                <span>Channel</span>
                <strong>Public</strong>
              </div>
              <div className="site-home__meta-row">
                <span>Observer ingest</span>
                <strong>Shared MQTT broker</strong>
              </div>
            </div>
          </section>
        </div>
      </section>

      <LiveStatsSection />

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
            <div className="site-home__card">
              <h3>Get on the air</h3>
              <p>
                Flash a supported device, pair it to your phone, and use the UK public profile. That gets you
                onto the same channel used across the wider network.
              </p>
              <Link to="/install">Open the install guide</Link>
            </div>

            <div className="site-home__card">
              <h3>Become an observer</h3>
              <p>
                Connect a repeater or room server to the broker, publish packets over MQTT, and add another view
                of the network from your own location.
              </p>
              <Link to="/install">Observer setup</Link>
            </div>

            <div className="site-home__card">
              <h3>Inspect live traffic</h3>
              <p>
                The live app shows repeater positions, path predictions, coverage layers, decoded packets, and
                the supporting stats for the UK feed.
              </p>
              <a href={site.appUrl}>Open the live map</a>
            </div>
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
            <div className="site-home__spec-row">
              <span>Profile</span>
              <strong>EU/UK Narrow</strong>
            </div>
            <div className="site-home__spec-row">
              <span>Frequency</span>
              <strong>869.618 MHz</strong>
            </div>
            <div className="site-home__spec-row">
              <span>Bandwidth</span>
              <strong>62.5 kHz</strong>
            </div>
            <div className="site-home__spec-row">
              <span>Spreading factor</span>
              <strong>SF8</strong>
            </div>
            <div className="site-home__spec-row">
              <span>Coding rate</span>
              <strong>CR8</strong>
            </div>
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
              <a href="https://discord.gg/bSuST8xvet" target="_blank" rel="noopener noreferrer" className="site-btn site-btn--primary">
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
