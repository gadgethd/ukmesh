import React from 'react';
import { Link } from 'react-router-dom';
import { LiveStatsSection } from '../components/LiveStatsSection.js';
import { getCurrentSite } from '../config/site.js';

export const HomePage: React.FC = () => {
  const site = getCurrentSite();

  return (
    <>
      <section className="site-home">
        <div className="site-content site-home__grid">
          <div className="site-home__intro">
            <h1 className="site-home__title">Teesside Mesh Network</h1>
            <p className="site-home__body">
              A regional MeshCore deployment for Teesside and the wider North East. The website documents the
              network, the live map shows what the observer hears, and the MQTT path lets repeater owners feed
              packet data into the shared view.
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
                <strong>Teesside and North East England</strong>
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
                <strong>MQTT via UK Mesh broker</strong>
              </div>
            </div>
            <p>
              Teesside views are filtered from the shared ingest path. Observers using IATA <strong>MME</strong>{' '}
              show up here and in the wider UK stack.
            </p>
          </section>
        </div>
      </section>

      <LiveStatsSection network={site.network} />

      <section className="site-section">
        <div className="site-content">
          <div className="site-section__head">
            <h2>What MeshCore is</h2>
            <p>
              MeshCore is open-source firmware for LoRa radios. Each node can relay packets, which is what lets
              traffic move across repeaters long after it has left the original transmitter. This site tracks the
              Teesside observer view, node positions, relay behaviour, and the public data that sits behind the live map.
            </p>
          </div>
        </div>
      </section>

      <section className="site-section">
        <div className="site-content">
          <div className="site-section__head">
            <h2>Use the network</h2>
            <p>Everything on the public site should answer one of three questions: what MeshCore is, how to join, and how to contribute useful coverage.</p>
          </div>
          <div className="site-home__cards">
            <div className="site-home__card">
              <h3>Get on the air</h3>
              <p>
                A handheld node is the fastest way in. Flash the firmware in a browser, pair it to your phone,
                set the UK profile, and use the default Public channel.
              </p>
              <Link to="/install">Open the install guide</Link>
            </div>

            <div className="site-home__card">
              <h3>Feed the dashboards</h3>
              <p>
                Repeater owners can run `meshcoretomqtt` on a Pi or other Linux host, publish packets over MQTT,
                and contribute live telemetry back into the map and stats pages.
              </p>
              <Link to="/install">Observer setup</Link>
            </div>

            <div className="site-home__card">
              <h3>Inspect live traffic</h3>
              <p>
                The map shows repeater locations, live packets, path predictions, coverage modelling, and the
                stats pages that sit behind the public network view.
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
            <p>These are the settings used on the Teesside side of the network.</p>
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
                The network is community-run. If you want help with hardware, range testing, observer setup, or
                repeater placement, the Discord is where that happens.
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
