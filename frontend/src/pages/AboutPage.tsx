import React from 'react';
import { Link } from 'react-router-dom';

export const AboutPage: React.FC = () => (
  <>
    <section className="site-page-hero">
      <div className="site-content">
        <h1 className="site-page-hero__title">What is MeshCore?</h1>
        <p className="site-page-hero__sub">
          LoRa radio meets mesh networking: resilient, off-grid communications with no internet required.
        </p>
      </div>
    </section>

    <div className="site-content site-prose">

      {/* ── LoRa ─────────────────────────────────────────────────────── */}
      <section className="prose-section">
        <h2>What is LoRa?</h2>
        <p>
          <strong>LoRa</strong> (Long Range) is a radio modulation technique developed by Semtech that
          allows small, low-power devices to communicate over long distances. Real-world range depends on
          terrain, antenna, mounting height, and radio settings. In poor urban conditions it may be under
          1 km, while elevated line-of-sight links can stretch to tens of kilometres.
        </p>
        <p>
          In the UK, LoRa typically operates in licence-exempt SRD spectrum around <strong>868 MHz</strong>.
          You do not need an amateur radio licence to run a node in these bands, but operation must stay
          within Ofcom licence-exempt limits (for example power and duty-cycle constraints in IR2030).
        </p>
        <div className="prose-facts">
          <div className="prose-fact">
            <span className="prose-fact__value">868 MHz</span>
            <span className="prose-fact__label">UK SRD band</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">&lt;1 to 30+ km</span>
            <span className="prose-fact__label">Range (terrain dependent)</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">IR2030</span>
            <span className="prose-fact__label">Licence-exempt limits apply</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">Shared</span>
            <span className="prose-fact__label">Non-protected spectrum</span>
          </div>
        </div>
      </section>

      {/* ── MeshCore ─────────────────────────────────────────────────── */}
      <section className="prose-section">
        <h2>What is MeshCore?</h2>
        <p>
          <strong>MeshCore</strong> is open-source firmware for ESP32-based LoRa hardware. It turns a
          cheap LoRa board into a self-contained mesh networking node. There is no central server, no
          cloud, no internet requirement. The network works entirely over radio.
        </p>
        <p>
          Each node can act as a <strong>repeater</strong> (relaying packets it hears to extend range),
          a <strong>companion node</strong> (paired to a phone for messaging), or a
          <strong> room server</strong> (a persistent message store for a location). A typical deployment
          uses a mixture of all three: fixed repeaters on high ground feeding into companions carried
          by users.
        </p>
        <p>
          Packets are routed <strong>automatically</strong>. When you send a message, your node broadcasts
          it. Any repeater that hears it rebroadcasts it, and so on until the destination is reached or
          the maximum hop count is exhausted. The route is discovered dynamically, no configuration
          required.
        </p>

        <h3>Supported hardware</h3>
        <p>MeshCore runs on a range of ESP32 LoRa boards. The most commonly used on our network:</p>
        <ul>
          <li><strong>Heltec WiFi LoRa 32 V4</strong>: our recommended choice. Small, integrated display, USB-C charging, built-in battery management.</li>
          <li><strong>Heltec WiFi LoRa 32 V3</strong>: previous generation, still well supported.</li>
          <li><strong>LILYGO T3S3</strong>: popular alternative with a slightly larger form factor.</li>
          <li><strong>Heltec Mesh Node T114</strong>: ultra-compact, designed specifically for mesh use.</li>
        </ul>
        <p>All boards are available from AliExpress or Amazon for £20–£40.</p>
      </section>

      {/* ── Teesside network ─────────────────────────────────────────── */}
      <section className="prose-section">
        <h2>Teesside Mesh Network</h2>
        <p>
          The Teesside Mesh Network is a community-run deployment of MeshCore covering Teesside and the
          surrounding area. We currently have repeater nodes across the region with more being added
          regularly.
        </p>
        <p>
          This dashboard (the one you're looking at right now) listens to radio traffic picked up by
          our observer node and presents it in real time. Every packet decoded is plotted on the map,
          showing the relay chain it took to get here, the signal strength at each hop, and the RF
          coverage each repeater provides based on its position and local terrain.
        </p>
        <p>
          The network uses the <strong>Public channel</strong> (the default MeshCore channel) so all
          standard MeshCore nodes in range can participate without any special configuration.
        </p>
        <div className="prose-actions">
          <Link to="/install" className="site-btn site-btn--primary">Get on the network →</Link>
          <a href="https://app.teessidemesh.com" className="site-btn site-btn--ghost">View live map</a>
        </div>
      </section>

      {/* ── How it works ─────────────────────────────────────────────── */}
      <section className="prose-section">
        <h2>How packets travel</h2>
        <p>
          When a MeshCore node sends a message it is broadcast over LoRa radio. Any node in range that
          hears it can relay it onward. The <em>hop count</em> tracks how many relays a packet has
          passed through. You can see this on the live map as the dotted path line that appears when
          a packet arrives.
        </p>
        <p>
          The longest relay chain we've seen so far on the Teesside network is displayed on the home
          page. Each hop represents another node extending the network's reach. A well-positioned
          repeater on high ground can add many kilometres to the effective range.
        </p>
        <p>
          All packet data is decoded and logged in real time. Our observer node hears packets from
          across the region, and the dashboard can show you the approximate signal path even for nodes
          many hops away.
        </p>
      </section>

    </div>
  </>
);
