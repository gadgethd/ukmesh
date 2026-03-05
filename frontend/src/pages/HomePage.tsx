import React from 'react';
import { Link } from 'react-router-dom';
import { LiveStatsSection } from '../components/LiveStatsSection.js';
import { getCurrentSite } from '../config/site.js';

export const HomePage: React.FC = () => {
  const site = getCurrentSite();

  return (
    <>
      {/* ── Hero ─────────────────────────────────────────────────────── */}
      <section className="site-hero">
        <div className="site-hero__glow" aria-hidden />
        <div className="site-content">
          <div className="site-hero__badge">North East England · LoRa 868 MHz</div>
          <h1 className="site-hero__title">
            Teesside<br />
            <span className="site-hero__title--accent">Mesh Network</span>
          </h1>
          <p className="site-hero__sub">
            An experimental off-grid communications network built on{' '}
            <a href="https://meshcore.co.uk" target="_blank" rel="noopener noreferrer">MeshCore</a>,{' '}
            a free, open-source LoRa mesh platform. No internet. No infrastructure. Just radio.
          </p>
          <div className="site-hero__actions">
            <a href={site.appUrl} className="site-btn site-btn--primary">Open Live Map →</a>
            <Link to="/about" className="site-btn site-btn--ghost">Learn more</Link>
          </div>
        </div>
      </section>

      <LiveStatsSection network={site.network} />

      {/* ── Radio config ─────────────────────────────────────────────── */}
      <section className="site-stats-section site-stats-section--alt">
        <div className="site-content">
          <p className="site-stats-section__eyebrow">Network radio configuration</p>
          <div className="site-stats-grid site-stats-grid--6">
            <div className="site-stat">
              <span className="site-stat__value">EU/UK Narrow</span>
              <span className="site-stat__label">Profile</span>
            </div>
            <div className="site-stat">
              <span className="site-stat__value">869.618</span>
              <span className="site-stat__label">Frequency (MHz)</span>
            </div>
            <div className="site-stat">
              <span className="site-stat__value">62.5<span className="site-stat__suffix">kHz</span></span>
              <span className="site-stat__label">Bandwidth</span>
            </div>
            <div className="site-stat">
              <span className="site-stat__value">SF8</span>
              <span className="site-stat__label">Spreading Factor</span>
            </div>
            <div className="site-stat">
              <span className="site-stat__value">CR8</span>
              <span className="site-stat__label">Coding Rate</span>
            </div>
          </div>
        </div>
      </section>

      {/* ── About cards ─────────────────────────────────────────────── */}
      <section className="site-section">
        <div className="site-content site-cards-row">

          <div className="site-card">
            <div className="site-card__icon">📡</div>
            <h2 className="site-card__title">What is MeshCore?</h2>
            <p className="site-card__body">
              MeshCore is open-source firmware for ESP32 LoRa hardware. Each node acts as
              both a radio and a relay. Packets hop between nodes automatically, extending
              range far beyond what a single radio can achieve.
            </p>
            <Link to="/about" className="site-card__link">Learn more →</Link>
          </div>

          <div className="site-card">
            <div className="site-card__icon">🔧</div>
            <h2 className="site-card__title">Get on the network</h2>
            <p className="site-card__body">
              All you need is a supported LoRa board and about 10 minutes. Flash the firmware
              in your browser, configure your node, and you're on the air. In the UK, 868 MHz
              operation is licence-exempt when run within Ofcom IR2030 limits.
            </p>
            <Link to="/install" className="site-card__link">Install guide →</Link>
          </div>

          <div className="site-card">
            <div className="site-card__icon">🗺️</div>
            <h2 className="site-card__title">Live map & analytics</h2>
            <p className="site-card__body">
              The live dashboard shows every packet we hear in real time: node positions,
              relay paths, RF coverage, and a decoded packet feed. Built entirely on open
              source tools.
            </p>
            <a href={site.appUrl} className="site-card__link">Open map →</a>
          </div>

        </div>
      </section>

      {/* ── Discord CTA ─────────────────────────────────────────────── */}
      <section className="site-section site-section--dark">
        <div className="site-content site-cta">
          <div className="site-cta__text">
            <h2 className="site-cta__title">Join the conversation</h2>
            <p className="site-cta__body">
              We hang out in the <strong>North East England</strong> regional channel on the
              MeshCore Discord. Come say hello, ask questions, or arrange a test contact.
              DM <strong>ibengr</strong> to find out more.
            </p>
          </div>
          <a
            href="https://discord.gg/bSuST8xvet"
            target="_blank"
            rel="noopener noreferrer"
            className="site-btn site-btn--primary"
          >
            Join Discord →
          </a>
        </div>
      </section>
    </>
  );
};
