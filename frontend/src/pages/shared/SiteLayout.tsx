import React, { useEffect, useRef, useState } from 'react';
import { Link, NavLink, Outlet } from 'react-router';

type SiteLayoutProps = {
  brandName: string;
  footerName: string;
  appUrl: string;
  integratedMap?: boolean;
};

const primary = [
  ['/repeater', 'Repeaters'], ['/regions', 'Regions'], ['/feed', 'Feed'],
] as const;
const healthCheckLink = ['/health-check', 'Health Check'] as const;
const groups = [
  ['Tools', [['/topology', 'Topology'], ['/companion', 'Companions'], ['/spam', 'Spam']]],
  ['Help', [['/install', 'Install'], ['/docs', 'Docs'], ['/open-source', 'Open Source']]],
] as const;

import { OWNER_SESSION_EVENT } from '../../utils/appEvents.js';
import { fetchJson } from '../../utils/api.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';

type OwnerSessionSummary = {
  ok: boolean;
  mqttUsername?: string | null;
};

function isOwnerSessionSummary(value: unknown): value is OwnerSessionSummary {
  if (!value || typeof value !== 'object') return false;
  const row = value as Record<string, unknown>;
  return typeof row.ok === 'boolean'
    && (row.mqttUsername == null || typeof row.mqttUsername === 'string');
}

function isSpamSummary(value: unknown): value is { activeIncidents: number } {
  return !!value && typeof value === 'object' && 'activeIncidents' in value
    && Number.isSafeInteger(value.activeIncidents) && Number(value.activeIncidents) >= 0;
}

function navClassName({ isActive }: { isActive: boolean }): string {
  return isActive ? 'site-nav__link site-nav__link--active' : 'site-nav__link';
}

export const SiteLayout: React.FC<SiteLayoutProps> = ({ brandName, footerName, appUrl, integratedMap }) => {
  const { privacyGeneration } = useRuntimeFeatures();
  const COOKIE_CONSENT_KEY = 'meshcore-cookie-consent-v1';
  const [menuOpen, setMenuOpen] = useState(false);
  const navRef = useRef<HTMLElement>(null);
  const menuTrigger = useRef<HTMLButtonElement>(null);
  const [ownerLabel, setOwnerLabel] = useState<string | null>(null);
  const [activeSpamIncidents, setActiveSpamIncidents] = useState(0);
  const [cookieConsent, setCookieConsent] = useState<boolean>(() => {
    try {
      return localStorage.getItem(COOKIE_CONSENT_KEY) === '1';
    } catch {
      return false;
    }
  });
  const closeMenu = () => {
    setMenuOpen(false);
    navRef.current?.querySelectorAll<HTMLDetailsElement>('details[open]').forEach(group => { group.open = false; });
  };
  const renderLink = ([to, label]: readonly [string, string]) => <NavLink key={to} to={to} onClick={closeMenu} className={navClassName}>
    {label}
    {to === '/spam' && activeSpamIncidents > 0 && <span className="site-nav__badge" aria-label={`${activeSpamIncidents} active incidents`}>{activeSpamIncidents}</span>}
  </NavLink>;

  useEffect(() => {
    setActiveSpamIncidents(0);
    const controller = new AbortController();
    fetchJson('/api/spam/messages/status', { signal: controller.signal }, { maxBytes: 64 * 1024, validate: isSpamSummary })
      .then((value) => setActiveSpamIncidents(Number(value?.activeIncidents ?? 0)))
      .catch(() => {});
    return () => controller.abort();
  }, [privacyGeneration]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      const summary = navRef.current?.querySelector<HTMLElement>('details[open] > summary');
      if (!summary && !menuOpen) return;
      closeMenu();
      (menuOpen ? menuTrigger.current : summary)?.focus();
    };
    const handlePointerDown = (event: PointerEvent) => {
      if (!navRef.current?.contains(event.target as Node)) closeMenu();
    };

    document.addEventListener('keydown', handleKeyDown);
    document.addEventListener('pointerdown', handlePointerDown);
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.removeEventListener('pointerdown', handlePointerDown);
    };
  }, [menuOpen]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    const loadOwnerSession = () => {
      fetchJson('/api/owner/session', { cache: 'no-store', credentials: 'same-origin', signal: controller.signal },
        { maxBytes: 1024 * 1024, validate: isOwnerSessionSummary })
        .then((json) => {
          if (cancelled) return;
          setOwnerLabel(json?.mqttUsername?.trim() || null);
        })
        .catch(() => {
          if (cancelled) return;
          setOwnerLabel(null);
        });
    };

    const handleOwnerSession = (event: Event) => {
      const detail = (event as CustomEvent<{ mqttUsername?: string | null }>).detail;
      setOwnerLabel(detail?.mqttUsername?.trim() || null);
    };

    loadOwnerSession();
    window.addEventListener(OWNER_SESSION_EVENT, handleOwnerSession as EventListener);
    return () => {
      cancelled = true;
      controller.abort();
      window.removeEventListener(OWNER_SESSION_EVENT, handleOwnerSession as EventListener);
    };
  }, []);

  const acceptCookies = () => {
    try {
      localStorage.setItem(COOKIE_CONSENT_KEY, '1');
    } catch {
      // Ignore storage failures and just hide the banner for this session.
    }
    setCookieConsent(true);
  };

  return (
    <div className="site-layout">
      <a href="#site-main" className="ui-skip-link">Skip to content</a>
      <nav ref={navRef} className={`site-nav${integratedMap ? ' site-nav--embedded-tools' : ''}`} aria-label="Main navigation">
        <Link to="/" className="site-nav__brand" onClick={closeMenu}>
          <span className="site-nav__icon">◈</span>
          <span className="site-nav__name">{brandName}</span>
        </Link>

        {integratedMap ? (
          <NavLink to="/live-map" onClick={closeMenu} className={state => `${navClassName(state)} site-nav__link--map`}>Live Map</NavLink>
        ) : (
          <a
              href={appUrl}
              className="site-nav__link site-nav__link--map site-nav__link--external"
              aria-label="Live Map (external app)"
              onClick={closeMenu}
            >
              Live Map <span className="site-nav__external-icon" aria-hidden="true">↗</span>
          </a>
        )}
        {integratedMap && renderLink(healthCheckLink)}

        <button
          ref={menuTrigger}
          type="button"
          className="site-nav__hamburger"
          aria-label={menuOpen ? 'Close menu' : 'Open menu'}
          aria-controls="site-navigation"
          aria-expanded={menuOpen}
          onClick={() => { if (menuOpen) closeMenu(); else setMenuOpen(true); }}
        >
          {menuOpen ? '✕' : '☰'}
        </button>
        <div
          id="site-navigation"
          className={`site-nav__links${menuOpen ? ' site-nav__links--open' : ''}`}
        >
          {primary.map(renderLink)}
          {!integratedMap && renderLink(healthCheckLink)}
          {groups.map(([label, items]) => (
            <details className="site-nav__group" key={label} onToggle={event => {
              if (event.currentTarget.open) navRef.current?.querySelectorAll<HTMLDetailsElement>('details[open]').forEach(other => {
                if (other !== event.currentTarget) other.open = false;
              });
            }}>
              <summary className="site-nav__link">{label} <span aria-hidden="true">▾</span></summary>
              <div className="site-nav__group-links">
                {items.map(renderLink)}
              </div>
            </details>
          ))}
          <NavLink
            to="/login"
            onClick={closeMenu}
            className={({ isActive }) => isActive ? 'site-nav__app-btn site-nav__app-btn--active' : 'site-nav__app-btn'}
          >
            {ownerLabel ?? 'Login'}
          </NavLink>
        </div>
      </nav>

      <main className="site-main" id="site-main" tabIndex={-1}>
        <Outlet />
      </main>

      <footer className="site-footer">
        <span>{footerName}</span>
        <span className="site-footer__sep">·</span>
        <a href="https://meshcore.gg/" target="_blank" rel="noopener noreferrer">Discord</a>
        <span className="site-footer__sep">·</span>
        <Link to="/contact">Contact</Link>
        <span className="site-footer__sep">·</span>
        <Link to="/open-source">Open Source</Link>
        <span className="site-footer__sep">·</span>
        <Link to="/privacy">Privacy</Link>
        <span className="site-footer__sep">·</span>
        {integratedMap ? <Link to="/live-map">Live Map</Link> : <a href={appUrl}>Live Map</a>}
      </footer>

      {!cookieConsent && (
        <div className="cookie-banner" role="dialog" aria-live="polite" aria-label="Cookie notice">
          <div className="cookie-banner__body">
            <strong>Cookies and site preferences</strong>
            <p>Cookies keep you signed in and remember your preferences. <Link to="/privacy">Privacy policy</Link>.</p>
          </div>
          <button type="button" className="cookie-banner__button" onClick={acceptCookies}>Accept</button>
        </div>
      )}
    </div>
  );
};
